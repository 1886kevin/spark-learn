前面的都是在构造task，其实task是真正运行在Executor（CoarseGrainedExecutorBackend）上的，下面就直接看他的实现，
话接上文，最终driver向CoarseGrainedExecutorBackend发送了launchTask的消息，
CoarseGrainedExecutorBackend在接收到这个消息后：

```
    case LaunchTask(data) =>
      if (executor == null) {
        logError("Received LaunchTask command but executor was null")
        System.exit(1)
      } else {
        1.首先先把接收到的数据反序列化
        val taskDesc = ser.deserialize[TaskDescription](data.value)
        logInfo("Got assigned task " + taskDesc.taskId)
        2. 通过executor来运行这个task
        executor.launchTask(this, taskId = taskDesc.taskId, attemptNumber = taskDesc.attemptNumber,
          taskDesc.name, taskDesc.serializedTask)
      }

```
接着就看他的实现了：
```
     def launchTask(
      context: ExecutorBackend,
      taskId: Long,
      attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer): Unit = {
    1. 这里又是通过task的代理来实现其启动的
    val tr = new TaskRunner(context, taskId = taskId, attemptNumber = attemptNumber, taskName,
      serializedTask)
    runningTasks.put(taskId, tr)
    threadPool.execute(tr)
  }

```
接着就看这个TaskRunner的run方法：
```
      override def run(): Unit = {
      val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)
      val deserializeStartTime = System.currentTimeMillis()
      Thread.currentThread.setContextClassLoader(replClassLoader)
      val ser = env.closureSerializer.newInstance()
      logInfo(s"Running $taskName (TID $taskId)")
      1. 运行前先向execBackend会向driver发送一个StatusUpdate的消息
      execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      var taskStart: Long = 0
      startGCTime = computeTotalGcTime()

      try {
       2. 反序列化这个任务
        val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(serializedTask)
       3. 根据反序列化的数据更新本地数据 
       updateDependencies(taskFiles, taskJars)
       4. 然后在反序列化这个task，得到最终运行的代码
        task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)
        task.setTaskMemoryManager(taskMemoryManager)

        // If this task has been killed before we deserialized it, let's quit now. Otherwise,
        // continue executing the task.
        if (killed) {
          // Throw an exception rather than returning, because returning within a try{} block
          // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
          // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
          // for the task.
          throw new TaskKilledException
        }

        logDebug("Task " + taskId + "'s epoch is " + task.epoch)
       5. 如果epoch大于mapOutputTracker中的epoch，则更新，并清空这个task的 map output locations的数据
        // Incremented every time a fetch fails so that client nodes know to clear  their cache of map output locations if this happens.
        env.mapOutputTracker.updateEpoch(task.epoch)

        // Run the actual task and measure its runtime.
        taskStart = System.currentTimeMillis()
        var threwException = true
        val (value, accumUpdates) = try {
        6. 程序真正的运行就在这里了，后续是对处理结果的操作在这里就不分析了，有兴趣自己下去看一下，接着就看这个run方法。
          val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = attemptNumber,
            metricsSystem = env.metricsSystem)
          threwException = false
          res
        } finally {
          val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
          val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()
          if (freedMemory > 0) {
            val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
            if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false) && !threwException) {
              throw new SparkException(errMsg)
            } else {
              logError(errMsg)
            }
          }

          if (releasedLocks.nonEmpty) {
            val errMsg =
              s"${releasedLocks.size} block locks were not released by TID = $taskId:\n" +
              releasedLocks.mkString("[", ", ", "]")
            if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false) && !threwException) {
              throw new SparkException(errMsg)
            } else {
              logError(errMsg)
            }
          }
        }
        val taskFinish = System.currentTimeMillis()

        // If the task has been killed, let's fail it.
        if (task.killed) {
          throw new TaskKilledException
        }

        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        val valueBytes = resultSer.serialize(value)
        val afterSerialization = System.currentTimeMillis()

        for (m <- task.metrics) {
          // Deserialization happens in two parts: first, we deserialize a Task object, which
          // includes the Partition. Second, Task.run() deserializes the RDD and function to be run.
          m.setExecutorDeserializeTime(
            (taskStart - deserializeStartTime) + task.executorDeserializeTime)
          // We need to subtract Task.run()'s deserialization time to avoid double-counting
          m.setExecutorRunTime((taskFinish - taskStart) - task.executorDeserializeTime)
          m.setJvmGCTime(computeTotalGcTime() - startGCTime)
          m.setResultSerializationTime(afterSerialization - beforeSerialization)
          m.updateAccumulators()
        }

        val directResult = new DirectTaskResult(valueBytes, accumUpdates, task.metrics.orNull)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit

        // directSend = sending directly back to the driver
        val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId, serializedDirectResult, StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }

        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)

      } catch {
        case ffe: FetchFailedException =>
          val reason = ffe.toTaskEndReason
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case _: TaskKilledException | _: InterruptedException if task.killed =>
          logInfo(s"Executor killed $taskName (TID $taskId)")
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled))

        case CausedBy(cDE: CommitDeniedException) =>
          val reason = cDE.toTaskEndReason
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          val metrics: Option[TaskMetrics] = Option(task).flatMap { task =>
            task.metrics.map { m =>
              m.setExecutorRunTime(System.currentTimeMillis() - taskStart)
              m.setJvmGCTime(computeTotalGcTime() - startGCTime)
              m.updateAccumulators()
              m
            }
          }
          val serializedTaskEndReason = {
            try {
              ser.serialize(new ExceptionFailure(t, metrics))
            } catch {
              case _: NotSerializableException =>
                // t is not serializable so just send the stacktrace
                ser.serialize(new ExceptionFailure(t, metrics, false))
            }
          }
          execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Utils.isFatalError(t)) {
            SparkUncaughtExceptionHandler.uncaughtException(t)
          }

      } finally {
        runningTasks.remove(taskId)
      }
    }
```
task.run方法：
```
final def run(
    taskAttemptId: Long,
    attemptNumber: Int,
    metricsSystem: MetricsSystem)
  : (T, AccumulatorUpdates) = {
    1. 创建了一个task运行的上下文，包含一些基本信息 stageId, partitionId, taskAttemptId,attemptNumber 等
    context = new TaskContextImpl(
      stageId,
      partitionId,
      taskAttemptId,
      attemptNumber,
      taskMemoryManager,
      metricsSystem,
      internalAccumulators,
      runningLocally = false)
    TaskContext.setTaskContext(context)
    context.taskMetrics.setHostname(Utils.localHostName())
    context.taskMetrics.setAccumulatorsUpdater(context.collectInternalAccumulators)
    taskThread = Thread.currentThread()
    if (_killed) {
      kill(interruptThread = false)
    }
    try {
      2. 最终他就会调用这个方法来执行 func了
      (runTask(context), context.collectAccumulators())
    } catch {
      case e: Throwable =>
        // Catch all errors; run task failure callbacks, and rethrow the exception.
        try {
          context.markTaskFailed(e)
        } catch {
          case t: Throwable =>
            e.addSuppressed(t)
        }
        throw e
    } finally {
      // Call the task completion callbacks.
      context.markTaskCompleted()
      try {
        Utils.tryLogNonFatalError {
          // Release memory used by this thread for unrolling blocks
          SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask()
          SparkEnv.get.blockManager.memoryStore.releasePendingUnrollMemoryForThisTask()
          // Notify any tasks waiting for execution memory to be freed to wake up and try to
          // acquire memory again. This makes impossible the scenario where a task sleeps forever
          // because there are no other tasks left to notify it. Since this is safe to do but may
          // not be strictly necessary, we should revisit whether we can remove this in the future.
          val memoryManager = SparkEnv.get.memoryManager
          memoryManager.synchronized { memoryManager.notifyAll() }
        }
      } finally {
        TaskContext.unset()
      }
    }
  }

```
这里的task其实是在反序列化的时候赋值的，也就是前面运行的TaskRunner中的run方法时：
```
        task = ser.deserialize[Task[Any]](taskBytes, Thread.currentThread.getContextClassLoader)
```
在我们的程序中其实真正用到的是下面这两个
![Screen Shot 2016-12-09 at 2.04.50 AM.png](http://upload-images.jianshu.io/upload_images/3736220-8bed1e7a8b68ef24.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

好了，写到这里整个程序就完成了，下个博客中，在谈我们刚开始说的代码的计算逻辑是怎么实现的。终于完成了，太佩服自己了，居然能狠下心来把这个整理出来。

