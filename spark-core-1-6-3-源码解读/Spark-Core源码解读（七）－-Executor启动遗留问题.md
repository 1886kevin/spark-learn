话不多说，直接看上面博客中最终的遗留问题，即driver（DriverEndpoint）端如何处理RegisterExecutor的消息。直接上代码了：

```
  case RegisterExecutor(executorId, executorRef, hostPort, cores, logUrls) =>
        1. 如果已经注册了，就返回重复注册的失败消息
        if (executorDataMap.contains(executorId)) {
          context.reply(RegisterExecutorFailed("Duplicate executor ID: " + executorId))
        } else {
         2. 获取这个Executor（CoarseGrainedExecutorBackend）的地址
          // If the executor's rpc env is not listening for incoming connections, `hostPort`
          // will be null, and the client connection should be used to contact the executor.
          val executorAddress = if (executorRef.address != null) {
              executorRef.address
            } else {
              context.senderAddress
            }
          logInfo(s"Registered executor $executorRef ($executorAddress) with ID $executorId")
          addressToExecutorId(executorAddress) = executorId
          totalCoreCount.addAndGet(cores)
          totalRegisteredExecutors.addAndGet(1)
          3. 在driver端用ExecutorData来存储Executor的信息
          val data = new ExecutorData(executorRef, executorRef.address, executorAddress.host,
            cores, cores, logUrls)
          // This must be synchronized because variables mutated
          // in this block are read when requesting executors
4. 下面这个代码只是在动态分配功能开启的时候，才有作用          CoarseGrainedSchedulerBackend.this.synchronized {
            executorDataMap.put(executorId, data)
            if (numPendingExecutors > 0) {
              numPendingExecutors -= 1
              logDebug(s"Decremented number of pending executors ($numPendingExecutors left)")
            }
          }
          // Note: some tests expect the reply to come after we put the executor in the map
5. 向Executor（CoarseGrainedExecutorBackend）返回已经注册的消息，然后再接收到这个消息后，才会new一个Executor对象，用于运行task         context.reply(RegisteredExecutor(executorAddress.host))
          listenerBus.post(
            SparkListenerExecutorAdded(System.currentTimeMillis(), executorId, data))
          makeOffers()
        }
```
再继续之前，先看一下这个总线中出发的事件SparkListenerExecutorAdded，它会交给每一个listenner来处理，但是能处理这个事件的listenner只有下面几个：

![Screen Shot 2016-12-07 at 2.13.37 AM.png](http://upload-images.jianshu.io/upload_images/3736220-dbc0c275f52946a1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
先看EventLoggingListener中的
```
  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    logEvent(event, flushLogger = true)
  }
       
```
这里会将这个事件写到日志中，接着看ExecutorsListener
```
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    val eid = executorAdded.executorId
    executorToLogUrls(eid) = executorAdded.executorInfo.logUrlMap
    executorIdToData(eid) = ExecutorUIData(executorAdded.time)
  }
       
```
再看HeartbeatReceiver中
```
  /**
   * If the heartbeat receiver is not stopped, notify it of executor registrations.
   */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    addExecutor(executorAdded.executorId)
  }


  def addExecutor(executorId: String): Option[Future[Boolean]] = {
    Option(self).map(_.ask[Boolean](ExecutorRegistered(executorId)))
  }
  1.下面的注释已经很清楚了，它用来接收的本类中的消息
  
    // Messages sent and received locally
    case ExecutorRegistered(executorId) =>
      executorLastSeen(executorId) = clock.getTimeMillis()
      context.reply(true)
```
当然这里还有一个ExecutorAllocationListener没有谈，因为它是用于动态分配资源的内容，就先不谈了。（先了解这个过程就可以）

到这里整个driver端的关于Executor注册的后续操作就全部完成了，下面再看一下Worker端的Executor。也就是CoarseGrainedExecutorBackend收到RegisteredExecutor消息后：

```
 
  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredExecutor(hostname) =>
      logInfo("Successfully registered with driver")
      executor = new Executor(executorId, hostname, env, userClassPath, isLocal = false)
        
```
这里只是new了一个Executor，接着就看一下Executor的初始化的过程，初始化的时候介绍两个重要的地方：

```
 1. 获取HeartbeatReceiver这个Endpoint的引用，因为下面注册的信息都是通过心跳来完成的
   // must be initialized before running startDriverHeartbeat()
  private val heartbeatReceiverRef =
    RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)
    
  2. 启动心跳，向driver注册信息
  startDriverHeartbeater()
```
下面详细看这个方法：
```
   private def startDriverHeartbeater(): Unit = {
    val intervalMs = conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s")

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    }
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }       
```
这里的实现其实很简单，就是周期性的启动一个线程运行reportHeartBeat方法。

```
/** Reports heartbeat and metrics for active tasks to the driver. */
  private def reportHeartBeat(): Unit = {
    // list of (task id, metrics) to send back to the driver
    val tasksMetrics = new ArrayBuffer[(Long, TaskMetrics)]()
    1. 整个线程gc占用的时间，而不是某个具体的task
    val curGCTime = computeTotalGcTime()

    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        2. 更新当前task的测量信息
        taskRunner.task.metrics.foreach { metrics =>
          metrics.updateShuffleReadMetrics()
          metrics.updateInputMetrics()
          metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
          metrics.updateAccumulators()

          if (isLocal) {
            // JobProgressListener will hold an reference of it during
            // onExecutorMetricsUpdate(), then JobProgressListener can not see
            // the changes of metrics any more, so make a deep copy of it
            val copiedMetrics = Utils.deserialize[TaskMetrics](
              Utils.serialize(metrics),
              Utils.getContextOrSparkClassLoader)
            tasksMetrics += ((taskRunner.taskId, copiedMetrics))
          } else {
            // It will be copied by serialization
            tasksMetrics += ((taskRunner.taskId, metrics))
          }
        }
      }
    }
    3. 将executorId，tasksMetrics，blockManager已心跳的消息形势发送给heartbeatReceiver。
    val message = Heartbeat(executorId, tasksMetrics.toArray, env.blockManager.blockManagerId)
    try {
      val response = heartbeatReceiverRef.askWithRetry[HeartbeatResponse](
          message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"))
      if (response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        4. 这里会根据返回的消息来注册这个blockManager，从这里你可以得知blockManager的注册是通过心跳来完成的
        env.blockManager.reregister()
      }
      heartbeatFailures = 0
    } catch {
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(s"Exit as unable to send heartbeats to driver " +
            s"more than $HEARTBEAT_MAX_FAILURES times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
  }

  
```
接着我们就看heartbeatReceiver有什么反应吧：
```
// Messages received from executors
    case heartbeat @ Heartbeat(executorId, taskMetrics, blockManagerId) =>
      if (scheduler != null) {
        if (executorLastSeen.contains(executorId)) {
         1. 如果包含这个executorId就跟新它的executorLastSeen
          executorLastSeen(executorId) = clock.getTimeMillis()
         2. 回复一个心跳信息，关于回复的这个心跳信息下面直接分析它的关键代码
          eventLoopThread.submit(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, taskMetrics, blockManagerId)
              val response = HeartbeatResponse(reregisterBlockManager = unknownExecutor)
              context.reply(response)
            }
          })
        } else {
          // This may happen if we get an executor's in-flight heartbeat immediately
          // after we just removed it. It's not really an error condition so we should
          // not log warning here. Otherwise there may be a lot of noise especially if
          // we explicitly remove executors (SPARK-4134).
          logDebug(s"Received heartbeat from unknown executor $executorId")
         3.它这里说明的是Executor的注册和心跳同时发送过来，最终心跳信息先到，这种情况下就直接返回 reregisterBlockManager为true的心跳回复信息      context.reply(HeartbeatResponse(reregisterBlockManager = true))
        }
      } else {
      
        // Because Executor will sleep several seconds before sending the first "Heartbeat", this
        // case rarely happens. However, if it really happens, log it and ask the executor to
        // register itself again.
        logWarning(s"Dropping $heartbeat because TaskScheduler is not ready yet")
        context.reply(HeartbeatResponse(reregisterBlockManager = true))
      }

  
```
心跳关键代码：
```
 val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, taskMetrics, blockManagerId)
  
```
因为它这里调用的方法比较多，看起来比较简单，我就不一句一句分析代码了，下面只写出代码片段：

```
  1.上面的代码会直接调用TaskSchedulerImpl：

  override def executorHeartbeatReceived(
      execId: String,
      taskMetrics: Array[(Long, TaskMetrics)], // taskId -> TaskMetrics
      blockManagerId: BlockManagerId): Boolean = {

    val metricsWithStageIds: Array[(Long, Int, Int, TaskMetrics)] = synchronized {
      taskMetrics.flatMap { case (id, metrics) =>
        taskIdToTaskSetManager.get(id).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, metrics)
        }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, metricsWithStageIds, blockManagerId)
  }
  
 2. 然后是dagScheduler.executorHeartbeatReceived
   def executorHeartbeatReceived(
      execId: String,
      taskMetrics: Array[(Long, Int, Int, TaskMetrics)], // (taskId, stageId, stateAttempt, metrics)
      blockManagerId: BlockManagerId): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, taskMetrics))
    blockManagerMaster.driverEndpoint.askWithRetry[Boolean](
      BlockManagerHeartbeat(blockManagerId), new RpcTimeout(600 seconds, "BlockManagerHeartbeat"))
  }
  
 3.这里的driverEndpoint时间上是BlockManagerMasterEndpoint：
 case BlockManagerHeartbeat(blockManagerId) =>
      context.reply(heartbeatReceived(blockManagerId))
 
 4. 调用heartbeatReceived方法
   private def heartbeatReceived(blockManagerId: BlockManagerId): Boolean = {
    if (!blockManagerInfo.contains(blockManagerId)) {
      4.1 若不包含，若在driver端，直接返回为true，若在worker端直接返回为false。 
      blockManagerId.isDriver && !isLocal
    } else {
      4.2 若包含这个blockManagerId，直接跟新其信息
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      true
    }
  }
```
再看最初的那行代码，有一个“！”，计算完成后得到的其实是相反的值。

```
 val unknownExecutor = !scheduler.executorHeartbeatReceived(
                executorId, taskMetrics, blockManagerId)
  
```
然后将这个unknownExecutor的值传给消息中的reregisterBlockManager，最终由接收端判断是否需要重新注册blockManager。接收端前面已经讲过了。

至此整个task运行前的准备的流程就都分析完了，接下来就开始分析task的执行了。
