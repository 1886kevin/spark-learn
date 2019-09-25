**为学日益。为道日损。损之又损，以至於无为。无为而不为。**

### 最初的例子
现在我们再回过头来看我们的例子：
```
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    1. 第一个参数只要计算的数据 ，第二个参数指程序的并行度
    val count = spark.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / (n - 1))
    spark.stop()
  }
}
  
```
从之前的博客中，我们其实仅仅分析到了SparkContext的初始化，接着我们看程序的业务代码，也就是真正运行的计算。由此可以看出，程序计算资源的分配与程序业务的计算是分开的。
本例这里有三个算子：parallelize，map，reduce。spark的算子有两种：transformation级别和action级别，只有action级别的算子才会触发job的执行，transformation级别的在框架中是lazy级别的，这里先从第三个算子看起：

```
 def reduce(f: (T, T) => T): T = withScope {
    val cleanF = sc.clean(f)
    val reducePartition: Iterator[T] => Option[T] = iter => {
      if (iter.hasNext) {
        Some(iter.reduceLeft(cleanF))
      } else {
        None
      }
    }
    var jobResult: Option[T] = None
    val mergeResult = (index: Int, taskResult: Option[T]) => {
      if (taskResult.isDefined) {
        jobResult = jobResult match {
          case Some(value) => Some(f(value, taskResult.get))
          case None => taskResult
        }
      }
    }
    1. 通过这里可以看出这个算子是触发了job的
    sc.runJob(this, reducePartition, mergeResult)
    // Get the final result out of our Option, or throw an exception if the RDD was empty
    jobResult.getOrElse(throw new UnsupportedOperationException("empty collection"))
  }  
```
reduce是action级别的，它触发了job，先不分析上面的业务逻辑，先看它runJob的操作：

```
   def runJob[T, U: ClassTag](
      rdd: RDD[T],
      processPartition: Iterator[T] => U,
      resultHandler: (Int, U) => Unit)
  {
    val processFunc = (context: TaskContext, iter: Iterator[T]) => processPartition(iter)
    runJob[T, U](rdd, processFunc, 0 until rdd.partitions.length, resultHandler)
  }
```
接着runJob的真正实现：
```
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      resultHandler: (Int, U) => Unit): Unit = {
    if (stopped.get()) {
      throw new IllegalStateException("SparkContext has been shutdown")
    }
    val callSite = getCallSite
    val cleanedFunc = clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    if (conf.getBoolean("spark.logLineage", false)) {
      logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
    }
    1. 它这里是通过dagScheduler来构建job
    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
    progressBar.foreach(_.finishAll())
    2. 它这个rdd执行了checkpoint，关于checkpoint后面再将
    rdd.doCheckpoint()
  }
```
所以先看这个dagScheduler.runJob：
```
  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    1. 提交这个job
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    2. 等待job处理结果
    waiter.awaitResult() match {
      case JobSucceeded =>
        logInfo("Job %d finished: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
      case JobFailed(exception: Exception) =>
        logInfo("Job %d failed: %s, took %f s".format
          (waiter.jobId, callSite.shortForm, (System.nanoTime - start) / 1e9))
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }
```
接着看这个submitJob：

```
def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    1. 获取partition的长度
    val maxPartitions = rdd.partitions.length
    2. 检查是否有越界的partition
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }
    3. 生成jobId
    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    4. 传入的函数参数，也就是业务代码的逻辑
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    5. 新建JobWaiter，用来返回job的处理结果
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    6.通过loop的方式，驱动程序的运行
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      SerializationUtils.clone(properties)))
    waiter
  }
  
```
下面看它是怎么实现的：
```
  private[scheduler] val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)

这个方法在DAGSchedulerEventProcessLoop中：
  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)  
```
### job中stage的pipeline的生成

它会直接调用handleJobSubmitted（第一部分）这个方法：
```
 private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      1. 关于stage的划分，是有宽依赖和窄依赖来划分的，每一个而这里的reduce是宽依赖的，前面的map是窄依赖的，而对于宽依赖对应的是ResultStage，ShuffleMapStage，宽依赖对应的算子为这个stage操作的pipeline的终点，窄依赖的算子对应的是pipeline的起点或中间节点
      finalStage = newResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
```
下面先直接说这个newResultStage的构建：

```
  private def newResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {
    1. 获取其父stage
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, jobId)
    val stage = new ResultStage(id, rdd, func, partitions, parentStages, jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

 
```
接下来接着看getParentStagesAndId这个方法：

```
    private def getParentStagesAndId(rdd: RDD[_], firstJobId: Int): (List[Stage], Int) = {
    1. 通过这个方法获取其父stage
    val parentStages = getParentStages(rdd, firstJobId)
    2. 通过这个操作获取其stageId
    val id = nextStageId.getAndIncrement()
    (parentStages, id)
  }
 
```
接着看这getParentStages

```
     private def getParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        3. 从这里可以看出如果有宽依赖（ShuffleDependency），会直接构建新的stage
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              parents += getShuffleMapStage(shufDep, firstJobId)
            case _ =>
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }
    1. 该方法的运行开始位置在这里
    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
     2. 通过上面定义的visit方法来处理waitingForVisit这个栈中的数据
        visit(waitingForVisit.pop())
    }
    parents.toList
  }
   
```
下面看这个构建stage的方法getShuffleMapStage：

```
  private def getShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      1. 如果之前已经构建了这个stage，则直接返回
      case Some(stage) => stage
      2. 若果没有则创建新的
      case None =>
        // We are going to register ancestor shuffle dependencies
      2.1 在创建新的ShuffleStage之前，如果有它依赖的 ShuffleStage则先构建其依赖的 ShuffleStage，之后看一下这个方法的实现      getAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          shuffleToMapStage(dep.shuffleId) = newOrUsedShuffleStage(dep, firstJobId)
        }
        // Then register current shuffleDep
      2.2 然后就是构建这个新的ShuffleStage
        val stage = newOrUsedShuffleStage(shuffleDep, firstJobId)
        shuffleToMapStage(shuffleDep.shuffleId) = stage
        stage
    }
  }
   
```
先看2.1中的getAncestorShuffleDependencies：

```
   /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
  private def getAncestorShuffleDependencies(rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    val parents = new Stack[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
             1. 如果构建的shuffleStage没有这个，就直接添加到stage的pipeline中，也就是这个parents中
              if (!shuffleToMapStage.contains(shufDep.shuffleId)) {
                parents.push(shufDep)
              }
            case _ =>
          }
          waitingForVisit.push(dep.rdd)
        }
      }
    }

    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    parents
  }   
```
看到这里你会发现，它这里其实使用回溯的方法来构建stage的pipeline的。
接着看这个newOrUsedShuffleStage方法，他在做些什么：

```
  private def newOrUsedShuffleStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.length
    1. 新建一个ShuffleMapStage，下面这个方法流程和newResultStage，也是经过回溯的方法来实现的
    val stage = newShuffleMapStage(rdd, numTasks, shuffleDep, firstJobId, rdd.creationSite)
    2. 如果这个shuffle的依赖已经注册到mapOutputTracker，就直接将shuffle输出位置的信息添加到这个stage中
    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      (0 until locs.length).foreach { i =>
        if (locs(i) ne null) {
          // locs(i) will be null if missing
          stage.addOutputLoc(i, locs(i))
        }
      }
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
       3. 如果这个shuffle的依赖已经注册到mapOutputTracker，如果没有，就注册这个shuffle的依赖
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)
    }
    stage
  }
  
```
ok，到这里还差一个方法也就是newResultStage和newShuffleMapStage的最后
要执行的updateJobIdStageIdMaps这个方法：

```
 private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parents: List[Stage] = getParentStages(s.rdd, jobId)
        val parentsWithoutThisJobId = parents.filter { ! _.jobIds.contains(jobId) }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }  
```
这个方法就是更新对应的job下stage的信息。ok，到这里整个job的pipeline就完成了，这里大家一定要多看几遍，知道看清楚看明白。

### 提交stage

handleJobSubmitted（第二部分）
```
    1. 生成一个ActiveJob，这个类很简单，自己看一下
    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    2. 清空taskLocation的缓存
    clearCacheLocs()
    3. 打印必要的信息，下面这些就是你在控制台上经常看到的信息了
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))
   4. 跟新相关的参数
    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    5. 这个监听器的一些操作了，上篇博客中已经讲过怎么看了，这里就不多介绍了
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage)

    submitWaitingStages()
  }
```
接着看这个submitStage，会根据这个stage生成task提交给Executor：

```
  /** Submits stage, but first recursively submits any missing parents. */
  private def submitStage(stage: Stage) {
    1. 此处需要注意一下：这是spark优于其他框架的一种体现，它这里会返回一个job的数组，也就是说如果有多个job依赖于同一个stage的话，它只执行一次。
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        2. 这里的这个missing不是丢失的意思，你直接可以理解为其依赖就行，
           下面这个方法，也是采用的回溯的方法（就是前面讲道德）实现的
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        if (missing.isEmpty) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          3. 如果他没有依赖的stage直接提交
          submitMissingTasks(stage, jobId.get)
        } else {
          4. 如果有的话，直接提交其父stage，还是根据回溯的方法实现，直到它没有依赖了
          for (parent <- missing) {
            submitStage(parent)
          }
          5. 暂时将这个stage放到waitingStages中
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

```
### 提交task

下面直接看这个submitMissingTasks：
```
 /** Called when stage's parents are available and we can now do its task. */
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
   1. 在运行这个stage之前，清空上一次这个stage中保存的partition的信息
    stage.pendingPartitions.clear()
    // First figure out the indexes of partition ids to compute.
    2. 获取要计算的partitions
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    // Create internal accumulators if the stage has no accumulators initialized.
    // Reset internal accumulators only if this stage is not partially submitted
    // Otherwise, we may override existing accumulator values from some tasks
    3.新建或者resetstage内部的累加器，用来统计task
    if (stage.internalAccumulators.isEmpty || stage.numPartitions == partitionsToCompute.size) {
      stage.resetInternalAccumulators()
    }

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
   4. 获取这个job的配置参数
    val properties = jobIdToActiveJob(jobId).properties
   
    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    5. Authority that decides whether tasks can commit output to HDFS. Uses a "first committer wins"* policy.
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    6. 获取task的location，可以task运行的本地性是在这里获得的，通过getPreferredLocs这个方法。
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          val job = s.activeJob.get
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))
        abortStage(stage, s"Task creation failed: $e\n${e.getStackTraceString}", Some(e))
        runningStages -= stage
        return
    }
    7. 创建这个stage提交的信息
    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)
   8. 又回通过总线触发一些列的动作，自己看啊！
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo, properties))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    9. 将ShuffleMapStage中的RDD，及其依赖关系；ResultStage中的RDD及其计算方法func广播出去，这里的这个func就是我们的业务逻辑计算，稍后还会分析，这里先提个醒。由此也可以看出ShuffleMapStage不会触发计算，只有ResultStage才会触发计算
    var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef).array()
        case stage: ResultStage =>
          closureSerializer.serialize((stage.rdd, stage.func): AnyRef).array()
      }

      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case NonFatal(e) =>
        abortStage(stage, s"Task serialization failed: $e\n${e.getStackTraceString}", Some(e))
        runningStages -= stage
        return
    }
   10. 现在就是构建具体的task了
    val tasks: Seq[Task[_]] = try {
      stage match {
        case stage: ShuffleMapStage =>
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = stage.rdd.partitions(id)
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, stage.internalAccumulators)
          }

        case stage: ResultStage =>
          val job = stage.activeJob.get
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = stage.rdd.partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptId,
              taskBinary, part, locs, id, stage.internalAccumulators)
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${e.getStackTraceString}", Some(e))
        runningStages -= stage
        return
    }

    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      stage.pendingPartitions ++= tasks.map(_.partitionId)
      logDebug("New pending partitions: " + stage.pendingPartitions)
     11.下面就是提交最终的task了，这里主要这是一个TaskSet，因为上面在构建task的时候，是根据每个partition构建了一个task
      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptId, jobId, properties))
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      markStageAsFinished(stage, None)
      val debugString = stage match {
        case stage: ShuffleMapStage =>
          s"Stage ${stage} is actually done; " +
            s"(available: ${stage.isAvailable}," +
            s"available outputs: ${stage.numAvailableOutputs}," +
            s"partitions: ${stage.numPartitions})"
        case stage : ResultStage =>
          s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})"
      }
      logDebug(debugString)
    }
  }

```
下面就看这个 taskScheduler.submitTasks方法：

```
    override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    1. task提交的同步，由synchronized这个关键字来保证
    this.synchronized {
    2. 创建TaskSetManager，稍后会详细分析
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])
      stageTaskSets(taskSet.stageAttemptId) = manager
    3.判断当前提交的数据集是否和之前stage记录的有冲突
      val conflictingTaskSet = stageTaskSets.exists { case (_, ts) =>
        ts.taskSet != taskSet && !ts.isZombie
      }
      if (conflictingTaskSet) {
        throw new IllegalStateException(s"more than one active taskSet for stage $stage:" +
          s" ${stageTaskSets.toSeq.map{_._2.taskSet.id}.mkString(",")}")
      }
     4. 这个schedulableBuilder是之前，调用TaskSchedulerImpl中的initialize方法来赋值的，这里具体的是 new FIFOSchedulableBuilder(rootPool)。
     5. 将这个manager和配置参数添加到schedulableBuilder中，因为之后提交的tasks会通过schedulableBuilder来获取
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
     6. 下面这个代码不会在这里阻塞，因为它是另外启动的一个线程
      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
    7. 提交这些tasks，其实这里是向driver发送了一个消息，而后driver会通过schedulableBuilder获取要提交的tasks
    backend.reviveOffers()
  }
   
```
接着分析 backend.reviveOffers()：
```
    override def reviveOffers() {
    driverEndpoint.send(ReviveOffers)
  }
     
```
这面的这个driverEndpoint指的是DriverEndpoint的一个对象，下面看这个DriverEndpoint处理ReviveOffers这个消息的方法：

```
      case ReviveOffers =>
        makeOffers()             
```
然后会调用这个makeOffers方法：
```
   private def makeOffers() {
      // Filter out executors under killing
     1. 首先会筛选出可用的executor
      val activeExecutors = executorDataMap.filterKeys(executorIsAlive)
     2. 构造一个WorkerOffer的集合
      val workOffers = activeExecutors.map { case (id, executorData) =>
        new WorkerOffer(id, executorData.executorHost, executorData.freeCores)
      }.toSeq

     3. workOffers经过处理后，也就是将这些tasks分配到这些executor上之后，提交这些task
      launchTasks(scheduler.resourceOffers(workOffers))
    }              
```
先看这个launchTasks方法：

```
 // Launch tasks returned by a set of resource offers
    private def launchTasks(tasks: Seq[Seq[TaskDescription]]) {
      for (task <- tasks.flatten) {
        1. 先把这个task序列化
        val serializedTask = ser.serialize(task)
        2. 序列化的task不能超过128M（默认情况下）
        if (serializedTask.limit >= akkaFrameSize - AkkaUtils.reservedSizeBytes) {
          scheduler.taskIdToTaskSetManager.get(task.taskId).foreach { taskSetMgr =>
            try {
              var msg = "Serialized task %s:%d was %d bytes, which exceeds max allowed: " +
                "spark.akka.frameSize (%d bytes) - reserved (%d bytes). Consider increasing " +
                "spark.akka.frameSize or using broadcast variables for large values."
              msg = msg.format(task.taskId, task.index, serializedTask.limit, akkaFrameSize,
                AkkaUtils.reservedSizeBytes)
              taskSetMgr.abort(msg)
            } catch {
              case e: Exception => logError("Exception in error callback", e)
            }
          }
        }
        else {
          3. 如果可以发送的话，就发送给executor，交由executor来执行
          val executorData = executorDataMap(task.executorId)
          executorData.freeCores -= scheduler.CPUS_PER_TASK
          executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
        }
      }
    }                 
```
再继续往下讲之前我们漏掉了一个最重要的东西，即   launchTasks(scheduler.resourceOffers(workOffers))，
也就是task是怎么分配到每个Executor上的：

### resourceOffers第一部分（从这里往后，一定要多看几遍源码，至少十遍，我自己都是看了10几遍才看明白的）：
```
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each slave as alive and remember its hostname
    // Also track if new executor is added
    1. 这里就跟注释上说的一样，标记活着的slave及其executor的信息
    var newExecAvail = false
    for (o <- offers) {
      executorIdToHost(o.executorId) = o.host
      executorIdToTaskCount.getOrElseUpdate(o.executorId, 0)
      if (!executorsByHost.contains(o.host)) {
        executorsByHost(o.host) = new HashSet[String]()
        executorAdded(o.executorId, o.host)
        newExecAvail = true
      }
     2. 如果你的集群有部署是夸rack的，会有这个信息，但是standalone模式下这个方法直接返回none，但是在yarn模式下可以配置相关信息
      for (rack <- getRackForHost(o.host)) {
        hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += o.host
      }
    }

    // Randomly shuffle offers to avoid always placing tasks on the same set of workers.
    3. 为了实现负载均衡，它这里会随机打乱一下这些offers
     val shuffledOffers = Random.shuffle(offers)
    // Build a list of tasks to assign to each worker.
    4. 这里会根据每个worker上可提供的cores的数量分配相同数量的task
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    5. 获取worker对应的core的个数
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    6. 这里前面有提到之前通过schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)，有先把数据放到这个rootPool上面
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
     7. 计算计算资源的本地性
        taskSet.executorAdded()
      }
    }
       
```
有新的Executor添加后，再调用  taskSet.executorAdded()重新更新一下当前taskSet的本地性参数。

```
   def recomputeLocality() {
    1. 记录的是上一个提交的task的LocalityLevel在myLocalityLevels的位置信息
        myLocalityLevels是这个taskSet中所有LocalityLevel的一个集合
    val previousLocalityLevel = myLocalityLevels(currentLocalityIndex)
    2. 重新计算一下，获得更新后的myLocalityLevels
    myLocalityLevels = computeValidLocalityLevels()
    3. 更新每个LocalityLevel的等待时间
    localityWaits = myLocalityLevels.map(getLocalityWait)
    4. 经过上面的操作后，上一个提交的task的LocalityLevel可能会改变，经过下面的处理，使其重新指向这个LocalityLevel
    currentLocalityIndex = getLocalityIndex(previousLocalityLevel)
  }
        
```
这里先看一下computeValidLocalityLevels方法：

```
   private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    1. pendingTasksForExecutor记录的是每一个Executor中分配的tasks
     如果满足if中的条件，就把相应的level加上，下面level的添加原理是一样的，等我分析完第二部分你应该就能明白这些代码的意思了。这里你只要记住，这个方法就是用来构建这个taskSet的localityLevel集合的就可以了
    if (!pendingTasksForExecutor.isEmpty && getLocalityWait(PROCESS_LOCAL) != 0 &&
        pendingTasksForExecutor.keySet.exists(sched.isExecutorAlive(_))) {
      levels += PROCESS_LOCAL
    }
    if (!pendingTasksForHost.isEmpty && getLocalityWait(NODE_LOCAL) != 0 &&
        pendingTasksForHost.keySet.exists(sched.hasExecutorsAliveOnHost(_))) {
      levels += NODE_LOCAL
    }
    if (!pendingTasksWithNoPrefs.isEmpty) {
      levels += NO_PREF
    }
    if (!pendingTasksForRack.isEmpty && getLocalityWait(RACK_LOCAL) != 0 &&
        pendingTasksForRack.keySet.exists(sched.hasHostAliveOnRack(_))) {
      levels += RACK_LOCAL
    }
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }        
```
 ### resourceOffers第二部分：      
```

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    1. 先说下这些参数代表的本地性：本地缓存，本地磁盘，同一台机器的不同节点中，同一个rack的其它节点中，其他rack的节点中
    var launchedTask = false
    2. 每个taskset的task会根据可用的LocalityLevels进行计算资源本地性最大的分配
    注意：这里是计算资源的本地性，不是数据的本地性，数据的本地性，前面已经讲过了，
     就是task最终会获得一个taskLocation的变量，这个变量中记录的是数据的本地性
    3. 像他注释上说的一样，首先会使用做大的LocalityLevel来尽量保证你的task都能获得PROCESS_LOCAL的LocalityLevel
    for (taskSet <- sortedTaskSets; maxLocality <- taskSet.myLocalityLevels) {
      do {
     4. 这里就看它是怎么来实现计算资源本地性最大的分配的，
         这里你一定要注意这个launchedTask变量的值，因为他每一次操作只操作的一个task
        launchedTask = resourceOfferSingleTaskSet(
            taskSet, maxLocality, shuffledOffers, availableCpus, tasks)
      } while (launchedTask)
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }
       
```
打开resourceOfferSingleTaskSet这个方法：

```
 private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      tasks: Seq[ArrayBuffer[TaskDescription]]) : Boolean = {
    var launchedTask = false
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      if (availableCpus(i) >= CPUS_PER_TASK) {
        try {
          1. 先看resourceOffer这个方法，这个方法返回的不是一个task  的集合，而是 Option[TaskDescription] 
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetManager(tid) = taskSet
            taskIdToExecutorId(tid) = execId
            executorIdToTaskCount(execId) += 1
            executorsByHost(host) += execId
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    2. 直到它的资源完全分配完了，才终止上面的那个while循环。
    return launchedTask
  }
 
```
resourceOffer：
```
 def resourceOffer(
      execId: String,
      host: String,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    if (!isZombie) {
      val curTime = clock.getTimeMillis()
      var allowedLocality = maxLocality
      if (maxLocality != TaskLocality.NO_PREF) {
       1. 这里先看下面的这个方法的说明
        allowedLocality = getAllowedLocalityLevel(curTime)
        if (allowedLocality > maxLocality) {
          // We're not allowed to search for farther-away tasks
          allowedLocality = maxLocality
        }
      }
      2. 这里也先看下面dequeueTask方法的说明
      dequeueTask(execId, host, allowedLocality) match {
        case Some((index, taskLocality, speculative)) => {
          // Found a task; do some bookkeeping and return a task description
          val task = tasks(index)
          val taskId = sched.newTaskId()
          // Do various bookkeeping
          copiesRunning(index) += 1
          val attemptNum = taskAttempts(index).size
          val info = new TaskInfo(taskId, index, attemptNum, curTime,
            execId, host, taskLocality, speculative)
          taskInfos(taskId) = info
          taskAttempts(index) = info :: taskAttempts(index)
          // Update our locality level for delay scheduling
          // NO_PREF will not affect the variables related to delay scheduling
          if (maxLocality != TaskLocality.NO_PREF) {
            currentLocalityIndex = getLocalityIndex(taskLocality)
            lastLaunchTime = curTime
          }
          // Serialize and return the task
          3. 然后上面的处理完成后，就开始序列化这个任务了
          val startTime = clock.getTimeMillis()
          val serializedTask: ByteBuffer = try {
            Task.serializeWithDependencies(task, sched.sc.addedFiles, sched.sc.addedJars, ser)
          } catch {
            // If the task cannot be serialized, then there's no point to re-attempt the task,
            // as it will always fail. So just abort the whole task-set.
            case NonFatal(e) =>
              val msg = s"Failed to serialize task $taskId, not attempting to retry it."
              logError(msg, e)
              abort(s"$msg Exception during serialization: $e")
              throw new TaskNotSerializableException(e)
          }
          if (serializedTask.limit > TaskSetManager.TASK_SIZE_TO_WARN_KB * 1024 &&
              !emittedTaskSizeWarning) {
            emittedTaskSizeWarning = true
            logWarning(s"Stage ${task.stageId} contains a task of very large size " +
              s"(${serializedTask.limit / 1024} KB). The maximum recommended task size is " +
              s"${TaskSetManager.TASK_SIZE_TO_WARN_KB} KB.")
          }
          4. 标明这个任务已启动
          addRunningTask(taskId)

          // We used to log the time it takes to serialize the task, but task size is already
          // a good proxy to task serialization time.
          // val timeTaken = clock.getTime() - startTime
          val taskName = s"task ${info.id} in stage ${taskSet.id}"
          logInfo(s"Starting $taskName (TID $taskId, $host, partition ${task.partitionId}," +
            s"$taskLocality, ${serializedTask.limit} bytes)")
          5. 对于dagScheduler来说，到这里上一个stage就完成了，这里会发送BeginEvent的事件来处理下一个stage了。
          sched.dagScheduler.taskStarted(task, info)
          6. 然后返回
          return Some(new TaskDescription(taskId = taskId, attemptNumber = attemptNum, execId,
            taskName, index, serializedTask))
        }
        case _ =>
      }
    }
    None
  } 
```
getAllowedLocalityLevel方法：
```
 private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    // Remove the scheduled or finished tasks lazily
    def tasksNeedToBeScheduledFrom(pendingTaskIds: ArrayBuffer[Int]): Boolean = {
      var indexOffset = pendingTaskIds.size
      while (indexOffset > 0) {
        indexOffset -= 1
        val index = pendingTaskIds(indexOffset)
        1. 如果存在还没有运行的task
        if (copiesRunning(index) == 0 && !successful(index)) {
          return true
        } else {
         2.如果不存在了，就将其从pendingTaskIds删除掉，这里删除的中的task
          pendingTaskIds.remove(indexOffset)
        }
      }
      false
    }
    // Walk through the list of tasks that can be scheduled at each location and returns true
    // if there are any tasks that still need to be scheduled. Lazily cleans up tasks that have
    // already been scheduled.
    def moreTasksToRunIn(pendingTasks: HashMap[String, ArrayBuffer[Int]]): Boolean = {
      val emptyKeys = new ArrayBuffer[String]
      val hasTasks = pendingTasks.exists {
        case (id: String, tasks: ArrayBuffer[Int]) =>
          if (tasksNeedToBeScheduledFrom(tasks)) {
            true
          } else {
            emptyKeys += id
            false
          }
      }
      // The key could be executorId, host or rackId
     3.这里删除的是分配到executorId, host or rackId上的task
      emptyKeys.foreach(id => pendingTasks.remove(id))
      hasTasks
    }

    while (currentLocalityIndex < myLocalityLevels.length - 1) {
      val moreTasks = myLocalityLevels(currentLocalityIndex) match {
        case TaskLocality.PROCESS_LOCAL => moreTasksToRunIn(pendingTasksForExecutor)
        case TaskLocality.NODE_LOCAL => moreTasksToRunIn(pendingTasksForHost)
        case TaskLocality.NO_PREF => pendingTasksWithNoPrefs.nonEmpty
        case TaskLocality.RACK_LOCAL => moreTasksToRunIn(pendingTasksForRack)
      }
      if (!moreTasks) {
        // This is a performance optimization: if there are no more tasks that can
        // be scheduled at a particular locality level, there is no point in waiting
        // for the locality wait timeout (SPARK-4939).
        lastLaunchTime = curTime
        logDebug(s"No tasks for locality level ${myLocalityLevels(currentLocalityIndex)}, " +
          s"so moving to locality level ${myLocalityLevels(currentLocalityIndex + 1)}")
       4. 如果这个locality level 下没有task，则转为下一个locality level 
        currentLocalityIndex += 1
      } else if (curTime - lastLaunchTime >= localityWaits(currentLocalityIndex)) {
        5. 如果有task，且大于等待的时间了，则转为下一个locality level 
        // Jump to the next locality level, and reset lastLaunchTime so that the next locality
        // wait timer doesn't immediately expire
        lastLaunchTime += localityWaits(currentLocalityIndex)
        logDebug(s"Moving to ${myLocalityLevels(currentLocalityIndex + 1)} after waiting for " +
          s"${localityWaits(currentLocalityIndex)}ms")
        currentLocalityIndex += 1
      } else {
        6. 否则就返回这个task的locality level 
        return myLocalityLevels(currentLocalityIndex)
      }
    }
    7. 如果没有更多的task了则返回这个locality level 
    myLocalityLevels(currentLocalityIndex)
  }

```
dequeueTask方法：
```
 private def dequeueTask(execId: String, host: String, maxLocality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value, Boolean)] =
  {
   1. 这些就是正常的获取相应的task及其Locality Level，需要注意的是后面对慢任务的操作
    for (index <- dequeueTaskFromList(execId, getPendingTasksForExecutor(execId))) {
      return Some((index, TaskLocality.PROCESS_LOCAL, false))
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      for (index <- dequeueTaskFromList(execId, getPendingTasksForHost(host))) {
        return Some((index, TaskLocality.NODE_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      // Look for noPref tasks after NODE_LOCAL for minimize cross-rack traffic
      for (index <- dequeueTaskFromList(execId, pendingTasksWithNoPrefs)) {
        return Some((index, TaskLocality.PROCESS_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- dequeueTaskFromList(execId, getPendingTasksForRack(rack))
      } {
        return Some((index, TaskLocality.RACK_LOCAL, false))
      }
    }

    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
      for (index <- dequeueTaskFromList(execId, allPendingTasks)) {
        return Some((index, TaskLocality.ANY, false))
      }
    }

    // find a speculative task if all others tasks have been scheduled
   2. 如果maxLocality通过getAllowedLocalityLevel改变过，且不满足前面的条件，那就说明它这里是慢任务，所以这个慢任务再启动的话是可以有更高的LocalityLevel的
    dequeueSpeculativeTask(execId, host, maxLocality).map {
      case (taskIndex, allowedLocality) => (taskIndex, allowedLocality, true)}
  }

```

ok，写到这里就都完成了， resourceOffers第一部分，从这个标题处一定要至少看10遍左右的源码，要不然真心看不懂，OK！ 今天就到这里吧！这一章又不明白的地方大家可以直接给我留言啊！

