这篇博客就不得不分析job的生成了，因为就还剩就还剩下这个模块了。job的生成，其实就是在JobScheduler的启动方法中，通过JobGenerator中的start方法来操作的，下面先看JobScheduler中的start方法：

### 初始化工作

```
def start(): Unit = synchronized {
    
    ......
    
    jobGenerator.start()
    logInfo("Started JobScheduler")
  }

```
在这里我就不全部贴出来了，因为之前的内容，前面的四篇博客都讲过了。因为这里是JobGenerator的start方法，先看一些JobGenerator初始化的一些内容：


```
/**
 * This class generates jobs from DStreams as well as drives checkpointing and cleaning
 * up DStream metadata.
 */
private[streaming]
class JobGenerator(jobScheduler: JobScheduler) extends Logging {
  1. 获取应用中的SparkStreamingContext，SparkConf和DStreamGraph
  private val ssc = jobScheduler.ssc
  private val conf = ssc.conf
  private val graph = ssc.graph
  2. 实例化自己应用的时钟类   
  val clock = {
    val clockClass = ssc.sc.conf.get(
      "spark.streaming.clock", "org.apache.spark.util.SystemClock")
    try {
      Utils.classForName(clockClass).newInstance().asInstanceOf[Clock]
    } catch {
      case e: ClassNotFoundException if clockClass.startsWith("org.apache.spark.streaming") =>
        val newClockClass = clockClass.replace("org.apache.spark.streaming", "org.apache.spark")
        Utils.classForName(newClockClass).newInstance().asInstanceOf[Clock]
    }
  }
  3. 用来不断产生GenerateJobs事件的时钟
  private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")

  // This is marked lazy so that this is initialized after checkpoint duration has been set
  // in the context and the generator has been started.
  4. 标记是否需要checkpoint
  private lazy val shouldCheckpoint = ssc.checkpointDuration != null && ssc.checkpointDir != null
  5. checkpointWriter是用来具体进行checkpoint的时候的写实现
  private lazy val checkpointWriter = if (shouldCheckpoint) {
    new CheckpointWriter(this, ssc.conf, ssc.checkpointDir, ssc.sparkContext.hadoopConfiguration)
  } else {
    null
  }
```
下面就直接点开这个start方法，看它处理的内容：


```
/** Start generation of jobs */
  def start(): Unit = synchronized {
    if (eventLoop != null) return // generator has already been started

    // Call checkpointWriter here to initialize it before eventLoop uses it to avoid a deadlock.
    // See SPARK-10125
    checkpointWriter
    1. 用来处理JobGeneratorEvent事件的一个loop，它的实现就是在后台启动了一个线程从时间队列中获取事件，加以处理
    eventLoop = new EventLoop[JobGeneratorEvent]("JobGenerator") {
      override protected def onReceive(event: JobGeneratorEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = {
        jobScheduler.reportError("Error in job generator", e)
      }
    }
    eventLoop.start()
    2. 这里如果有开启checkpoint，他就会从checkpoint的目录中读取信息
    if (ssc.isCheckpointPresent) {
      restart()
    } else {
      startFirstTime()
    }
  }
```
当然这里是第一次启动，直接看这个startFirstTime方法：

```
private def startFirstTime() {
    val startTime = new Time(timer.getStartTime())
    graph.start(startTime - graph.batchDuration)
    timer.start(startTime.milliseconds)
    logInfo("Started JobGenerator at " + startTime)
  }

```
这里我们先看graph.start(startTime - graph.batchDuration)，直接看它的实现：


```
 def start(time: Time) {
    this.synchronized {
      require(zeroTime == null, "DStream graph computation already started")
      1. 配置0时刻为当前时刻
      zeroTime = time
      2. 配置开始时刻为当前时刻
      startTime = time
      3.关于outputStreams和inputStreams我们接下来详细说明
      outputStreams.foreach(_.initialize(zeroTime))
      outputStreams.foreach(_.remember(rememberDuration))
      outputStreams.foreach(_.validateAtStart)
      inputStreams.par.foreach(_.start())
    }
  }
```
首先看inputStream是在哪里添加到graph中的：
这里从从例子的最开始看socketTextStream的继承关系
```
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

   1.调用socketTextStream方法：
  def socketTextStream(
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[String] = withNamedScope("socket text stream") {
    socketStream[String](hostname, port, SocketReceiver.bytesToLines, storageLevel)
  }
   
  2. 调用socketStream方法：
  def socketStream[T: ClassTag](
      hostname: String,
      port: Int,
      converter: (InputStream) => Iterator[T],
      storageLevel: StorageLevel
    ): ReceiverInputDStream[T] = {
    new SocketInputDStream[T](this, hostname, port, converter, storageLevel)
  }
  3.SocketInputDStream继承自ReceiverInputDStream
  class SocketInputDStream[T: ClassTag](
    ssc_ : StreamingContext,
    host: String,
    port: Int,
    bytesToObjects: InputStream => Iterator[T],
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[T](ssc_) {

  def getReceiver(): Receiver[T] = {
    new SocketReceiver(host, port, bytesToObjects, storageLevel)
  }
 4. ReceiverInputDStream继承自InputDStream
  abstract class ReceiverInputDStream[T: ClassTag](ssc_ : StreamingContext)
  extends InputDStream[T](ssc_) {
  
  ...
  5. 在InputDStream中实例化的时候将其添加到了graph中
  abstract class InputDStream[T: ClassTag] (ssc_ : StreamingContext)
  extends DStream[T](ssc_) {

  private[streaming] var lastValidTime: Time = null

  ssc.graph.addInputStream(this)
  ...
```
接下来再看OutputDStream如何添加到graph中的，还是从最初的代码开始：


```
1. 原始代码中的print方法
wordCounts.print()

2.print方法的具体实现
def print(): Unit = ssc.withScope {
    print(10)
}

def print(num: Int): Unit = ssc.withScope {
    def foreachFunc: (RDD[T], Time) => Unit = {
      (rdd: RDD[T], time: Time) => {
        val firstNum = rdd.take(num + 1)
        // scalastyle:off println
        println("-------------------------------------------")
        println("Time: " + time)
        println("-------------------------------------------")
        firstNum.take(num).foreach(println)
        if (firstNum.length > num) println("...")
        println()
        // scalastyle:on println
      }
    }
    foreachRDD(context.sparkContext.clean(foreachFunc), displayInnerRDDOps = false)
  }
 3. 上面会调用foreachRDD方法，而在这里new的ForEachDStream会向graph进行注册register
private def foreachRDD(
      foreachFunc: (RDD[T], Time) => Unit,
      displayInnerRDDOps: Boolean): Unit = {
    new ForEachDStream(this,
      context.sparkContext.clean(foreachFunc, false), displayInnerRDDOps).register()
  }
4. 在register方法的具体实现中就将这个OutputStream添加到了graph中
private[streaming] def register(): DStream[T] = {
    ssc.graph.addOutputStream(this)
    this
  }
```
然后回过头来继续看前面的这段代码：

```
 def start(time: Time) {
    this.synchronized {
      require(zeroTime == null, "DStream graph computation already started")
      1. 配置0时刻为当前时刻
      zeroTime = time
      2. 配置开始时刻为当前时刻
      startTime = time
      3.关于outputStreams和inputStreams我们接下来详细说明
      outputStreams.foreach(_.initialize(zeroTime))
      outputStreams.foreach(_.remember(rememberDuration))
      outputStreams.foreach(_.validateAtStart)
      inputStreams.par.foreach(_.start())
    }
  }
```
这里其实只是执行了一系列初始化的工作：
1. 将所有的outputStreams都initialize，初始化首次执行时间，依赖的DStream一并设置。
2. 如果设置了duration，将所有的outputStreams都remember，依赖的DStream一并设置
3. 启动前验证，主要是验证chechpoint设置是否冲突以及各种Duration
将所有的inputStreams启动；读者扫描了下目前版本1.6.3InputDStraem及其所有的子类。start方法啥都没做。因为在Streaming中，inputStreams都已经交由ReceiverTracker管理了。

### job的生成
然后再回到前面提到的
```
    timer.start(startTime.milliseconds)
```
这里这个timer是前面提到的，也就是

```
    private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,
    longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")
```
这里每个一个周期它将会向eventLoop 提交一个GenerateJobs的事件，然后eventLoop会调用处理事件的方法

```
 /** Processes all events */
  private def processEvent(event: JobGeneratorEvent) {
    logDebug("Got event " + event)
    event match {
      case GenerateJobs(time) => generateJobs(time)
      case ClearMetadata(time) => clearMetadata(time)
      case DoCheckpoint(time, clearCheckpointDataLater) =>
        doCheckpoint(time, clearCheckpointDataLater)
      case ClearCheckpointData(time) => clearCheckpointData(time)
    }
  }
  
```
执行generateJobs方法：


```
/** Generate jobs and perform checkpoint for the given `time`.  */
  private def generateJobs(time: Time) {
    // Set the SparkEnv in this thread, so that job generation code can access the environment
    // Example: BlockRDDs are created in this thread, and it needs to access BlockManager
    // Update: This is probably redundant after threadlocal stuff in SparkEnv has been removed.
    1. 这里需要主要的是上面它谈到的注释，就是说后面程序的运行会创建BlockRDDs，所以下面的关键点就是RDD是怎么产生的？
    SparkEnv.set(ssc.env)
    Try {
     2.获取这个Batch中从receivers中获得的blocks
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch
     3. 生成job
      graph.generateJobs(time) // generate jobs using allocated block
    } match {
      case Success(jobs) =>
        val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
        jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos))
      case Failure(e) =>
        jobScheduler.reportError("Error generating jobs for time " + time, e)
    }
    eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
  }
  
```
下面看它是怎么具体生成job的：

```
  def generateJobs(time: Time): Seq[Job] = {
    logDebug("Generating jobs for time " + time)
    val jobs = this.synchronized {
      1.这里注意这里，它是通过outputStream来构建job的
      outputStreams.flatMap { outputStream =>
        val jobOption = outputStream.generateJob(time)
        jobOption.foreach(_.setCallSite(outputStream.creationSite))
        jobOption
      }
    }
    logDebug("Generated " + jobs.length + " jobs for time " + time)
    jobs
  }
```
前文中我们的应用程序构建的DStream依赖关系依次是：

   **SocketInputDStream->FlatMappedDStream->MappedDStream->ShuffledDStream－>ForEachDStream**
然后这里先调用ForEachDStream的generateJob：（从后往前推）

```
  override def generateJob(time: Time): Option[Job] = {
    parent.getOrCompute(time) match {
      case Some(rdd) =>
        val jobFunc = () => createRDDWithLocalProperties(time, displayInnerRDDOps) {
          foreachFunc(rdd, time)
        }
        Some(new Job(time, jobFunc))
      case None => None
    }
  } 
```
它这里会调用parent.getOrCompute(time)方法，而它的parent就是ShuffledDStream
接着看ShuffledDStream的getOrCompute方法：这个类没有实现这个方法就直接看它的继承类DStream中的getOrCompute方法：


```
 private[streaming] final def getOrCompute(time: Time): Option[RDD[T]] = {
    // If RDD was already generated, then retrieve it from HashMap,
    // or else compute the RDD
    generatedRDDs.get(time).orElse {
      // Compute the RDD if time is valid (e.g. correct time in a sliding window)
      // of RDD generation, else generate nothing.
      if (isTimeValid(time)) {

        val rddOption = createRDDWithLocalProperties(time, displayInnerRDDOps = false) {
          // Disable checks for existing output directories in jobs launched by the streaming
          // scheduler, since we may need to write output to an existing directory during checkpoint
          // recovery; see SPARK-4835 for more details. We need to have this call here because
          // compute() might cause Spark jobs to be launched.
          PairRDDFunctions.disableOutputSpecValidation.withValue(true) {
            1. 这里最重要的是这个方法，它这里会直接调用实现类的compute方法，关于其他的代码是什么意思，后面会详细介绍的，这里先弄清这个流程
            compute(time)
          }
        }

        rddOption.foreach { case newRDD =>
          // Register the generated RDD for caching and checkpointing
          if (storageLevel != StorageLevel.NONE) {
            newRDD.persist(storageLevel)
            logDebug(s"Persisting RDD ${newRDD.id} for time $time to $storageLevel")
          }
          if (checkpointDuration != null && (time - zeroTime).isMultipleOf(checkpointDuration)) {
            newRDD.checkpoint()
            logInfo(s"Marking RDD ${newRDD.id} for time $time for checkpointing")
          }
          generatedRDDs.put(time, newRDD)
        }
        rddOption
      } else {
        None
      }
    }
  }
```
然后接着看这个类的compute方法：


```
 override def compute(validTime: Time): Option[RDD[(K, C)]] = {
    parent.getOrCompute(validTime) match {
      case Some(rdd) => Some(rdd.combineByKey[C](
          createCombiner, mergeValue, mergeCombiner, partitioner, mapSideCombine))
      case None => None
    }
  }
```
看这里它又去找父类的getOrCompute方法了，从而又在执行上面重复的操作。
这里需要注意的是根据这个getOrCompute方法，他获得的是RDD，重磅炸弹出现了，DStream的本质就是去实现一个个的RDD来进行计算，所以说DStream只是RDD的一个模版类。

然后这里在哪里停止呢？ 肯定是在inputDStream那停止呀！因为inputDStream前面没有依赖了，它完全针对block生成RDD。下面就是该方法的实现。下面这些英文注释我就不一一翻译了，相信大家能看得懂，如果看不懂的话，奉劝你就不要搞spark了（开玩笑）。

题外话：不过确实需要一点点英语基础！学习spark你需要务必看懂它源码中的注释

```
override def compute(validTime: Time): Option[RDD[T]] = {
    val blockRDD = {

      if (validTime < graph.startTime) {
        // If this is called for any time before the start time of the context,
        // then this returns an empty RDD. This may happen when recovering from a
        // driver failure without any write ahead log to recover pre-failure data.
        new BlockRDD[T](ssc.sc, Array.empty)
      } else {
        // Otherwise, ask the tracker for all the blocks that have been allocated to this stream
        // for this batch
        val receiverTracker = ssc.scheduler.receiverTracker
        val blockInfos = receiverTracker.getBlocksOfBatch(validTime).getOrElse(id, Seq.empty)

        // Register the input blocks information into InputInfoTracker
        val inputInfo = StreamInputInfo(id, blockInfos.flatMap(_.numRecords).sum)
        ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

        // Create the BlockRDD
        createBlockRDD(validTime, blockInfos)
      }
    }
    Some(blockRDD)
  }
```
然后根据这个blockRDD，后面的每一个DStream都会生成一个RDD

new BlockRDD[T](ssc.sc, validBlockIds).map(_.flatMap(flatMapFunc)).map(_.map[U](mapFunc)).combineByKey[C](createCombiner, mergeValue, mergeCombiner, partitioner, mapSideCombine)。

在本例中则为

new BlockRDD[T](ssc.sc, validBlockIds).map(_.flatMap(t=>t.split(" "))).map(_.map[U](t=>(t,1))).combineByKey[C](t=>t, (t1,t2)=>t1+t2, (t1,t2)=>t1+t2,partitioner, true)

而最终的print为

() => foreachFunc(new BlockRDD[T](ssc.sc, validBlockIds).map(_.flatMap(t=>t.split(" "))).map(_.map[U](t=>(t,1))).combineByKey[C](t=>t, (t1,t2)=>t1+t2, (t1,t2)=>t1+t2,partitioner, true),time)

其中foreachFunc为 print方法中的foreachFunc

至此，RDD已经通过DStream实例化完成。其实这里整个计算运算逻辑也已经构建完成了。

上面在实例化RDD的时候，有两个方法没有详细说明下面一一来解释说明：
1. getOrCompute

```
private[streaming] final def getOrCompute(time: Time): Option[RDD[T]] = {
    // If RDD was already generated, then retrieve it from HashMap,
    // or else compute the RDD
    generatedRDDs.get(time).orElse {
      // Compute the RDD if time is valid (e.g. correct time in a sliding window)
      // of RDD generation, else generate nothing.
      if (isTimeValid(time)) {
        1.这里需要注意的是这句话，可能刚一开始大家有点不能理解这段代码，具体的操作其实就是配置好创建RDD前的本地环境。
        val rddOption = createRDDWithLocalProperties(time, displayInnerRDDOps = false) {
          // Disable checks for existing output directories in jobs launched by the streaming
          // scheduler, since we may need to write output to an existing directory during checkpoint
          // recovery; see SPARK-4835 for more details. We need to have this call here because
          // compute() might cause Spark jobs to be launched.
          PairRDDFunctions.disableOutputSpecValidation.withValue(true) {
            compute(time)
          }
        }

        rddOption.foreach { case newRDD =>
          // Register the generated RDD for caching and checkpointing
          if (storageLevel != StorageLevel.NONE) {
            newRDD.persist(storageLevel)
            logDebug(s"Persisting RDD ${newRDD.id} for time $time to $storageLevel")
          }
          if (checkpointDuration != null && (time - zeroTime).isMultipleOf(checkpointDuration)) {
            newRDD.checkpoint()
            logInfo(s"Marking RDD ${newRDD.id} for time $time for checkpointing")
          }
          generatedRDDs.put(time, newRDD)
        }
        rddOption
      } else {
        None
      }
    }
  }

```
直接看这个方法：

```
  protected[streaming] def createRDDWithLocalProperties[U](
      time: Time,
      displayInnerRDDOps: Boolean)(body: => U): U = {
      ...
}
  
```
里面我就不具体说明了，我只分析这个方法体，它这里其实也是scala语法中的柯里化的形式。这这里执行的body就是这面这段代码：

```
     // Disable checks for existing output directories in jobs launched by the streaming
          // scheduler, since we may need to write output to an existing directory during checkpoint
          // recovery; see SPARK-4835 for more details. We need to have this call here because
          // compute() might cause Spark jobs to be launched.
  PairRDDFunctions.disableOutputSpecValidation.withValue(true) {
       compute(time)
  }  
```
这里上面这段注释可能大家不太明白，其实这里就是值在运行这段代码的的时候，是不允许checkpoint进行恢复的，因为有可能我会在那个恢复目录里面进行写操作。
这里有意思的是这个withValue的方法：

```
  def withValue[S](newval: T)(thunk: => S): S = {
    val oldval = value
    tl set newval

    try thunk
    finally tl set oldval
  }
```
看到这你应该很明白了，就是在我执行前我先要把它赋值为true，执行完成后再把值恢复为false。这这里的表现就是，在我的代码执行的时候，不允许程序进行恢复操作。

然后接下来就进行里面的compute(time)计算了。这个方法在调用的时候是通过下面这个方法来创建RDD的

2.createBlockRDD

```
  private[streaming] def createBlockRDD(time: Time, blockInfos: Seq[ReceivedBlockInfo]): RDD[T] = {
    1. 如果blockblockInfo不为空的话，下面的注释写的很清楚，主要有两种生成RDD的方式。一种是经过wal纪录中的block来创建（当driver出错后，可以进行恢复），一种是直接根据block来创建。
    if (blockInfos.nonEmpty) {
      val blockIds = blockInfos.map { _.blockId.asInstanceOf[BlockId] }.toArray

      // Are WAL record handles present with all the blocks
      val areWALRecordHandlesPresent = blockInfos.forall { _.walRecordHandleOption.nonEmpty }

      if (areWALRecordHandlesPresent) {
        // If all the blocks have WAL record handle, then create a WALBackedBlockRDD
        val isBlockIdValid = blockInfos.map { _.isBlockIdValid() }.toArray
        val walRecordHandles = blockInfos.map { _.walRecordHandleOption.get }.toArray
        new WriteAheadLogBackedBlockRDD[T](
          ssc.sparkContext, blockIds, walRecordHandles, isBlockIdValid)
      } else {
        // Else, create a BlockRDD. However, if there are some blocks with WAL info but not
        // others then that is unexpected and log a warning accordingly.
        if (blockInfos.find(_.walRecordHandleOption.nonEmpty).nonEmpty) {
          if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
            logError("Some blocks do not have Write Ahead Log information; " +
              "this is unexpected and data may not be recoverable after driver failures")
          } else {
            logWarning("Some blocks have Write Ahead Log information; this is unexpected")
          }
        }
        val validBlockIds = blockIds.filter { id =>
          ssc.sparkContext.env.blockManager.master.contains(id)
        }
        if (validBlockIds.size != blockIds.size) {
          logWarning("Some blocks could not be recovered as they were not found in memory. " +
            "To prevent such data loss, enabled Write Ahead Log (see programming guide " +
            "for more details.")
        }
        new BlockRDD[T](ssc.sc, validBlockIds)
      }
    } else {
      // If no block is ready now, creating WriteAheadLogBackedBlockRDD or BlockRDD
      // according to the configuration
      if (WriteAheadLogUtils.enableReceiverLog(ssc.conf)) {
        new WriteAheadLogBackedBlockRDD[T](
          ssc.sparkContext, Array.empty, Array.empty, Array.empty)
      } else {
        new BlockRDD[T](ssc.sc, Array.empty)
      }
    }
  }
```
这里要说的就这么多，关于这个方法，其他部分大家看注释把！现在就是根据DStreamGraph生成了我们要处理的数据（RDD）和处理逻辑，当然这里还没有具体执行啊（框架lazy），也就是我们的job。

### 下面直接看job的提交
先从我们上文中的代码说起：

```

  def generateJobs(time: Time): Seq[Job] = {
    logDebug("Generating jobs for time " + time)
    val jobs = this.synchronized {
      outputStreams.flatMap { outputStream =>
        val jobOption = outputStream.generateJob(time)
        jobOption.foreach(_.setCallSite(outputStream.creationSite))
        jobOption
      }
    }
    logDebug("Generated " + jobs.length + " jobs for time " + time)
    jobs
  }

```
在这段代码执行完，我们就获得了要提交的jobs。然后回过头来在JobGenerator中提交job


```

 /** Generate jobs and perform checkpoint for the given `time`.  */
  private def generateJobs(time: Time) {
    // Set the SparkEnv in this thread, so that job generation code can access the environment
    // Example: BlockRDDs are created in this thread, and it needs to access BlockManager
    // Update: This is probably redundant after threadlocal stuff in SparkEnv has been removed.
    SparkEnv.set(ssc.env)
    Try {
      jobScheduler.receiverTracker.allocateBlocksToBatch(time) // allocate received blocks to batch
      graph.generateJobs(time) // generate jobs using allocated block
    } match {
      case Success(jobs) =>
        val streamIdToInputInfos = jobScheduler.inputInfoTracker.getInfo(time)
        jobScheduler.submitJobSet(JobSet(time, jobs, streamIdToInputInfos))
      case Failure(e) =>
        jobScheduler.reportError("Error generating jobs for time " + time, e)
    }
    eventLoop.post(DoCheckpoint(time, clearCheckpointDataLater = false))
  }
```
在这里使用的是Try{}match{}方法，也就是Try中的代码执行成功后，会匹配到 case Success(jobs) =>，然后获取输入流对应的InputInfo的信息，然后就直接提交这个jobset。
```

  def submitJobSet(jobSet: JobSet) {
    if (jobSet.jobs.isEmpty) {
      logInfo("No jobs added for time " + jobSet.time)
    } else {
     1. 触发StreamingListenerBatchSubmitted事件 listenerBus.post(StreamingListenerBatchSubmitted(jobSet.toBatchInfo))
     2.纪录提交的job
      jobSets.put(jobSet.time, jobSet)
     3. 具体在这里执行
      jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job)))
      logInfo("Added jobs for time " + jobSet.time)
    }
  }
 ```
上面可以看出它这里job的执行其实又封装了一个线程JobHandler，从而具体的任务通过这个线程提交给Spark Core运行即可。

在这个JobHandler的run方法中有一个非常重要的操作也就是

```

      PairRDDFunctions.disableOutputSpecValidation.withValue(true) {
            job.run()
}
```
从这里可以看到在job构建和运行的时候均不允许读取checkpoint中的内容。
然后接着看这个 job.run()

```
  def run() {
    _result = Try(func())
  }
```
其实看到这里你可能就懵了，这个func是什么啊？它肯定是我们程序的执行，那他是在哪里传进来的呢？  他当然是在这里实现的了

```
   private[streaming] def generateJob(time: Time): Option[Job] = {
    getOrCompute(time) match {
      case Some(rdd) => {
        val jobFunc = () => {
          val emptyFunc = { (iterator: Iterator[T]) => {} }
          context.sparkContext.runJob(rdd, emptyFunc)
        }
        Some(new Job(time, jobFunc))
      }
      case None => None
    }
  }

```

是吗？ 肯定不是，因为这是DStream的方法，DStream只是一个抽象类，它不是程序的具体实现类。所以你需要看具体的实现类了（也就是outputDStream的实现类），在我们的例子中就是这个ForEachDStream。也就是在生成job的时候实际调用的是ForEachDStream中的generateJob。


```
 override def generateJob(time: Time): Option[Job] = {
    parent.getOrCompute(time) match {
      case Some(rdd) =>
        val jobFunc = () => createRDDWithLocalProperties(time, displayInnerRDDOps) {
          foreachFunc(rdd, time)
        }
        Some(new Job(time, jobFunc))
      case None => None
    }
  }

```

那我们在构建它的时候是怎么构建的呢？这里我直接把代码贴出来：


```
  def print(num: Int): Unit = ssc.withScope {
    def foreachFunc: (RDD[T], Time) => Unit = {
      (rdd: RDD[T], time: Time) => {
        val firstNum = rdd.take(num + 1)
        // scalastyle:off println
        println("-------------------------------------------")
        println("Time: " + time)
        println("-------------------------------------------")
        firstNum.take(num).foreach(println)
        if (firstNum.length > num) println("...")
        println()
        // scalastyle:on println
      }
    }
    foreachRDD(context.sparkContext.clean(foreachFunc), displayInnerRDDOps = false)
  }


  private def foreachRDD(
      foreachFunc: (RDD[T], Time) => Unit,
      displayInnerRDDOps: Boolean): Unit = {
    new ForEachDStream(this,
      context.sparkContext.clean(foreachFunc, false), displayInnerRDDOps).register()
  }

```
根据这个你应该明白了吧！ 上文中提到的func就是上面的foreachFunc这个方法。爽不爽，哈哈！！！！

还有更爽的呢！下面直接看这个方法中有针对RDD的take操作，那take操作是什么呢？我们接着往下走：
这里先不讲它具体的算法，只看它里面的一句内容：


```
def take(num: Int): Array[T] = withScope {
       .......

        val res = sc.runJob(this, (it: Iterator[T]) => it.take(left).toArray, p)
       ......
  }
```
也就是这个runJob，看到这句话，之后的操作就交给Spark Core了。

而上面提到的foreachFunc也就是传入job中func方法。

另外一旦看到这个runJob也就说明关于Spark Streaming的分析也就结束了，没想到写出来会有这么多，本来以为1，2天就能写完呢，如今却写了一个礼拜，不过，这一个礼拜的博客，让我真对spark streaming有了一些更细节的认识。谢谢我自己吧！ 也谢谢spark！  也感谢帮我的人，之后我的博客也会继续写下去的。 再次谢谢大家的支持！！！

关于上面job的生成及计算逻辑 这里给大家分享几张图：


![job1.png](http://upload-images.jianshu.io/upload_images/3736220-ad0e30f9f702672d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

![
![job3.png](http://upload-images.jianshu.io/upload_images/3736220-9f18d44d095f7500.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
](http://upload-images.jianshu.io/upload_images/3736220-d346dc54d57c54b3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


