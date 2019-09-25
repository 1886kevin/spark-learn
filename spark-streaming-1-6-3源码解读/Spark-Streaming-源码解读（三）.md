##  jobScheduler中的start方法

这节课主要讲这个start方法，但是在讲之前呢，先看一下我们使用的这个类的初始化，jobScheduler对象是在SparkStreamContext中new出来的。所以直接看一下这个类在初始化的时候干了什么：

```
   // Use of ConcurrentHashMap.keySet later causes an odd runtime problem due to Java 7/8 diff
  // https://gist.github.com/AlainODea/1375759b8720a3f9f094
  1. 该变量存储的是一个batch中的job的集合，
     这里需要指出的是在编译的时候需要指定如果你采用java8编译然后在java7上运行的话，
     需要明确指定bootstrap class path，否则会有异常。因为ConcurrentHashMap在java7和java8中是不同的，
     详细信息：请看上面的网址中的解释。
  private val jobSets: java.util.Map[Time, JobSet] = new ConcurrentHashMap[Time, JobSet]
  2. 该变量只的是可以并行运行的job，默认为1
  private val numConcurrentJobs = ssc.conf.getInt("spark.streaming.concurrentJobs", 1)
  3. 后台启动一个含有numConcurrentJobs个线程的线程池
  private val jobExecutor =
    ThreadUtils.newDaemonFixedThreadPool(numConcurrentJobs, "streaming-job-executor")
  4. 后续详细介绍JobGenerator，很重要
  private val jobGenerator = new JobGenerator(this)
  4. 系统时钟
  val clock = jobGenerator.clock
  5. Streaming程序的总线，用来完成整个job的调度
  val listenerBus = new StreamingListenerBus()
```
接下来直接看 jobScheduler中start方法的工作内容：

```
  def start(): Unit = synchronized {
    if (eventLoop != null) return // scheduler has already been started
    1. 首先会启动一个loop，这个loop的作用就是接收关于job的事件消息，从而驱动整个job的进行，其接收的信息包括：JobStarted，JobCompleted，ErrorReported。（整个spark系统的运行就是通过消息驱动的方式来完成的）
    logDebug("Starting JobScheduler")
    eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler") {
      override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = reportError("Error in job scheduler", e)
    }
    eventLoop.start()
    // attach rate controllers of input streams to receive batch completion updates
   //  rateController其实就是用来调节inputDStream接收数据的
    2. 这里将rateController添加到streaming的监听器中，用来在job完成后调节接收数据的速率
    for {
      inputDStream <- ssc.graph.getInputStreams
      rateController <- inputDStream.rateController
    } ssc.addStreamingListener(rateController)
    
    3. 启动streaming程序的监听总线
    listenerBus.start(ssc.sparkContext)
    4. ReceiverTracker是用来管理ReceiverInputDStreams中的receivers的执行
    receiverTracker = new ReceiverTracker(ssc)
    5. 用来管理 input streams，还有它们的数据统计信息
    inputInfoTracker = new InputInfoTracker(ssc)
    6. 下面的启动接下来分两部分来分析：
    receiverTracker.start()
    jobGenerator.start()
    logInfo("Started JobScheduler")
  }

```
## receiver的启动
直接看receiverTracker.start()代码了：
```
  /** Start the endpoint and receiver execution thread. */
  def start(): Unit = synchronized {
    if (isTrackerStarted) {
      throw new SparkException("ReceiverTracker already started")
    }
   
    if (!receiverInputStreams.isEmpty) {
      1. 创建一个RPC的Endpoint，用来处理receiver的启动
      endpoint = ssc.env.rpcEnv.setupEndpoint(
        "ReceiverTracker", new ReceiverTrackerEndpoint(ssc.env.rpcEnv))
      if (!skipReceiverLaunch) launchReceivers()
      logInfo("ReceiverTracker started")
      2. receiver启动后修改其状态
      trackerState = Started
    }
  }
```
上述代码会调用launchReceivers这个方法：
```
  private def launchReceivers(): Unit = {
    val receivers = receiverInputStreams.map(nis => {
      val rcvr = nis.getReceiver()
      rcvr.setReceiverId(nis.id)
      rcvr
    })
    1. 首先先启动一个测试类的job，用来确保所有的slaves都已经注册，并防止所有的receiver都分配到一个slave上。（个人觉得没有必要，当然它这里官方也已经提出这不是一个很好的方法，建议有时间，看下这个方法上的注释）
    runDummySparkJob()

    logInfo("Starting " + receivers.length + " receivers")
    2. 向前面注册的endpoint发送启动receivers的消息
    endpoint.send(StartAllReceivers(receivers))
  }
```
因为前面endpoint是这样来创建的：

```
endpoint = ssc.env.rpcEnv.setupEndpoint(  "ReceiverTracker", new ReceiverTrackerEndpoint(ssc.env.rpcEnv))
```
然后直接看这个ReceiverTrackerEndpoint（一个RPC通信的实体）
```
   override def receive: PartialFunction[Any, Unit] = {
      // Local messages
      case StartAllReceivers(receivers) =>
        1. 根据分配策略为每一个receiver分配可用的Executor，并得到Executor的位置信息
        val scheduledLocations = schedulingPolicy.scheduleReceivers(receivers, getExecutors)
        for (receiver <- receivers) {
        2. 获取当前receiver分配的Executor，其实它这里的Executor是TaskLocation，也就是Executor运行的位置信息。这里也可以理解成将分配到这个位置的Executor。
          val executors = scheduledLocations(receiver.streamId)
        3. 更新当前的已分配Executor的信息
          updateReceiverScheduledExecutors(receiver.streamId, executors)
        4. 更新已分配的receiver的位置信息  
         receiverPreferredLocations(receiver.streamId) = receiver.preferredLocation
        5. 启动receiver
          startReceiver(receiver, executors)
        }
```
上面是一个for循环，也就是说receiver是一个个启动的：
```
private def startReceiver(
        receiver: Receiver[_],
        scheduledLocations: Seq[TaskLocation]): Unit = {
      1. 判断是否应该启动receiver的方法
      def shouldStartReceiver: Boolean = {
        // It's okay to start when trackerState is Initialized or Started
        !(isTrackerStopping || isTrackerStopped)
      }
    
      val receiverId = receiver.streamId
      2. 判断是否能启动receiver
      if (!shouldStartReceiver) {
        onReceiverJobFinish(receiverId)
        return
      }
      //checkpoint 目录
      val checkpointDirOption = Option(ssc.checkpointDir)
      3. hadoop 配置信息，因为现在还是在driver端，而receiver的启动在worker端，所有需要序列化该配置
      val serializableHadoopConf =
        new SerializableConfiguration(ssc.sparkContext.hadoopConfiguration)
      
      4. 这才是真正启动receiver的方法，注意：它是在worker端启动的。（当然如果在kafka中你是用direct API的方式也有可能在driver端启动）
      // Function to start the receiver on the worker node
      val startReceiverFunc: Iterator[Receiver[_]] => Unit =
        (iterator: Iterator[Receiver[_]]) => {
          if (!iterator.hasNext) {
            throw new SparkException(
              "Could not start receiver as object not found.")
           }
       5.  这里是通过一个向spark core 提交job方式来，而spark core具体是通过task来完成job的
            if (TaskContext.get().attemptNumber() == 0) {
            val receiver = iterator.next()
            assert(iterator.hasNext == false)
       6.  这里才是真正的启动我们的receiver，千万注意它是在worker上运行的
            val supervisor = new ReceiverSupervisorImpl(
              receiver, SparkEnv.get, serializableHadoopConf.value, checkpointDirOption)
            supervisor.start()
            supervisor.awaitTermination()
          } else {
            // It's restarted by TaskScheduler, but we want to reschedule it again. So exit it.
          }
        }

      // Create the RDD using the scheduledLocations to run the receiver in a Spark job
      7. 因为我们的spark core的计算全部都是在RDD的基础上计算的，这个RDD的重要作用就是指名partition的location信息，spark core在部署job的时候，会通过这个location信息，使job在location的节点上运行（其实从这里可以看出RDD的partition中是可以没有数据的）
      val receiverRDD: RDD[Receiver[_]] =
        if (scheduledLocations.isEmpty) {
          ssc.sc.makeRDD(Seq(receiver), 1)
        } else {
          val preferredLocations = scheduledLocations.map(_.toString).distinct
          ssc.sc.makeRDD(Seq(receiver -> preferredLocations))
        }
      receiverRDD.setName(s"Receiver $receiverId")
      ssc.sparkContext.setJobDescription(s"Streaming job running receiver $receiverId")
      ssc.sparkContext.setCallSite(Option(ssc.getStartSite()).getOrElse(Utils.getCallSite()))
      8. 在这里就明确的指明了receiver的启动是通过submitJob来完成的
      val future = ssc.sparkContext.submitJob[Receiver[_], Unit, Unit](
        receiverRDD, startReceiverFunc, Seq(0), (_, _) => Unit, ())
      // We will keep restarting the receiver job until ReceiverTracker is stopped
     9. 这里没什么要说明的，就是这个函数下面有一个submitJobThreadPool，其实这是onComplete方法的一个隐式转换，最终使这段代码运行在submitJobThreadPool这个线程池分配的线程中。因为这里的future是异步的，不能阻塞整个代码的执行，需要将其放到另外一个线程中，等待job完成，再进行后续处理。
      future.onComplete {
        case Success(_) =>
          if (!shouldStartReceiver) {
            onReceiverJobFinish(receiverId)
          } else {
            logInfo(s"Restarting Receiver $receiverId")
            self.send(RestartReceiver(receiver))
          }
        case Failure(e) =>
          if (!shouldStartReceiver) {
            onReceiverJobFinish(receiverId)
          } else {
            logError("Receiver has been stopped. Try to restart it.", e)
            logInfo(s"Restarting Receiver $receiverId")
            self.send(RestartReceiver(receiver))
          }
      }(submitJobThreadPool)
      logInfo(s"Receiver ${receiver.streamId} started")
    }
```
recever最终的启动是上文中的 supervisor.start()，直接看代码：

```
  /** Start the supervisor */
  def start() {
    onStart()
    startReceiver()
  }
```
其中onStart()的真正实现是在

```
override protected def onStart() {  
   registeredBlockGenerators.foreach { _.start() }
}
```
其中startReceiver()的真正实现是在
```
  def startReceiver(): Unit = synchronized {
    try {
      if (onReceiverStart()) {
        logInfo("Starting receiver")
        receiverState = Started
       // 也就是这里，这里的实现是在SocketReceiver中的onStart方法中
        receiver.onStart()
        logInfo("Called receiver onStart")
      } else {
        // The driver refused us
        stop("Registered unsuccessfully because Driver refused to start receiver " + streamId, None)
      }
    } catch {
      case NonFatal(t) =>
        stop("Error starting receiver " + streamId, Some(t))
    }
  }
```
SocketReceiver中的onStart方法：
```
   def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() { receive() }
    }.start()
  }
```
这之后这也就完成了整个receiver的启动过程了，细心的你一定会发现上文中提到的onStart在干嘛？也就是这个
```
override protected def onStart() {  
   registeredBlockGenerators.foreach { _.start() }
}
```
Ok，我们现在开始分析这个东西. 这个registeredBlockGenerators是在ReceiverSupervisorImpl实例化的：

```
 1. 这里指出registeredBlockGenerators是一个BlockGenerator，且对它的操作必须是同步的。
 private val registeredBlockGenerators = new mutable.ArrayBuffer[BlockGenerator]
    with mutable.SynchronizedBuffer[BlockGenerator]

  /** Divides received data records into data blocks for pushing in BlockManager. */
  2. 我不知道它这里为什么会成为listener，其实它就是提供了一套将buffer数据添加到blockManager中的指令而已
  private val defaultBlockGeneratorListener = new BlockGeneratorListener {
    def onAddData(data: Any, metadata: Any): Unit = { }

    def onGenerateBlock(blockId: StreamBlockId): Unit = { }

    def onError(message: String, throwable: Throwable) {
      reportError(message, throwable)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]) {
      pushArrayBuffer(arrayBuffer, None, Some(blockId))
    }
  }
  private val defaultBlockGenerator = createBlockGenerator(defaultBlockGeneratorListener)
  
  ....
  3. 这里就创建BlockGenerator的地方了，BlockGenerator用于将buffer数据转化成到block中，当然这里也会把这个BlockGenerator添加到registeredBlockGenerators中
    override def createBlockGenerator(
      blockGeneratorListener: BlockGeneratorListener): BlockGenerator = {
    // Cleanup BlockGenerators that have already been stopped
    registeredBlockGenerators --= registeredBlockGenerators.filter{ _.isStopped() }

    val newBlockGenerator = new BlockGenerator(blockGeneratorListener, streamId, env.conf)
    registeredBlockGenerators += newBlockGenerator
    newBlockGenerator
  }
```
所以上文中的启动，其实就是调用的BlockGenerator中的start方法。

先看BlockGenerator初始化的一些信息：
```
  1. 自定义一些状态，其后会设置为Initialized
  private object GeneratorState extends Enumeration {
    type GeneratorState = Value
    val Initialized, Active, StoppedAddingData, StoppedGeneratingBlocks, StoppedAll = Value
  }
  import GeneratorState._
  2. spark.streaming.blockInterval指多久处理一次接receiver收到的buffer，默认200ms
  private val blockIntervalMs = conf.getTimeAsMs("spark.streaming.blockInterval", "200ms")
  require(blockIntervalMs > 0, s"'spark.streaming.blockInterval' should be a positive value")
 3. 具体执行2中提到的操作
  private val blockIntervalTimer =
    new RecurringTimer(clock, blockIntervalMs, updateCurrentBuffer, "BlockGenerator")
 4. 默认该类中最多能存10个block块
  private val blockQueueSize = conf.getInt("spark.streaming.blockQueueSize", 10)
  private val blocksForPushing = new ArrayBlockingQueue[Block](blockQueueSize)
 5. 将block放到BlockManager中的线程，如果BlockGenerator未停止会一些while循环来处理
  private val blockPushingThread = new Thread() { override def run() { keepPushingBlocks() } }
 6. 要处理的buffer
  @volatile private var currentBuffer = new ArrayBuffer[Any]
 7.当前状态
  @volatile private var state = Initialized

```
然后再看这个start方法：
其实就是启动这个更新buffer的线程和处理block的线程
```
  /** Start block generating and pushing threads. */
  def start(): Unit = synchronized {
    if (state == Initialized) {
      state = Active
      blockIntervalTimer.start()
      blockPushingThread.start()
      logInfo("Started BlockGenerator")
    } else {
      throw new SparkException(
        s"Cannot start BlockGenerator as its not in the Initialized state [state = $state]")
    }
  }
```
由上面可以看出，这个处理buffer的线程是通过updateCurrentBuffer来执行的：

```
private def updateCurrentBuffer(time: Long): Unit = {
    try {
      var newBlock: Block = null
      synchronized {
        if (currentBuffer.nonEmpty) {
          1. 其实它这里在不停在将原来的buffer放到block中，然后又重新new 一个ArrayArrayBuffer来等待存放新的buffer。
          val newBlockBuffer = currentBuffer
          currentBuffer = new ArrayBuffer[Any]
          val blockId = StreamBlockId(receiverId, time - blockIntervalMs)
          2. 会发消息给监听对象
          listener.onGenerateBlock(blockId)
          newBlock = new Block(blockId, newBlockBuffer)
        }
      }

      if (newBlock != null) {
        3. 将newBlock放入blocksForPushing中，等待处理。这里需要注意的是如果放入blocksForPushing的block满了的时候，这个线程是要挂起的
        blocksForPushing.put(newBlock)  // put is blocking when queue is full
      }
    } catch {
      case ie: InterruptedException =>
        logInfo("Block updating timer thread was interrupted")
      case e: Exception =>
        reportError("Error in block updating thread", e)
    }
  }
```
然后这个处理block的线程，是通过keepPushingBlocks这个方法来操作的
```
/** Keep pushing blocks to the BlockManager. */
  private def keepPushingBlocks() {
    logInfo("Started block pushing thread")

    def areBlocksBeingGenerated: Boolean = synchronized {
      state != StoppedGeneratingBlocks
    }

    try {
      // While blocks are being generated, keep polling for to-be-pushed blocks and push them.
      1. 如果blcok一直生成的话，就一直把生成的block放到blockManager中
      while (areBlocksBeingGenerated) {
        Option(blocksForPushing.poll(10, TimeUnit.MILLISECONDS)) match {
          case Some(block) => pushBlock(block)
          case None =>
        }
      }

      // At this point, state is StoppedGeneratingBlock. So drain the queue of to-be-pushed blocks.
      logInfo("Pushing out the last " + blocksForPushing.size() + " blocks")
      2. 如果blcok的生成停止了，就把blocksForPushing队列中剩下的block放到blockManager中。
      while (!blocksForPushing.isEmpty) {
        val block = blocksForPushing.take()
        logDebug(s"Pushing block $block")
      3. 具体把block放到blockManager中的方法
        pushBlock(block)
        logInfo("Blocks left to push " + blocksForPushing.size())
      }
      logInfo("Stopped block pushing thread")
    } catch {
      case ie: InterruptedException =>
        logInfo("Block pushing thread was interrupted")
      case e: Exception =>
        reportError("Error in block pushing thread", e)
    }
  }
```
下面就直接看这个pushBlock方法吧：
```
  private def pushBlock(block: Block) {
    listener.onPushBlock(block.id, block.buffer)
    logInfo("Pushed block " + block.id)
  }
```
通过监听对象来驱动pushBlock的操作，这里的这个listener，就是前文提到过的那个defaultBlockGeneratorListener

```
   /** Divides received data records into data blocks for pushing in BlockManager. */
  private val defaultBlockGeneratorListener = new BlockGeneratorListener {
    def onAddData(data: Any, metadata: Any): Unit = { }

    def onGenerateBlock(blockId: StreamBlockId): Unit = { }

    def onError(message: String, throwable: Throwable) {
      reportError(message, throwable)
    }

    def onPushBlock(blockId: StreamBlockId, arrayBuffer: ArrayBuffer[_]) {
      pushArrayBuffer(arrayBuffer, None, Some(blockId))
    }
  }
```
通过defaultBlockGeneratorListener会调用pushArrayBuffer，然后我们在看这个pushArrayBuffer
```
  def pushArrayBuffer(
      arrayBuffer: ArrayBuffer[_],
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    pushAndReportBlock(ArrayBufferBlock(arrayBuffer), metadataOption, blockIdOption)
  }
```
没办法，还得继续走：
```
 def pushAndReportBlock(
      receivedBlock: ReceivedBlock,
      metadataOption: Option[Any],
      blockIdOption: Option[StreamBlockId]
    ) {
    val blockId = blockIdOption.getOrElse(nextBlockId)
    val time = System.currentTimeMillis
    1. 将block存储在blockManager中
    val blockStoreResult = receivedBlockHandler.storeBlock(blockId, receivedBlock)
    logDebug(s"Pushed block $blockId in ${(System.currentTimeMillis - time)} ms")
    val numRecords = blockStoreResult.numRecords
    val blockInfo = ReceivedBlockInfo(streamId, numRecords, metadataOption, blockStoreResult)
     2. 向trackerEndpoint汇报AddBlock
    trackerEndpoint.askWithRetry[Boolean](AddBlock(blockInfo))
    logDebug(s"Reported block $blockId")
  }
  ```
上面有两个比较重要的操作：先说1，这里如果打开wal的方式就使用wal的存储操作，如果没打开就使用默认的操作，这里先介绍默认的方式，后续在介绍wal的方式。

```
 def storeBlock(blockId: StreamBlockId, block: ReceivedBlock): ReceivedBlockStoreResult = {

    var numRecords = None: Option[Long]

    val putResult: Seq[(BlockId, BlockStatus)] = block match {
      case ArrayBufferBlock(arrayBuffer) =>
        numRecords = Some(arrayBuffer.size.toLong)
        blockManager.putIterator(blockId, arrayBuffer.iterator, storageLevel,
          tellMaster = true)

```
到此就真正的把接收到的buffer添加到blockManager中了，累死我了！不过很开心把一些细节都摸透了。当然这里关于blockManager这个组件，我会在Spark Core的章节给大家讲明白的。
先写到这吧！ 太晚了，没想到光写个receiver写了一晚上。看来job的生成又得拖到明天了。
