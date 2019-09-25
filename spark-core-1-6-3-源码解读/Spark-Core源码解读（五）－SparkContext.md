**道生一，一生二，二生三，三生万物。**

今天直接分析SparkPi，上篇博客中最后，通过反射的方法启动了业务逻辑类（SparkPi）中的main方法，下面就直接看这个main方法：

```
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
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
下面会每行代码都给大家解释清楚：
###  SparkConf
关于这个SparkConf里包含就是要提交的App的参数信息，这本来没什么好讲的不过这里有几个方法，个人觉得蛮有意思的，就简单介绍一下：
registerKryoClasses
registerAvroSchemas
getTimeAsSeconds
getSizeAsMb
1. registerKryoClasses
这个方法的意思就是注册用Kryo序列化的类，也就是指定该类的序列化器为Kryo，关于Kryo的使用是spark性能优化的一个重要的组成部分。这里简单介绍一处使用这个方法的地方，也就是图计算的时候：

```
object GraphXUtils {
  /**
   * Registers classes that GraphX uses with Kryo.
   */
  def registerKryoClasses(conf: SparkConf) {
    conf.registerKryoClasses(Array(
      classOf[Edge[Object]],
      classOf[(VertexId, Object)],
      classOf[EdgePartition[Object, Object]],
      classOf[BitSet],
      classOf[VertexIdToIndexMap],
      classOf[VertexAttributeBlock[Object]],
      classOf[PartitionStrategy],
      classOf[BoundedPriorityQueue[Object]],
      classOf[EdgeDirection],
      classOf[GraphXPrimitiveKeyOpenHashMap[VertexId, Int]],
      classOf[OpenHashSet[Int]],
      classOf[OpenHashSet[Long]]))
  }
}

```
2. registerAvroSchemas也就是注册再进行序列化前先把Schema注册，其最终的压缩效率会提高好多倍，这里就举spark自身的一个例子，这里是我在debug模式下截取的运行图：


![Screen Shot 2016-12-04 at 10.19.30 PM.png](http://upload-images.jianshu.io/upload_images/3736220-8308570105695922.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


看到了吗？ 最终的结果normalLength＝101， fingerprintlength=19，只从本例来讲压缩效率提高了5倍。
3. getTimeAsSeconds和getSizeAsMb这两个方法指的就是在你获取相应的值时可以按单位显示，举个例子：

```
  val maxBufferSizeMb = conf.getSizeAsMb("spark.kryoserializer.buffer.max", "64m").toInt

```
这里个人就觉得这几个方法比较有用一些，至于别的，大家自己看源码。 

### val spark = new SparkContext(conf)

这里先画一个结构图，先写清楚里面SparkContext的每一个组件，之后会一一分析：

![Screen Shot 2016-12-05 at 10.59.35 PM.png](http://upload-images.jianshu.io/upload_images/3736220-bb806d735594f700.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

写到这里就直接看SparkContext吧！因为这个SparkContext是整个Spark系统中最最重要的一个类，我尽量把它最重要的代码讲清楚：


```
1. callsite的作用
 // The call site where this SparkContext was constructed.
  //用户代码中调用spark接口的堆栈信息方法中
 //其具体功能实现在Utils.scala的getCallSite
 //功能描述：获取当前SparkContext的当前调用堆栈，将栈里最靠近栈底的属于spark或者Scala核心的类压入callStack的栈顶，
 //并将此类的方法存入lastSparkMethod；将栈里最靠近栈顶的用户类放入callStack，将此类的行号存入firstUserLine，
 //类名存入firstUserFile，最终返回的样例类CallSite存储了最短栈和长度默认为20的最长栈的样例类。
 //在JavaWordCount例子中，获得的数据如下：
 //最短栈：JavaSparkContext at JavaWordCount.java:44；
 //最长栈：org.apache.spark.api.java.JavaSparkContext.<init>(JavaSparkContext.scala:61)
 //org.apache.spark.examples.JavaWordCount.main(JavaWordCount.java:44)。
 //这个变量其实主要是给开发者看的，对于具体程序的运行没有必然的影响。当然它的信息也会反应在Spark ui的界面上的  
  private val creationSite: CallSite = Utils.getCallSite()

  // If true, log warnings instead of throwing exceptions when multiple 
  2.是否允许使用多个SparkContext，一般建议不要使用多个
   SparkContexts are active
  private val allowMultipleContexts: Boolean =
    config.getBoolean("spark.driver.allowMultipleContexts", false)

  // In order to prevent multiple SparkContexts from being active at the same time, mark this
  // context as having started construction.
  // NOTE: this must be placed at the beginning of the SparkContext constructor.
  3. 如果打开了可以使用多个SparkContext，会直接打印warning信息
  SparkContext.markPartiallyConstructed(this, allowMultipleContexts)
  4. 程序启动的开始时间
  val startTime = System.currentTimeMillis()
  5. 用于判断当前上下文的状态
  private[spark] val stopped: AtomicBoolean = new AtomicBoolean(false)
  
  ......
  
  
  // Generate the random name for a temp folder in external block store.
  // Add a timestamp as the suffix here to make it more safe
  6. 用于是否打开了外部存储的开关，关于外部存储一般都会采用tachyon
  val externalBlockStoreFolderName = "spark-" + randomUUID.toString()
  @deprecated("Use externalBlockStoreFolderName instead.", "1.4.0")
  val tachyonFolderName = externalBlockStoreFolderName
  
  ......
  // An asynchronous listener bus for Spark events
 7. 它是Spark系统消息系统的具体实现，后台会自动开辟一个线程来监听事件，现在基本上所有的分布式系统都是通过消息驱动的方式来进行并发执行的
  private[spark] val listenerBus = new LiveListenerBus
  ......
    // Set SPARK_USER for user who is running SparkContext.
  val sparkUser = Utils.getCurrentUserName()
  ......
  
   // Thread Local variable that can be used by users to pass information down the stack
 8. 本地线程中的参数信息
  protected[spark] val localProperties = new InheritableThreadLocal[Properties] {
    override protected def childValue(parent: Properties): Properties = {
      // Note: make a clone such that changes in the parent properties aren't reflected in
      // the those of the children threads, which has confusing semantics (SPARK-10563).
      SerializationUtils.clone(parent).asInstanceOf[Properties]
    }
    override protected def initialValue(): Properties = new Properties()
  }
  
  .....
  
  try {
    9. 下面就是一些参数的配置
    _conf = config.clone()
    _conf.validateSettings()

    if (!_conf.contains("spark.master")) {
      throw new SparkException("A master URL must be set in your configuration")
    }
    if (!_conf.contains("spark.app.name")) {
      throw new SparkException("An application name must be set in your configuration")
    }

    // System property spark.yarn.app.id must be set if user code ran by AM on a YARN cluster
    // yarn-standalone is deprecated, but still supported
    if ((master == "yarn-cluster" || master == "yarn-standalone") &&
        !_conf.contains("spark.yarn.app.id")) {
      throw new SparkException("Detected yarn-cluster mode, but isn't running on a cluster. " +
        "Deployment to YARN is not supported directly by SparkContext. Please use spark-submit.")
    }

    if (_conf.getBoolean("spark.logConf", false)) {
      logInfo("Spark configuration:\n" + _conf.toDebugString)
    }

    // Set Spark driver host and port system properties
    _conf.setIfMissing("spark.driver.host", Utils.localHostName())
    _conf.setIfMissing("spark.driver.port", "0")

    _conf.set("spark.executor.id", SparkContext.DRIVER_IDENTIFIER)

    _jars = _conf.getOption("spark.jars").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten
    _files = _conf.getOption("spark.files").map(_.split(",")).map(_.filter(_.size != 0))
      .toSeq.flatten

    _eventLogDir =
      if (isEventLogEnabled) {
      10.估计每个人在配置的时候，都会配置这个参数，因为如果你不配置的话，它的默认的路径就是这个EventLoggingListener.DEFAULT_LOG_DIR＝"/tmp/spark-events"
        val unresolvedDir = conf.get("spark.eventLog.dir", EventLoggingListener.DEFAULT_LOG_DIR)
          .stripSuffix("/")
        Some(Utils.resolveURI(unresolvedDir))
      } else {
        None
      }
    11. 产生的eventLog的文件，若需要压缩的话需要使用的压缩类
    _eventLogCodec = {
      val compress = _conf.getBoolean("spark.eventLog.compress", false)
      if (compress && isEventLogEnabled) {
Some(CompressionCodec.getCodecName(_conf)).map(CompressionCodec.getShortName)
      } else {
        None
      }
    }

    _conf.set("spark.externalBlockStore.folderName", externalBlockStoreFolderName)

    if (master == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")

    // "_jobProgressListener" should be set up before creating SparkEnv because when creating
    // "SparkEnv", some messages will be posted to "listenerBus" and we should not miss them.
    12. 在listenerBus接收到event的时候，会遍历所有的listener中，并调用合适的listenner来处理相应的事件
    _jobProgressListener = new JobProgressListener(_conf)
    listenerBus.addListener(jobProgressListener)

    // Create the Spark execution environment (cache, map output tracker, etc)
    13. 创建SparkEnv，在这里我们先把它最为SparkContext的第一部分，先说完这个SparkEnv在分析下面的部分
    _env = createSparkEnv(_conf, isLocal, listenerBus)
    SparkEnv.set(_env)
```
### SparkEnv
紧接着就直接看这个createSparkEnv：

```
   // This function allows components created by SparkEnv to be mocked in unit tests:
  private[spark] def createSparkEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus): SparkEnv = {
    SparkEnv.createDriverEnv(conf, isLocal, listenerBus, SparkContext.numDriverCores(master))
  }
  
```
它这里直接通过 SparkEnv.createDriverEnv来创建Driver端的env：
```
   /**
   * Create a SparkEnv for the driver.
   */
  private[spark] def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus,
      numCores: Int,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    assert(conf.contains("spark.driver.host"), "spark.driver.host is not set on the driver!")
    assert(conf.contains("spark.driver.port"), "spark.driver.port is not set on the driver!")
    val hostname = conf.get("spark.driver.host")
    val port = conf.get("spark.driver.port").toInt
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      hostname,
      port,
      isDriver = true,
      isLocal = isLocal,
      numUsableCores = numCores,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }  
```
上面携带的参数只有conf,SparkContext.DRIVER_IDENTIFIER,hostname, port是有赋值的；
接着再看这个create方法：

```
  /**
   * Helper method to create a SparkEnv for a driver or an executor.
   */
  private def create(
      conf: SparkConf,
      executorId: String,
      hostname: String,
      port: Int,
      isDriver: Boolean,
      isLocal: Boolean,
      numUsableCores: Int,
      listenerBus: LiveListenerBus = null,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    // Listener bus is only used on the driver
   1. 有上面可以知道，我们现在确实是在Driver端
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }

    val securityManager = new SecurityManager(conf)

    // Create the ActorSystem for Akka and get the port it binds to.
    val actorSystemName = if (isDriver) driverActorSystemName else executorActorSystemName
    2. 创建driver端的RpcEnv
    val rpcEnv = RpcEnv.create(actorSystemName, hostname, port, conf, securityManager,
      clientMode = !isDriver)
   3. 关于这一个参数，其实就是akka的actorSystem，如果我们使用netty来实现RPC协议的话，其实这个参数是没用的，这里的创建只不过是预留给老版本的代码的（1.4.0）
    val actorSystem: ActorSystem =
      if (rpcEnv.isInstanceOf[AkkaRpcEnv]) {
        rpcEnv.asInstanceOf[AkkaRpcEnv].actorSystem
      } else {
        val actorSystemPort =
          if (port == 0 || rpcEnv.address == null) {
            port
          } else {
            rpcEnv.address.port + 1
          }
        // Create a ActorSystem for legacy codes
        AkkaUtils.createActorSystem(
          actorSystemName + "ActorSystem",
          hostname,
          actorSystemPort,
          conf,
          securityManager
        )._1
      }

    // Figure out which port Akka actually bound to in case the original port is 0 or occupied.
    // In the non-driver case, the RPC env's address may be null since it may not be listening
    // for incoming connections.
    if (isDriver) {
      conf.set("spark.driver.port", rpcEnv.address.port.toString)
    } else if (rpcEnv.address != null) {
      conf.set("spark.executor.port", rpcEnv.address.port.toString)
    }

    // Create an instance of the class with the given name, possibly initializing it with our conf
    4. 根据SparkConf来实例化一个类，具体的应用在下面
    def instantiateClass[T](className: String): T = {
      val cls = Utils.classForName(className)
      // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
      // SparkConf, then one taking no arguments
      try {
        cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
          .newInstance(conf, new java.lang.Boolean(isDriver))
          .asInstanceOf[T]
      } catch {
        case _: NoSuchMethodException =>
          try {
            cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
          } catch {
            case _: NoSuchMethodException =>
              cls.getConstructor().newInstance().asInstanceOf[T]
          }
      }
    }

    // Create an instance of the class named by the given SparkConf property, or defaultClassName
    // if the property is not set, possibly initializing it with our conf
    def instantiateClassFromConf[T](propertyName: String, defaultClassName: String): T = {
      instantiateClass[T](conf.get(propertyName, defaultClassName))
    }
 5. 需要注意的是，这里Serializer针对的对象是Shuffle数据，以及RDD Cache等场合，而Spark Task的序列化是通过spark.closure.serializer来配置。
    val serializer = instantiateClassFromConf[Serializer](
      "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
    logDebug(s"Using serializer: ${serializer.getClass}")

    val closureSerializer = instantiateClassFromConf[Serializer](
      "spark.closure.serializer", "org.apache.spark.serializer.JavaSerializer")
   
    6. 在RpcEnv中新建或查找endpoint，mapOutputTracker会使用这个方法
    def registerOrLookupEndpoint(
        name: String, endpointCreator: => RpcEndpoint):
      RpcEndpointRef = {
      if (isDriver) {
        logInfo("Registering " + name)
        rpcEnv.setupEndpoint(name, endpointCreator)
      } else {
        RpcUtils.makeDriverRef(name, conf, rpcEnv)
      }
    }
   7. 这个玩意很重要，shuffle的数据信息都会保存在这里
    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf)
    } else {
      new MapOutputTrackerWorker(conf)
    }
    8. 在个endpoint，是在shuffle阶段获取时访问的节点，该节点会将具体的shuffle数据的信息交给相应的节点
    // Have to assign trackerActor after initialization as MapOutputTrackerActor
    // requires the MapOutputTracker itself
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    // Let the user specify short names for shuffle managers
   9. 指定shuffle的类型
    val shortShuffleMgrNames = Map(
      "hash" -> "org.apache.spark.shuffle.hash.HashShuffleManager",
      "sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager",
      "tungsten-sort" -> "org.apache.spark.shuffle.sort.SortShuffleManager")
    val shuffleMgrName = conf.get("spark.shuffle.manager", "sort")
    val shuffleMgrClass = shortShuffleMgrNames.getOrElse(shuffleMgrName.toLowerCase, shuffleMgrName)
    val shuffleManager = instantiateClass[ShuffleManager](shuffleMgrClass)
   10. 是否使用以前的memoryManage管理机制，这个UnifiedMemoryManager是spark 1.6.＊之后最后要的改进，稍后会详细分析这个的
    val useLegacyMemoryManager = conf.getBoolean("spark.memory.useLegacyMode", false)
    val memoryManager: MemoryManager =
      if (useLegacyMemoryManager) {
        new StaticMemoryManager(conf, numUsableCores)
      } else {
        UnifiedMemoryManager(conf, numUsableCores)
      }
    11. 下面就是blockManager的一些配置，通过blockTransferService来进行block的传输，通过blockManagerMaster来管理集群的blockManager，最终通过blockManager来管理集群的所有block信息
    val blockTransferService = new NettyBlockTransferService(conf, securityManager, numUsableCores)

    val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint(
      BlockManagerMaster.DRIVER_ENDPOINT_NAME,
      new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)),
      conf, isDriver)

    // NB: blockManager is not valid until initialize() is called later.
    val blockManager = new BlockManager(executorId, rpcEnv, blockManagerMaster,
      serializer, conf, memoryManager, mapOutputTracker, shuffleManager,
      blockTransferService, securityManager, numUsableCores)
   12. 广播数据的管理者
    val broadcastManager = new BroadcastManager(isDriver, conf, securityManager)
   13.缓存数据的管理者，可以看到这里缓存数据管理的其实也是blockManager中的block
    val cacheManager = new CacheManager(blockManager)
   14. 测量系统的创建，Streaming 的博客中有清晰说明
    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      MetricsSystem.createMetricsSystem("driver", conf, securityManager)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      conf.set("spark.executor.id", executorId)
      val ms = MetricsSystem.createMetricsSystem("executor", conf, securityManager)
      ms.start()
      ms
    }
    // Set the sparkFiles directory, used when downloading dependencies.  In local mode,
    // this is a temporary directory; in distributed mode, this is the executor's current working
    // directory.
    val sparkFilesDir: String = if (isDriver) {
      Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
    } else {
      "."
    }
   
   15. output数据的协调器，也就是说当要输出数据到HDFS上的时候，需要获得他的允许
    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)
   16. 最终就是根据上面生成的参数来实例化这个SparkEnv类，最终返回这个实例
    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      actorSystem,
      serializer,
      closureSerializer,
      cacheManager,
      mapOutputTracker,
      shuffleManager,
      broadcastManager,
      blockTransferService,
      blockManager,
      securityManager,
      sparkFilesDir,
      metricsSystem,
      memoryManager,
      outputCommitCoordinator,
      conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
    if (isDriver) {
      envInstance.driverTmpDirToDelete = Some(sparkFilesDir)
    }

    envInstance
  }

```
OK，SparkEnv总算弄完了，后面接着分析Spark Context的第二部分

### SparkContext第二部分
```
1. 原数据清理器，这里清理的原数据指的就是之前进行persist操作的数据，更具体点说就是persistentRdds中的数据
 _metadataCleaner = new MetadataCleaner(MetadataCleanerType.SPARK_CONTEXT, this.cleanup, _conf)
 2. 用于跟踪spark程序中job，stage，task的完成度的信息
    _statusTracker = new SparkStatusTracker(this)
 3. 是否打开ConsoleProgressBar的开关，也就是通常在控制台上显示的什么什么完成了多少了的信息
    _progressBar =
      if (_conf.getBoolean("spark.ui.showConsoleProgress", true) && !log.isInfoEnabled) {
        Some(new ConsoleProgressBar(this))
      } else {
        None
      }
    4.下面是Spark UI部分，在Streaming博客中也有详细介绍
    _ui =
      if (conf.getBoolean("spark.ui.enabled", true)) {
        Some(SparkUI.createLiveUI(this, _conf, listenerBus, _jobProgressListener,
          _env.securityManager, appName, startTime = startTime))
      } else {
        // For tests, do not enable the UI
        None
      }
    // Bind the UI before starting the task scheduler to communicate
    // the bound port to the cluster manager properly
    _ui.foreach(_.bind())
     5.  这里面就是配置hadoop系统的信息，其中有一个比较重要的参数io.file.buffer.size，io.file.buffer.size都被用来设置缓存的大小。不论是对硬盘或者是网络操作来讲，较大的缓存都可以提供更高的数据传输，但这也就意味着更大的内存消耗和延迟。这个参数要设置为系统页面大小的倍数，以byte为单位，默认值是4KB，一般情况下，可以设置为64KB（65536byte）。
    _hadoopConfiguration = SparkHadoopUtil.get.newConfiguration(_conf)
    
    // Add each JAR given through the constructor
    6. 将spark.jars和spark.files中的文件添加到env.rpcEnv.fileServer上，详细过程看下这个addjar方法。
    if (jars != null) {
      jars.foreach(addJar)
    }

    if (files != null) {
      files.foreach(addFile)
    }
    7. 下面又是一些配置一些参数的操作
    _executorMemory = _conf.getOption("spark.executor.memory")
      .orElse(Option(System.getenv("SPARK_EXECUTOR_MEMORY")))
      .orElse(Option(System.getenv("SPARK_MEM"))
      .map(warnSparkMem))
      .map(Utils.memoryStringToMb)
      .getOrElse(1024)

    // Convert java options to env vars as a work around
    // since we can't set env vars directly in sbt.
    for { (envKey, propKey) <- Seq(("SPARK_TESTING", "spark.testing"))
      value <- Option(System.getenv(envKey)).orElse(Option(System.getProperty(propKey)))} {
      executorEnvs(envKey) = value
    }
    Option(System.getenv("SPARK_PREPEND_CLASSES")).foreach { v =>
      executorEnvs("SPARK_PREPEND_CLASSES") = v
    }
    // The Mesos scheduler backend relies on this environment variable to set executor memory.
    // TODO: Set this only in the Mesos scheduler.
    executorEnvs("SPARK_EXECUTOR_MEMORY") = executorMemory + "m"
    executorEnvs ++= _conf.getExecutorEnv
    executorEnvs("SPARK_USER") = sparkUser

    // We need to register "HeartbeatReceiver" before "createTaskScheduler" because Executor will
    // retrieve "HeartbeatReceiver" in the constructor. (SPARK-6640)
    _heartbeatReceiver = env.rpcEnv.setupEndpoint(
      HeartbeatReceiver.ENDPOINT_NAME, new HeartbeatReceiver(this))

    // Create and start the scheduler
    8. 下面是spark程序运行的总体重要流程，我将在下一篇博客中详细分析，我们先分析剩余的部分
    val (sched, ts) = SparkContext.createTaskScheduler(this, master)
    _schedulerBackend = sched
    _taskScheduler = ts
    _dagScheduler = new DAGScheduler(this)
    _heartbeatReceiver.ask[Boolean](TaskSchedulerIsSet)

    // start TaskScheduler after taskScheduler sets DAGScheduler reference in DAGScheduler's
    // constructor
    _taskScheduler.start()

    _applicationId = _taskScheduler.applicationId()
    _applicationAttemptId = taskScheduler.applicationAttemptId()
    _conf.set("spark.app.id", _applicationId)
    _ui.foreach(_.setAppId(_applicationId))
    _env.blockManager.initialize(_applicationId)

   9. 开启测量系统，这个内容在streaming的博客中分析的十分清楚，麻烦看下我streaming的博客
    // The metrics system for Driver need to be set spark.app.id to app ID.
    // So it should start after we get app ID from the task scheduler and set spark.app.id.
    metricsSystem.start()
    // Attach the driver metrics servlet handler to the web ui after the metrics system is started.
    metricsSystem.getServletHandlers.foreach(handler => ui.foreach(_.attachHandler(handler)))
  10. 打印eventlog的listener，并将其添加到listenerBus中
    _eventLogger =
      if (isEventLogEnabled) {
        val logger =
          new EventLoggingListener(_applicationId, _applicationAttemptId, _eventLogDir.get,
            _conf, _hadoopConfiguration)
        logger.start()
        listenerBus.addListener(logger)
        Some(logger)
      } else {
        None
      }
 11. 资源的动态分配，这个会在性能优化的时候再讲
    // Optionally scale number of executors dynamically based on workload. Exposed for testing.
    val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(_conf)
    if (!dynamicAllocationEnabled && _conf.getBoolean("spark.dynamicAllocation.enabled", false)) {
      logWarning("Dynamic Allocation and num executors both set, thus dynamic allocation disabled.")
    }

    _executorAllocationManager =
      if (dynamicAllocationEnabled) {
        Some(new ExecutorAllocationManager(this, listenerBus, _conf))
      } else {
        None
      }
    _executorAllocationManager.foreach(_.start())
    12. 后台后启动一个线程，一直在执行clean数据的操作，包括RDD，shuffle，广播变量，累加器，checkpoint的数据，这个也会在性能优化的时候详细分析
    _cleaner =
      if (_conf.getBoolean("spark.cleaner.referenceTracking", true)) {
        Some(new ContextCleaner(this))
      } else {
        None
      }
    _cleaner.foreach(_.start())
    13. 主要是注册spark.extraListeners中的listener
    setupAndStartListenerBus()
    14. 向总线发送SparkListenerEnvironmentUpdate的消息,主要更新了ScheduleMole的信息
    postEnvironmentUpdate()
    15.向总线发送SparkListenerApplicationStart的消息，主要更新了job的开始时间
    postApplicationStart()

    // Post init
    16. 其实这个方法就是为了保证在_taskScheduler中，变量backend已经ok。
    _taskScheduler.postStartHook()
    _env.metricsSystem.registerSource(_dagScheduler.metricsSource)
    _env.metricsSystem.registerSource(new BlockManagerSource(_env.blockManager))
    _executorAllocationManager.foreach { e =>
      _env.metricsSystem.registerSource(e.executorAllocationManagerSource)
    }

    // Make sure the context is stopped if the user forgets about it. This avoids leaving
    // unfinished event logs around after the JVM exits cleanly. It doesn't help if the JVM
    // is killed, though.
    17. 这个就是用来当我们的程序正常或异常结束时，执行addShutdownHook添加的hooktask，这里hooktask就是｛｝中的这个方法。
    _shutdownHookRef = ShutdownHookManager.addShutdownHook(
      ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY) { () =>
      logInfo("Invoking stop() from shutdown hook")
      stop()
    }
  } catch {
    case NonFatal(e) =>
      logError("Error initializing SparkContext.", e)
      try {
        stop()
      } catch {
        case NonFatal(inner) =>
          logError("Error stopping SparkContext after init error.", inner)
      } finally {
        throw e
      }
  }
```
好了，写到这里，整个的SparkContext就写完了，因为它是最最重要的类，不得不专门分析一下。下面就是我们应用的具体调度了。

