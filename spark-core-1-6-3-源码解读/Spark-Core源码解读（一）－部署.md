### Spark 部署模式讨论
通常情况下，Spark Core的部署方式一般有三种：standalone模式，yarn模式，mesos模式，EC2模式。
1. 生产环境下国内一般都是使用的yarn模式，因为阿里最先使用的，所以关于yarn的技术和使用比较多一些。
2. 生产环境下国外使用的mesos模式的比较多一些，比如twitter，ebay等。至于国内，据我的了解，没有几家大的互联网公司使用的mesos。
3. EC2模式，这个不太清楚，我自己没用过也研究过。
4. 关于华为我透漏一点，因为最近大家都在讨论华为云什么的，其实我们的spark项目就是部署在华为云上的，但是华为云给我们的都是一个个的linux系统，而我们采用的也是yarn模式。唯一的好处就是根据项目对资源的需要我们可以通过cloud立即获取更多的计算资源。

yarn和mesos都是开源分布式资源管理框架，关于二者的不同，请看下面：

1、最大的不同点在于他们所采用的scheduler：mesos让framework决定mesos提供的这个资源是否适合该job，从而接受或者拒绝这个资源。而对于yarn来说，决定权在于yarn，是yarn本身（自行替应用程序作主）决定这个资源是否适合该job，对于各种各样的应用程序来说或许这就是个错误的决定（这就是现代人为什么拒绝父母之命媒妁之言而选择自由婚姻的缘故吧）。所以从scaling的角度来说，mesos更scalable。
2、其次，yarn是MapReduce进化的产物，yarn从诞生之日起就是为hadoopjobs管理资源的（yarn也开始朝着mesos涉及的领域进军），yarn只为jobs提供了一个static partitioning。而mesos的设计目标是为各个框架（hadoop、spark、web services等）提供dynamical partitioning，让各个集群框架共用数据中心机器。
3、myriad项目将让yarn运行在mesos上面。

自己把对二者的理解画了一幅简图：

![Screen Shot 2016-11-28 at 10.56.31 PM.png](http://upload-images.jianshu.io/upload_images/3736220-c01d5bb2c8407a39.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

另外这还有一个视频，个人觉得分析的很好，有VPN，再加上英语不错的小伙伴可以看一下：
https://www.youtube.com/watch?v=aXJxyEnkHd4

这个个人强烈建议使用Mesos。另外我们的项目也在向这方面转型，到时候大家可以一起讨论。

### standalone模式－源码
前面提到的都是一些常用的部署方式，除了standalone模式以外，各自都自称体系，spark core中只是使用了其提供的接口，具体的资源分配都由各自的框架来分配，所以这里只谈论Spark 自带的standalone的模式，因为还是有一些小公司在使用这种方式。

首先spark的启动是通过${SPARK_HOME}/sbin目录下的start-all.sh来启动的。那就直接打开start-all.sh看一下它里面的内容：

```
......
# Load the Spark configuration
. "${SPARK_HOME}/sbin/spark-config.sh"

# Start Master
"${SPARK_HOME}/sbin"/start-master.sh $TACHYON_STR

# Start Workers
"${SPARK_HOME}/sbin"/start-slaves.sh $TACHYON_STR

```
shell脚本我就不一步步分析了，他调用了spark-config.sh，start-master.sh，start-slaves.sh三个脚本。
而在spark-config.sh中加载的就是一些配置信息和环境变量

#### Master
---
下面看start-master.sh

```
......

CLASS="org.apache.spark.deploy.master.Master"

......

"${SPARK_HOME}/sbin"/spark-daemon.sh start $CLASS 1 \
  --ip $SPARK_MASTER_IP --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT \
  $ORIGINAL_ARGS
```
在这里是调用了Master这个类，当然也传入了必要的参数 如ip，port等，打开这个类后，你直接看它的伴生对象，会看到一个main方法，这正是我们想要的：

```

private[deploy] object Master extends Logging {
  val SYSTEM_NAME = "sparkMaster"
  val ENDPOINT_NAME = "Master"

  def main(argStrings: Array[String]) {
    1. 注册的log对象
    SignalLogger.register(log)
    val conf = new SparkConf
    2. 构建Master的参数，其中包括host，port，webUiPort，propertiesFile，这些都是通过start-master.sh这个脚本传进来了，后面详细介绍
    val args = new MasterArguments(argStrings, conf)
    3. 创建RpcEnv和Endpoint，spark在akka或netty的基础上封装了一个RPC系统，这里关于RPC系统你可以这样理解：RpcEnv就是一个server，endpoint就是client，如果client1想和另一个client2通信，它就需要向server获取client2的信息。
    val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
    rpcEnv.awaitTermination()
  }

  /**
   * Start the Master and return a three tuple of:
   *   (1) The Master RpcEnv
   *   (2) The web UI bound port
   *   (3) The REST server bound port, if any
   */
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    1. spark中负责安全的类，集群中节点的交流，比如akka，netty的通信还有shuffle阶段使用的BlockTransferService的授权都是由这个类来管理的。
    val securityMgr = new SecurityManager(conf)
    2. 首先创建RpcEnv，也就是server端 
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
    3. 在RpcEnv中为当前的Master对象创建一个Endpoint，之后就可以直接根据这个Endpoint和Master通信了。
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
    4. 就是向masterEndpoint发送一个BoundPortsRequest，这里会返回一个BoundPortsResponse的信息
    val portsResponse =
    masterEndpoint.askWithRetry[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}

```
关于上面的MasterArguments这个类，在new一个对象的时候会先读取spark-env.sh中的配置信息，然后再获取通过shell传入的参数，然后再读去spark-defaults.conf或者自定义的propertiesFile中的数据，如果有重复的值，那么后面的会覆盖前面的值。 
所以spark中配置信息获取的优先性为：
propertiesFile   >   spark-defaults.conf   >   传入参数    > spark-env.sh

接着要分析的就是new Master这个对象了，了解我写博客的习惯就知道下面要干什么了，就是看这个Master初始化的内容了：
 这里我还是会依次来说明的
```
1. 这里先看这个Master是继承自ThreadSafeRpcEndpoint，所以它本身就是一个Endpoint了，当然它也会包含receive、receiveAndReply的方法的
private[deploy] class Master(
    override val rpcEnv: RpcEnv,
    address: RpcAddress,
    webUiPort: Int,
    val securityMgr: SecurityManager,
    val conf: SparkConf)
  extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {
  1. 后台启动一个线程用于按固定时间间隔检查并remove掉死掉的workers
  private val forwardMessageThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-forward-message-thread")
   2. 后台重启一个线程用于执行重建UI的代码
   private val rebuildUIThread =
    ThreadUtils.newDaemonSingleThreadExecutor("master-rebuild-ui-thread")
  3. 执行rebuildUIThread的ExecutionContext
  private val rebuildUIContext = ExecutionContext.fromExecutor(rebuildUIThread)
  4. hadoop配置信息
  private val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
  5. 日期格式，应用构建application IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss") // For application IDs
 6. 60s内master接收不到worker的心跳信息，master就认为该worker 丢失了
  private val WORKER_TIMEOUT_MS = conf.getLong("spark.worker.timeout", 60) * 1000
 7. 最多的可显示在Spark UI上的已完成的applications
  private val RETAINED_APPLICATIONS = conf.getInt("spark.deploy.retainedApplications", 200)
 8.  最多的可显示在Spark UI上的已完成的drivers
  private val RETAINED_DRIVERS = conf.getInt("spark.deploy.retainedDrivers", 200)
 9. 若超时时间超过15个timeout则彻底删除worker，允许worker停留的最大时间即为REAPER_ITERATIONS＊WORKER_TIMEOUT_MS
  private val REAPER_ITERATIONS = conf.getInt("spark.dead.worker.persistence", 15)
 10. 恢复模式spark中包含ZOOKEEPER，FILESYSTEM，NONE
  private val RECOVERY_MODE = conf.get("spark.deploy.recoveryMode", "NONE")
  11. 这个参数用来表示在standalone模式下，一个Application允许的最大的连续启动Executor失败的次数
  private val MAX_EXECUTOR_RETRIES = conf.getInt("spark.deploy.maxExecutorRetries", 10)

  val workers = new HashSet[WorkerInfo]
  val idToApp = new HashMap[String, ApplicationInfo]
  val waitingApps = new ArrayBuffer[ApplicationInfo]
  val apps = new HashSet[ApplicationInfo]

  private val idToWorker = new HashMap[String, WorkerInfo]
  private val addressToWorker = new HashMap[RpcAddress, WorkerInfo]

  private val endpointToApp = new HashMap[RpcEndpointRef, ApplicationInfo]
  private val addressToApp = new HashMap[RpcAddress, ApplicationInfo]
  private val completedApps = new ArrayBuffer[ApplicationInfo]
  private var nextAppNumber = 0
  // Using ConcurrentHashMap so that master-rebuild-ui-thread can add a UI after asyncRebuildUI
  private val appIdToUI = new ConcurrentHashMap[String, SparkUI]

  private val drivers = new HashSet[DriverInfo]
  private val completedDrivers = new ArrayBuffer[DriverInfo]
  // Drivers currently spooled for scheduling
  private val waitingDrivers = new ArrayBuffer[DriverInfo]
  private var nextDriverNumber = 0

  Utils.checkHost(address.host, "Expected hostname")
  12. 测量系统，这个我们在Streaming（－）中讲过
  private val masterMetricsSystem = MetricsSystem.createMetricsSystem("master", conf, securityMgr)
  private val applicationMetricsSystem = MetricsSystem.createMetricsSystem("applications", conf,
    securityMgr)
  13. 测量系统需要的源信息
  private val masterSource = new MasterSource(this)

  // After onStart, webUi will be set
  private var webUi: MasterWebUI = null

  private val masterPublicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else address.host
  }

  private val masterUrl = address.toSparkURL
  private var masterWebUiUrl: String = _

  private var state = RecoveryState.STANDBY

  private var persistenceEngine: PersistenceEngine = _

  private var leaderElectionAgent: LeaderElectionAgent = _

  private var recoveryCompletionTask: ScheduledFuture[_] = _

  private var checkForWorkerTimeOutTask: ScheduledFuture[_] = _

  // As a temporary workaround before better ways of configuring memory, we allow users to set
  // a flag that will perform round-robin scheduling across the nodes (spreading out each app
  // among all the nodes) instead of trying to consolidate each app onto a small # of nodes.
  14. 这个参数默认开启，避免每一个app只在一部分节点上计算
  private val spreadOutApps = conf.getBoolean("spark.deploy.spreadOut", true)

  // Default maxCores for applications that don't specify it (i.e. pass Int.MaxValue)
  private val defaultCores = conf.getInt("spark.deploy.defaultCores", Int.MaxValue)
  if (defaultCores < 1) {
    throw new SparkException("spark.deploy.defaultCores must be positive")
  }

  // Alternative application submission gateway that is stable across Spark versions
  15，后面这三个参数用于启动一个restServer用于接收提交的application
  private val restServerEnabled = conf.getBoolean("spark.master.rest.enabled", true)
  private var restServer: Option[StandaloneRestServer] = None
  private var restServerBoundPort: Option[Int] = None

```
接下来看这个OnStart方法，这里在RPC Endpoint创建的时候，会调用这个Endpoint的OnStart：

```
  def startRpcEnvAndEndpoint(
      host: String,
      port: Int,
      webUiPort: Int,
      conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
    ......
    val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
      new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
    ......
}

```
也就是在setupEndpoint这个方法里调用的Master这个Endpoint中的OnStart方法。（关于RPC之后，会专门写一篇博客的）下面就直接看这个OnStart：

```
logInfo("Starting Spark master at " + masterUrl)
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    1. 创建MasterWebUI界面，也就是显示Master信息的页面
    webUi = new MasterWebUI(this, webUiPort)
    2. bind  web接口后面的Http server
    webUi.bind()
    3. 通过访问这个url可以获取master的信息
    masterWebUiUrl = "http://" + masterPublicAddress + ":" + webUi.boundPort
    4. 按固定时间间隔检查并remove掉死掉的workers
    checkForWorkerTimeOutTask = forwardMessageThread.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        self.send(CheckForWorkerTimeOut)
      }
    }, 0, WORKER_TIMEOUT_MS, TimeUnit.MILLISECONDS)
    5. 创建restServer，用于接收提交的application
    if (restServerEnabled) {
      val port = conf.getInt("spark.master.rest.port", 6066)
      restServer = Some(new StandaloneRestServer(address.host, port, conf, self, masterUrl))
    }
    6. 启动restServer
    restServerBoundPort = restServer.map(_.start())
    7. 向master的测量系统注册源
    masterMetricsSystem.registerSource(masterSource)
    8. 启动master的测量系统   
    masterMetricsSystem.start()
    9. 启动application的测量系统
    applicationMetricsSystem.start()
    // Attach the master and app metrics servlet handler to the web ui after the metrics systems are
    // started.
10. 通过和attachHandler的绑定，可以把源信息展现在桌面上 masterMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
    applicationMetricsSystem.getServletHandlers.foreach(webUi.attachHandler)
 11. 之后就是standalone模式下恢复机制，这里主要介绍下ZooKeeper的方式，其余的大家有兴趣的话，自己看一下
    val serializer = new JavaSerializer(conf)
    val (persistenceEngine_, leaderElectionAgent_) = RECOVERY_MODE match {
      case "ZOOKEEPER" =>
        logInfo("Persisting recovery state to ZooKeeper")
        val zkFactory =
          new ZooKeeperRecoveryModeFactory(conf, serializer)
        1.创建持久化引擎，用于持久化一些数据信息，如application，driverdeng
        2. 创建实现leader选举的类，用于在为leader的master出错后，选举别的master为leader
        (zkFactory.createPersistenceEngine(), zkFactory.createLeaderElectionAgent(this))
      case "FILESYSTEM" =>
        val fsFactory =
          new FileSystemRecoveryModeFactory(conf, serializer)
        (fsFactory.createPersistenceEngine(), fsFactory.createLeaderElectionAgent(this))
      case "CUSTOM" =>
        val clazz = Utils.classForName(conf.get("spark.deploy.recoveryMode.factory"))
        val factory = clazz.getConstructor(classOf[SparkConf], classOf[Serializer])
          .newInstance(conf, serializer)
          .asInstanceOf[StandaloneRecoveryModeFactory]
        (factory.createPersistenceEngine(), factory.createLeaderElectionAgent(this))
      case _ =>
        (new BlackHolePersistenceEngine(), new MonarchyLeaderAgent(this))
    }
    persistenceEngine = persistenceEngine_
    leaderElectionAgent = leaderElectionAgent_

```
到这里Master的启动就完成了。

#### Worker
---
通过上文中可以知道，接下来要启动的shell脚本是start-slaves.sh

```
......

"${SPARK_HOME}/sbin/slaves.sh" cd "${SPARK_HOME}" \; "${SPARK_HOME}/sbin/start-slave.sh" "spark://$SPARK_MASTER_IP:$SPARK_MASTER_PORT"

......

```
这里slaves.sh会循环ssh到需要启动slave的节点，真正的启动worker还是通过start-slave.sh


```
......

CLASS="org.apache.spark.deploy.worker.Worker"

MASTER=$1

function start_instance {
  WORKER_NUM=$1
  shift

  if [ "$SPARK_WORKER_PORT" = "" ]; then
    PORT_FLAG=
    PORT_NUM=
  else
    PORT_FLAG="--port"
    PORT_NUM=$(( $SPARK_WORKER_PORT + $WORKER_NUM - 1 ))
  fi
  WEBUI_PORT=$(( $SPARK_WORKER_WEBUI_PORT + $WORKER_NUM - 1 ))

  "${SPARK_HOME}/sbin"/spark-daemon.sh start $CLASS $WORKER_NUM \
     --webui-port "$WEBUI_PORT" $PORT_FLAG $PORT_NUM $MASTER "$@"
}

if [ "$SPARK_WORKER_INSTANCES" = "" ]; then
  start_instance 1 "$@"
else
  for ((i=0; i<$SPARK_WORKER_INSTANCES; i++)); do
    start_instance $(( 1 + $i )) "$@"
  done
fi
......

```
由这可以看出，它是在每个节点上都会加载并执行worker类，传入的参数肯定包括master及其他参数。
下面看worker的源码实现，这里按道理应该直接看Worker的main方法，不过它main方法里面和Master方法中的代码基本上一摸一样，这里就不分析了，大家自己看一下。

所以这里直接来看这个Worker类，下面还是一步步的讲解每个变量：

这里基本上每个变量都有注释，我就不一一标记了，只指出一些重要的信息

```
private[deploy] class Worker(
    override val rpcEnv: RpcEnv,
    webUiPort: Int,
    cores: Int,
    memory: Int,
    masterRpcAddresses: Array[RpcAddress],
    systemName: String,
    endpointName: String,
    workDirPath: String = null,
    val conf: SparkConf,
    val securityMgr: SecurityManager)
  extends ThreadSafeRpcEndpoint with Logging {
  
  1. 本地RpcEnv的host和port
  private val host = rpcEnv.address.host
  private val port = rpcEnv.address.port

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  // A scheduled executor used to send messages at the specified time.
  private val forwordMessageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("worker-forward-message-scheduler")

  // A separated thread to clean up the workDir. Used to provide the implicit parameter of `Future`
  // methods.
  private val cleanupThreadExecutor = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonSingleThreadExecutor("worker-cleanup-thread"))

  // For worker and executor IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
  // Send a heartbeat every (heartbeat timeout) / 4 milliseconds
  private val HEARTBEAT_MILLIS = conf.getLong("spark.worker.timeout", 60) * 1000 / 4

  // Model retries to connect to the master, after Hadoop's model.
  // The first six attempts to reconnect are in shorter intervals (between 5 and 15 seconds)
  // Afterwards, the next 10 attempts are between 30 and 90 seconds.
  // A bit of randomness is introduced so that not all of the workers attempt to reconnect at
  // the same time.
  1. 它这里的意思就是 当worker启动后，在最开始向master注册的6次，每次重新注册的时间间隔为5～15s
  private val INITIAL_REGISTRATION_RETRIES = 6
 2. 超过6次后每次重新注册的时间间隔为30～90s，最多不超过16次
 private val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 10
 3. 下面几个参数会联合产生一个注册时间间隔的随机值，使得所有workers尽量不同时向Master注册
  private val FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND = 0.500
  private val REGISTRATION_RETRY_FUZZ_MULTIPLIER = {
    val randomNumberGenerator = new Random(UUID.randomUUID.getMostSignificantBits)
    randomNumberGenerator.nextDouble + FUZZ_MULTIPLIER_INTERVAL_LOWER_BOUND
  }
  private val INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(10 *
    REGISTRATION_RETRY_FUZZ_MULTIPLIER))
  private val PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS = (math.round(60
    * REGISTRATION_RETRY_FUZZ_MULTIPLIER))
 4. 是否清理worker产生的文件
  private val CLEANUP_ENABLED = conf.getBoolean("spark.worker.cleanup.enabled", false)
  // How often worker will clean up old app folders
 5.多久清理一次
  private val CLEANUP_INTERVAL_MILLIS =
    conf.getLong("spark.worker.cleanup.interval", 60 * 30) * 1000
  // TTL for app folders/data;  after TTL expires it will be cleaned up
  6. 清理Application的到期时间
  private val APP_DATA_RETENTION_SECONDS =
    conf.getLong("spark.worker.cleanup.appDataTtl", 7 * 24 * 3600)
  7.是否开启测试
  private val testing: Boolean = sys.props.contains("spark.testing")
  private var master: Option[RpcEndpointRef] = None
  private var activeMasterUrl: String = ""
  private[worker] var activeMasterWebUiUrl : String = ""
  8. 通过这个uri可以获取相应的endpoint
  private val workerUri = rpcEnv.uriOf(systemName, rpcEnv.address, endpointName)
  private var registered = false
  private var connected = false
  private val workerId = generateWorkerId()
  9. spark在本地系统中的根目录
  private val sparkHome =
    if (testing) {
      assert(sys.props.contains("spark.test.home"), "spark.test.home is not set!")
      new File(sys.props("spark.test.home"))
    } else {
      new File(sys.env.get("SPARK_HOME").getOrElse("."))
    }

  var workDir: File = null
  val finishedExecutors = new LinkedHashMap[String, ExecutorRunner]
  val drivers = new HashMap[String, DriverRunner]
  val executors = new HashMap[String, ExecutorRunner]
  val finishedDrivers = new LinkedHashMap[String, DriverRunner]
  val appDirectories = new HashMap[String, Seq[String]]
  val finishedApps = new HashSet[String]
  10. 可以显示在UI界面上最多的Executors和Drivers
  val retainedExecutors = conf.getInt("spark.worker.ui.retainedExecutors",
    WorkerWebUI.DEFAULT_RETAINED_EXECUTORS)
  val retainedDrivers = conf.getInt("spark.worker.ui.retainedDrivers",
    WorkerWebUI.DEFAULT_RETAINED_DRIVERS)
  11. 在BlockManager中是否开启外部Shuffle服务，若开启，shuffleClient则为ExternalShuffleClient，若不开启，shuffleClient则为BlockTransferService。 默认情况下，不开启
  // The shuffle service is not actually started unless configured.
  private val shuffleService = new ExternalShuffleService(conf, securityMgr)
  12. The public DNS name of the Spark master and workers (default: none).
  private val publicAddress = {
    val envVar = conf.getenv("SPARK_PUBLIC_DNS")
    if (envVar != null) envVar else host
  }
  private var webUi: WorkerWebUI = null

  private var connectionAttemptCount = 0
  13. worker的测量系统及其source
  private val metricsSystem = MetricsSystem.createMetricsSystem("worker", conf, securityMgr)
  private val workerSource = new WorkerSource(this)
  14. 两个Future对象，用于保存向Master注册处理后得到的Future对象。Future 表示一个可能还没有实际完成的异步任务的结果，
  private var registerMasterFutures: Array[JFuture[_]] = null
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None
  // A thread pool for registering with masters. Because registering with a master is a blocking
  // action, this thread pool must be able to create "masterRpcAddresses.size" threads at the same
  // time so that we can register with all masters.
  private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "worker-register-master-threadpool",
    masterRpcAddresses.size // Make sure we can register with all masters at the same time
  )
  15. 初始化时，可用的cores和memory
  var coresUsed = 0
  var memoryUsed = 0

```
因为worker也是继承自ThreadSafeRpcEndpoint，所以在它初始化后，肯定会启动onStart方法，下面就直接分析该方法：


```
    webUi = new MasterWebUI(this, webUiPort)
    2. bind  web接口后面的Http server
    
    override def onStart() {
    assert(!registered)
    logInfo("Starting Spark worker %s:%d with %d cores, %s RAM".format(
      host, port, cores, Utils.megabytesToString(memory)))
    logInfo(s"Running Spark version ${org.apache.spark.SPARK_VERSION}")
    logInfo("Spark home: " + sparkHome)
    1. 创建worker工作目录
    createWorkDir()
    2. 如果开启了spark.shuffle.service.enabled的开关，就为shuffle在外部启动一个server
    shuffleService.startIfEnabled()
    3.  创建WorkerWebUI界面，也就是显示Worker信息的页面
    webUi = new WorkerWebUI(this, workDir, webUiPort)
    4. bind  web接口后面的Http server 
    webUi.bind()
    5. 向Master注册，这个才是最重要的
    registerWithMaster()
    6. 测量系统，这个就不多说了，和Master中的作用一摸一样
    metricsSystem.registerSource(workerSource)
    metricsSystem.start()
    // Attach the worker metrics servlet handler to the web ui after the metrics system is started.
    metricsSystem.getServletHandlers.foreach(webUi.attachHandler)
  }

```
接下来就看这个详细注册过程：

worker端：

```
   private def registerWithMaster() {
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      1. 刚开始时，registrationRetryTimer为None，所以会直接执行这里
      case None =>
      2. 刚开始时，registered为false，表示还未注册
        registered = false
      3. 向Master直接进行注册
        registerMasterFutures = tryRegisterAllMasters()
        connectionAttemptCount = 0
        registrationRetryTimer = Some(forwordMessageScheduler.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              Option(self).foreach(_.send(ReregisterWithMaster))
            }
          },
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          TimeUnit.SECONDS))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }
```
这里先直接看tryRegisterAllMasters这个方法：

```
    private def tryRegisterAllMasters(): Array[JFuture[_]] = {
    1. 这里会有多个master的原因是因为一般在使用standalone模式的话，都会使用ZooKeeper，这样的话，一般会有3个Master，但是只有一个是ALIVE状态的
    masterRpcAddresses.map { masterAddress =>
      registerMasterThreadPool.submit(new Runnable {
        override def run(): Unit = {
          try {
            logInfo("Connecting to master " + masterAddress + "...")
     2. 获取Master的Endpoint，并向其注册
             val masterEndpoint =
              rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterAddress, Master.ENDPOINT_NAME)
            registerWithMaster(masterEndpoint)
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        }
      })
    }
  }
  
```
registerWithMaster方法：
```
  private def registerWithMaster(masterEndpoint: RpcEndpointRef): Unit = {
  1.这里它就会直接向Master中发送注册的消息了
    masterEndpoint.ask[RegisterWorkerResponse](RegisterWorker(
      workerId, host, port, self, cores, memory, webUi.boundPort, publicAddress))
      .onComplete {
        // This is a very fast action so we can use "ThreadUtils.sameThread"
        case Success(msg) =>
          Utils.tryLogNonFatalError {
            handleRegisterResponse(msg)
          }
        case Failure(e) =>
          logError(s"Cannot register with master: ${masterEndpoint.address}", e)
          System.exit(1)
      }(ThreadUtils.sameThread)
  }
  
```
关于这个方法，我这里多分析一点，首先看这个ask：

```
   def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)
  
```
这里返回类型是Future，所以可以另启一个线程，用来处理结果，但是在这里，先看这个onComplete方法：

```
  def onComplete[U](func: Try[T] => U)(implicit executor: ExecutionContext): Unit
```
接着在看该程序中，第二个参数详细传入的是ThreadUtils.sameThread，所以这里当就这个线程来说，会阻塞等有返回结果后再继续执行，这里注意，master之间的注册是不会阻塞的，因为我们为每一个注册都分配了一个线程，只是在线程中阻塞，直到有返回结果或超时。

下面接着分析，当我们向Master发送注册信息后，Master的反应:

Master端：
```
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    1.匹配到worker注册的消息
    case RegisterWorker(
        id, workerHost, workerPort, workerRef, cores, memory, workerUiPort, publicAddress) => {
      logInfo("Registering worker %s:%d with %d cores, %s RAM".format(
        workerHost, workerPort, cores, Utils.megabytesToString(memory)))
   2. 该master若为standby状态，直接回复MasterInStandby，此时Worker不做任何处理
      if (state == RecoveryState.STANDBY) {
        context.reply(MasterInStandby)
      } else if (idToWorker.contains(id)) {
   3. 如果是已注册的woker，回复重复注册的失败消息
        context.reply(RegisterWorkerFailed("Duplicate worker ID"))
      } else {
    
        val worker = new WorkerInfo(id, workerHost, workerPort, cores, memory,
          workerRef, workerUiPort, publicAddress)
     4. 执行注册操作
        if (registerWorker(worker)) {
     5. 持久化 该worker     
         persistenceEngine.addWorker(worker)
     6. 返回该worker已注册的消息
          context.reply(RegisteredWorker(self, masterWebUiUrl))
     7. 重新资源分配，因为好多地方会调用这个方法，稍后集中分析它。
          schedule()
        } else {
          val workerAddress = worker.endpoint.address
          logWarning("Worker registration failed. Attempted to re-register worker at same " +
            "address: " + workerAddress)
       8. 返回又一次注册相同的地址的失败消息
          context.reply(RegisterWorkerFailed("Attempted to re-register worker at same address: "
            + workerAddress))
        }
      }
    }
  
```
这里看一下上面的这个registerWorker方法：


```
  private def registerWorker(worker: WorkerInfo): Boolean = {
    // There may be one or more refs to dead workers on this same node (w/ different ID's),
    // remove them.
    1. 如果该worker曾经注册过，但现在被标记为DEAD的状态，则先从原来的workers中删掉
    workers.filter { w =>
      (w.host == worker.host && w.port == worker.port) && (w.state == WorkerState.DEAD)
    }.foreach { w =>
      workers -= w
    }

    val workerAddress = worker.endpoint.address
    if (addressToWorker.contains(workerAddress)) {
      val oldWorker = addressToWorker(workerAddress)
      if (oldWorker.state == WorkerState.UNKNOWN) {
        // A worker registering from UNKNOWN implies that the worker was restarted during recovery.
        // The old worker must thus be dead, so we will remove it and accept the new worker.
     2. 如果满足这个条件的话，就说明在Master恢复的时候，worker却在重启（或者由于别的原因导致worker没有对master响应），所以在这里将这个worker标记为 UNKNOWN，所以如果这个worker在重启后，再向Master注册，Master就应该删除它原来的注册信息    
        removeWorker(oldWorker)
      } else {
    3. 重复注册错误
        logInfo("Attempted to re-register worker at same address: " + workerAddress)
        return false
      }
    }
    4. 将worker的信息重新加入相关的变量中
    workers += worker
    idToWorker(worker.id) = worker
    addressToWorker(workerAddress) = worker
    true
  }
  
```
接着在worker端看对Master回复信息的处理：

worker端，在onComplete处理接到的信息：

```

    masterEndpoint.ask[RegisterWorkerResponse](RegisterWorker(
      workerId, host, port, self, cores, memory, webUi.boundPort, publicAddress))
      .onComplete {
        // This is a very fast action so we can use "ThreadUtils.sameThread"
        case Success(msg) =>
          Utils.tryLogNonFatalError {
            handleRegisterResponse(msg)
          }
        case Failure(e) =>
          logError(s"Cannot register with master: ${masterEndpoint.address}", e)
          System.exit(1)
      }(ThreadUtils.sameThread)
      
```
这里我们上文已经很清晰的分析过了，下面就直接看handleRegisterResponse对回复的信息的处理：


```
private def handleRegisterResponse(msg: RegisterWorkerResponse): Unit = synchronized {
    msg match {
      case RegisteredWorker(masterRef, masterWebUiUrl) =>
        logInfo("Successfully registered with master " + masterRef.address.toSparkURL)
        1. 注册成功先修改这个变量的状态
        registered = true
        2. 改变该worker的Master的一些信息
        changeMaster(masterRef, masterWebUiUrl)
        3. 开始向Master发送心跳信息
        forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            self.send(SendHeartbeat)
          }
        }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
        4. 如果开启cleanup的话，也开始向master发送WorkDirCleanup消息
         if (CLEANUP_ENABLED) {
          logInfo(
            s"Worker cleanup enabled; old application directories will be deleted in: $workDir")
          forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              self.send(WorkDirCleanup)
            }
          }, CLEANUP_INTERVAL_MILLIS, CLEANUP_INTERVAL_MILLIS, TimeUnit.MILLISECONDS)
        }

      case RegisterWorkerFailed(message) =>
       5. 错误推出
        if (!registered) {
          logError("Worker registration failed: " + message)
          System.exit(1)
        }
       6. 不理会，程序正常执行
      case MasterInStandby =>
        // Ignore. Master not yet ready.
    }
  }
      
```
这里详细看一下changeMaster

```
 private def changeMaster(masterRef: RpcEndpointRef, uiUrl: String) {
    // activeMasterUrl it's a valid Spark url since we receive it from master.
    activeMasterUrl = masterRef.address.toSparkURL
    activeMasterWebUiUrl = uiUrl
    master = Some(masterRef)
    connected = true
    // Cancel any outstanding re-registration attempts because we found a new master
     取消重新注册的操作
    cancelLastRegistrationRetry()
  }

      
```
接着这个cancelLastRegistrationRetry，这里还要详解一下registerMasterFutures和registrationRetryTimer。
其中registerMasterFutures中保存向master注册的线程的future对象，因为这里它是通过在同一个线程中处理的，所以它会阻塞这个向master注册的运行的线程，但这里一般耗时都比较短，所以没什么影响。

registrationRetryTimer中保存的是一直启动注册事件的future对象
因为这里future对象会保存在根据处理的结果对结果处理的线程中，所以可以根据这个future对象取消相应的操作。
在这里就是不再向Master发送注册消息
```
  
  private def cancelLastRegistrationRetry(): Unit = {
    if (registerMasterFutures != null) {
      registerMasterFutures.foreach(_.cancel(true))
      registerMasterFutures = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }
        
```
在结束这篇博客前，再看一下这个registerWithMaster方法

```
  
   private def registerWithMaster() {
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      case None =>
        registered = false
        1. 上文中分析到了这里
        registerMasterFutures = tryRegisterAllMasters()
  
        connectionAttemptCount = 0
        2. 在INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS后，按固定时间向worker放送向Master注册的消息。
         registrationRetryTimer = Some(forwordMessageScheduler.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              Option(self).foreach(_.send(ReregisterWithMaster))
            }
          },
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          TimeUnit.SECONDS))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }       
   
```
接着就看下就收到ReregisterWithMaster这个消息的处理

```
case ReregisterWithMaster =>
      reregisterWithMaster()
```
再看reregisterWithMaster方法：


```
 /**
   * Re-register with the master because a network failure or a master failure has occurred.
   * If the re-registration attempt threshold is exceeded, the worker exits with error.
   * Note that for thread-safety this should only be called from the rpcEndpoint.
   */
  private def reregisterWithMaster(): Unit = {
    Utils.tryOrExit {
      1. 连接次数（其实这里指的就是注册次数）
      connectionAttemptCount += 1
      2. 如果注册完成了，就取消之后的重新注册的操作
      if (registered) {
        cancelLastRegistrationRetry()
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        logInfo(s"Retrying connection to master (attempt # $connectionAttemptCount)")
        /**
         * Re-register with the active master this worker has been communicating with. If there
         * is none, then it means this worker is still bootstrapping and hasn't established a
         * connection with a master yet, in which case we should re-register with all masters.
         *
         * It is important to re-register only with the active master during failures. Otherwise,
         * if the worker unconditionally attempts to re-register with all masters, the following
         * race condition may arise and cause a "duplicate worker" error detailed in SPARK-4592:
         *
         *   (1) Master A fails and Worker attempts to reconnect to all masters
         *   (2) Master B takes over and notifies Worker
         *   (3) Worker responds by registering with Master B
         *   (4) Meanwhile, Worker's previous reconnection attempt reaches Master B,
         *       causing the same Worker to register with Master B twice
         *
         * Instead, if we only register with the known active master, we can assume that the
         * old master must have died because another master has taken over. Note that this is
         * still not safe if the old master recovers within this interval, but this is a much
         * less likely scenario.
         */
        master match {
          case Some(masterRef) =>
            // registered == false && master != None means we lost the connection to master, so
            // masterRef cannot be used and we need to recreate it again. Note: we must not set
            // master to None due to the above comments.
            1. 这里看英文注释应该很清楚了，就是我们已经获取了masterRef这个通信句柄，但是却没注册成功，这就说明该链接断开了
            所以先取消原来阻塞的用来等待消息回复的线程
            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            val masterAddress = masterRef.address
            2. 然后在重新注册
            registerMasterFutures = Array(registerMasterThreadPool.submit(new Runnable {
              override def run(): Unit = {
                try {
                  logInfo("Connecting to master " + masterAddress + "...")
                  val masterEndpoint =
                    rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterAddress, Master.ENDPOINT_NAME)
                  registerWithMaster(masterEndpoint)
                } catch {
                  case ie: InterruptedException => // Cancelled
                  case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
                }
              }
            }))
          case None =>
            3. 如果没有获取master，所以先取消原来阻塞的用来等待消息回复的线程

            if (registerMasterFutures != null) {
              registerMasterFutures.foreach(_.cancel(true))
            }
            4.重新开始最初的注册
            // We are retrying the initial registration
            registerMasterFutures = tryRegisterAllMasters()
        }
        // We have exceeded the initial registration retry threshold
        // All retries from now on should use a higher interval
        5. 如果超过最初设定开始阶段的重新注册次数，改变重新注册次数，使时间间隔变长。
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel(true))
          registrationRetryTimer = Some(
            forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
              override def run(): Unit = Utils.tryLogNonFatalError {
                self.send(ReregisterWithMaster)
              }
            }, PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
              TimeUnit.SECONDS))
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }
```
ok，写到这里，standalone模式下最初的部署工作就完成了，然后就等待application提交了，其实在master中还有一个比较重要的方法schedule，这个设计资源调度，再之后的讲资源调度的时候，会详细介绍。

最后的最后，在画一个流程图，其实本人不爱画流程图，因为它忽略了很多实现的细节，不过，流程图确实有助于理解。其实部署的工作实现的很简单：



![Screen Shot 2016-11-30 at 4.46.28 PM.png](http://upload-images.jianshu.io/upload_images/3736220-1bb243fd9fe6fe3b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


