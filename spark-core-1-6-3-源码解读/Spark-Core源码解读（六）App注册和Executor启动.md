**人不为己，天诛地灭：其实这句话中的‘为’应该是二声，也就是人如果不做自己的话**

昨天有最重要的一部分没有分析，今天再讲之前，先看一幅任务调度的流程图，而后再对其细节加以分析，因为这样效果要好一些，对我也对大家，我分析的清楚，您听的明白。

![Screen Shot 2016-12-06 at 12.37.03 AM.png](http://upload-images.jianshu.io/upload_images/3736220-2cbc1170f8d2699f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
    
然后就直接分析源码：
```
    // Create and start the scheduler
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
```

这一次直接根据注释来分析。

### the scheduler（driver端）

直接看创建scheduler的方法，这个方法中根据不同的模式，会有不同的实现，这里只看standalone模式下的：
```
    case SPARK_REGEX(sparkUrl) =>
        val scheduler = new TaskSchedulerImpl(sc)
        val masterUrls = sparkUrl.split(",").map("spark://" + _)
        val backend = new SparkDeploySchedulerBackend(scheduler, sc, masterUrls)
        scheduler.initialize(backend)
        (backend, scheduler)
```
关于TaskSchedulerImpl和SparkDeploySchedulerBackend的初始化，大家务必自己看一下，具体的细节我就不贴出来了，但是放心里面用到的关键的变量我肯定会加以分析的：
这里直接看  scheduler.initialize(backend)这个方法的实现：

    
```
    def initialize(backend: SchedulerBackend) {
    this.backend = backend
    // temporarily set rootPool name to empty
    rootPool = new Pool("", schedulingMode, 0, 0)
    1. 指定job的调度模式
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, conf)
      }
    }
    2. 一般都会使用FIFO的，他的buildPools其实什么都没做
    schedulableBuilder.buildPools()
  }

```
scheduler（TaskSchedulerImpl）的initialize方法最终是将是将SparkDeploySchedulerBackend和schedulableBuilder绑定到TaskSchedulerImpl中。

接着看代码的执行，接着就是new一个DAGScheduler了，这个DAGScheduler在当前的资源分配阶段是没什么用的，它是用于在构建job的阶段的。接着就是启动_taskScheduler.start：

    
    
```
     1. 启动这个SparkDeploySchedulerBackend
  backend.start()

    if (!isLocal && conf.getBoolean("spark.speculation", false)) {
      logInfo("Starting speculative execution thread")
      2. 检查慢任务
      speculationScheduler.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
          checkSpeculatableTasks()
        }
      }, SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
```
这里我先不展开分析，先把这个流程走通。接着看  backend.start()

```
override def start() {
    1. 这里的start方法是在SparkDeploySchedulerBackend中，而SparkDeploySchedulerBackend继承自CoarseGrainedSchedulerBackend，下面这一行代码启动的就是CoarseGrainedSchedulerBackend中的start方法，在start方法中会创建一个至关重要的类DriverEndpoint的对象，因为它是整个Application运行的驱动器。
    super.start()
    2. 这个用于与LauncherServer进行交流，而这个LauncherServer是部署在集群之外的应用上，除非你的系统需要这样来实现，一般是不会启动LauncherServer。这里除非你指定LauncherServer的secretkey和port，它是不会进行连接操作的。所以默认情况下，这行代码其实什么也没做
    launcherBackend.connect()

    // The endpoint for executors to talk to us
    3.这个的driverUrl其实就是指的上文中创建的DriverEndpoint的对象
    val driverUrl = rpcEnv.uriOf(SparkEnv.driverActorSystemName,
      RpcAddress(sc.conf.get("spark.driver.host"),
sc.conf.get("spark.driver.port").toInt),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
    4. 构建Executor运行需要的配置参数
    val args = Seq(
      "--driver-url", driverUrl,
      "--executor-id", "{{EXECUTOR_ID}}",
      "--hostname", "{{HOSTNAME}}",
      "--cores", "{{CORES}}",
      "--app-id", "{{APP_ID}}",
      "--worker-url", "{{WORKER_URL}}")
    val extraJavaOpts = sc.conf.getOption("spark.executor.extraJavaOptions")
      .map(Utils.splitCommandString).getOrElse(Seq.empty)
    val classPathEntries = sc.conf.getOption("spark.executor.extraClassPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)
    val libraryPathEntries = sc.conf.getOption("spark.executor.extraLibraryPath")
      .map(_.split(java.io.File.pathSeparator).toSeq).getOrElse(Nil)

    // When testing, expose the parent class path to the child. This is processed by
    // compute-classpath.{cmd,sh} and makes all needed jars available to child processes
    // when the assembly is built with the "*-provided" profiles enabled.
    val testingClassPath =
      if (sys.props.contains("spark.testing")) {
        sys.props("java.class.path").split(java.io.File.pathSeparator).toSeq
      } else {
        Nil
      }

    // Start executors with a few necessary configs for registering with the scheduler
    val sparkJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
     5. 通过Command的方式来指定，在worker的运行Executor的类
    val command = Command("org.apache.spark.executor.CoarseGrainedExecutorBackend",
      args, sc.executorEnvs, classPathEntries ++ testingClassPath, libraryPathEntries, javaOpts)
    val appUIAddress = sc.ui.map(_.appUIAddress).getOrElse("")
    val coresPerExecutor = conf.getOption("spark.executor.cores").map(_.toInt) 
    val appDesc = new ApplicationDescription(sc.appName, maxCores, sc.executorMemory,
      command, appUIAddress, sc.eventLogDir, sc.eventLogCodec, coresPerExecutor)
    6. 就是真正去提交App的类，当然也是非常重要的
    client = new AppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
    client.start()
    launcherBackend.setState(SparkAppHandle.State.SUBMITTED)
    7. 等待App注册成功，记住它是会阻塞的，直到注册成功或失败
    waitForRegistration()
    launcherBackend.setState(SparkAppHandle.State.RUNNING)
  }

```
紧接着就看这个AppClient的start方法干什么了：
```
def start() {
    // Just launch an rpcEndpoint; it will call back into the listener.
    endpoint.set(rpcEnv.setupEndpoint("AppClient", new ClientEndpoint(rpcEnv)))
  }
```
它这里会创建一个Endpoint，我要的就是这个Endpoint，还记得之前谈过的嘛？ 在RPC中创建Endpoint后会去调用其OnStart方法（关于这部分的具体实现，我会专门写一篇关于RPC协议的博客），那接下来就是要看ClientEndpoint中的OnStart方法了：

```
  override def onStart(): Unit = {
      try {
        registerWithMaster(1)
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          stop()
      }
    }
```
看到了，向Master注册这个App，接着看她怎么具体实现的：

```
  private def registerWithMaster(nthRetry: Int) {
      1. 获取每个注册线程的future对象
      registerMasterFutures.set(tryRegisterAllMasters())
      2. 启动一个周期检测future结果的线程        registrationRetryTimer.set(registrationRetryThread.scheduleAtFixedRate(new Runnable {
        override def run(): Unit = {
          3.若已注册，则取消所有注册线程，及其线程池
          if (registered.get) {
            registerMasterFutures.get.foreach(_.cancel(true))
            registerMasterThreadPool.shutdownNow()
          3. 若注册超过最多的注册次数，放弃
          } else if (nthRetry >= REGISTRATION_RETRIES) {
            markDead("All masters are unresponsive! Giving up.")
          } else {
          4. 继续注册
            registerMasterFutures.get.foreach(_.cancel(true))
            registerWithMaster(nthRetry + 1)
          }
        }
      }, REGISTRATION_TIMEOUT_SECONDS, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS))
    }
```
接着看它怎么实现的这个注册：
```
 private def tryRegisterAllMasters(): Array[JFuture[_]] = {
      根据master的地址分别启动与master个数相等的线程来向master发送RegisterApplication消息
      for (masterAddress <- masterRpcAddresses) yield {
        registerMasterThreadPool.submit(new Runnable {
          override def run(): Unit = try {
            if (registered.get) {
              return
            }
            logInfo("Connecting to master " + masterAddress.toSparkURL + "...")
            val masterRef =
              rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, masterAddress, Master.ENDPOINT_NAME)
            masterRef.send(RegisterApplication(appDescription, self))
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
          }
        })
      }
    }
       
```
上面的这些内容全部都是在driver端实现的，根据他发送的消息，这里直接撞到master中。

### RegisterApplication（Master）

```
   case RegisterApplication(description, driver) => {
      // TODO Prevent repeated registrations from some driver
      if (state == RecoveryState.STANDBY) {
        // ignore, don't send response
      } else {
        logInfo("Registering app " + description.name) 
        val app = createApplication(description, driver)
        registerApplication(app)
        logInfo("Registered app " + description.name + " with ID " + app.id)
        1. 持久化app信息，放到HDFS上
        persistenceEngine.addApplication(app)
        2. 这里的driver其实指的是AppClient
        driver.send(RegisteredApplication(app.id, self))
        schedule()
      }
    }
       
```
下面还要看一下createApplication和registerApplication两个方法：
```
  private def createApplication(desc: ApplicationDescription, driver: RpcEndpointRef):
      ApplicationInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    1.由此可见appId其实是在Master端生成的
    val appId = newApplicationId(date)
    new ApplicationInfo(now, appId, desc, date, driver, defaultCores)
  }
       
```
这里看一下newApplicationId方法，它这里其实是根据时间来构造的appId：
```
   /** Generate a new app ID given a app's submission date */
  private def newApplicationId(submitDate: Date): String = {
    val appId = "app-%s-%04d".format(createDateFormat.format(submitDate), nextAppNumber)
    nextAppNumber += 1
    appId
  }       
```
接着再看注册的方法：
```
  private def registerApplication(app: ApplicationInfo): Unit = {
    val appAddress = app.driver.address
    if (addressToApp.contains(appAddress)) {
      logInfo("Attempted to re-register application at same address: " + appAddress)
      return
    }

    applicationMetricsSystem.registerSource(app.appSource)
    apps += app
    idToApp(app.id) = app
    endpointToApp(app.driver) = app
    addressToApp(appAddress) = app
    waitingApps += app
  }
```
其实它这里就是把应用的信息记录在Master中。

### app注册完成
app注册成功后，会向driver发送RegisteredApplication消息：

```
       case RegisteredApplication(appId_, masterRef) =>
        // FIXME How to handle the following cases?
        // 1. A master receives multiple registrations and sends back multiple
        // RegisteredApplications due to an unstable network.
        // 2. Receive multiple RegisteredApplication from different masters because the master is
        // changing.
        1. 这里是spark的一个bug，就是它也没有处理同一个app由于意外，接收到两次RegisteredApplication的问题
        appId.set(appId_)
        registered.set(true)
        master = Some(masterRef)
        2. 上面的都好理解，直接看它的实现
        listener.connected(appId.get)
```
先看这个listener是什么，首先看AppClient的类

```
  private[spark] class AppClient(
    rpcEnv: RpcEnv,
    masterUrls: Array[String],
    appDescription: ApplicationDescription,
    listener: AppClientListener,
    conf: SparkConf)
  extends Logging {
  
```
这个listener也是AppClientListener类型的，接着再看new的时候，在SparkDeploySchedulerBackend中的

```
   client = new AppClient(sc.env.rpcEnv, masters, appDesc, this, conf)
```
这行代码会指定其实这个listener就是SparkDeploySchedulerBackend，所以就直接看它的connected方法：

```
       override def connected(appId: String) {
    logInfo("Connected to Spark cluster with app ID " + appId)
    this.appId = appId
    这里重要的是这个方法，再看他的实现
    notifyContext()
    launcherBackend.setAppId(appId)
  }
    
```
这里我把两个方法放一起：
```
   1. 这个就是前面也就是在SparkDeploySchedulerBackend的start方法中调用的下面的方法，从而导致driver端现在的程序还在阻塞
    private def waitForRegistration() = {
    registrationBarrier.acquire()
  }
  2. 上面的代码调用了这个方法，就是发送一个信号，取消上面的阻塞，现在driver端的程序会继续执行了，但是现在先不讲driver端的内容。
  private def notifyContext() = {
    registrationBarrier.release()
  }
      
```
###  启动Executor
接着还继续将Master端的程序运行：master给driver发送完RegisteredApplication会直接调用schedule()方法了。前面分析过schedule()其实就是启动Driver和Executor的，那么Driver其实已经启动完了，下面就该为app启动Executor了。

```
   private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit = {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    1. 将这个添加的executor记录在worker中
    worker.addExecutor(exec)
    2. 向worker发送信息启动Executor
    worker.endpoint.send(LaunchExecutor(masterUrl,
      exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory))
    3.向driver（AppClient）发送ExecutorAdded，在这里driver其实什么都没做只是打印了一些信息。
    exec.application.driver.send(
      ExecutorAdded(exec.id, worker.id, worker.hostPort, exec.cores, exec.memory))
  }
        
```
下面接直接看worker端Executor的启动了：

```
  case LaunchExecutor(masterUrl, appId, execId, appDesc, cores_, memory_) =>
      1. 必须保证worker的眼收到的是ALIVE状态的master的消息
      if (masterUrl != activeMasterUrl) {
        logWarning("Invalid Master (" + masterUrl + ") attempted to launch executor.")
      } else {
        try {
          logInfo("Asked to launch executor %s/%d for %s".format(appId, execId, appDesc.name))

          // Create the executor's working directory
          2. 在HDFS上创建工作目录
          val executorDir = new File(workDir, appId + "/" + execId)
          if (!executorDir.mkdirs()) {
            throw new IOException("Failed to create directory " + executorDir)
          }

          // Create local dirs for the executor. These are passed to the executor via the
          // SPARK_EXECUTOR_DIRS environment variable, and deleted by the Worker when the
          // application finishes.
          val appLocalDirs = appDirectories.get(appId).getOrElse {
            Utils.getOrCreateLocalRootDirs(conf).map { dir =>
              val appDir = Utils.createDirectory(dir, namePrefix = "executor")
              Utils.chmod700(appDir)
              appDir.getAbsolutePath()
            }.toSeq
          }
          appDirectories(appId) = appLocalDirs
          3. 这才是最重要的，由此看来executor和driver的创建很类似，都是通过一个代理类来实现的，其实其创建的步骤根本就是一样的，甚至代码都是一样的
          val manager = new ExecutorRunner(
            appId,
            execId,
            appDesc.copy(command = Worker.maybeUpdateSSLSettings(appDesc.command, conf)),
            cores_,
            memory_,
            self,
            workerId,
            host,
            webUi.boundPort,
            publicAddress,
            sparkHome,
            executorDir,
            workerUri,
            conf,
            appLocalDirs, ExecutorState.RUNNING)
          executors(appId + "/" + execId) = manager
          4. 启动这个代理类创建Executor
          manager.start()
          coresUsed += cores_
          memoryUsed += memory_
          5. 向master汇报ExecutorStateChanged的消息
          sendToMaster(ExecutorStateChanged(appId, execId, manager.state, None, None))
        } catch {
          case e: Exception => {
            logError(s"Failed to launch executor $appId/$execId for ${appDesc.name}.", e)
            if (executors.contains(appId + "/" + execId)) {
              executors(appId + "/" + execId).kill()
              executors -= appId + "/" + execId
            }
            sendToMaster(ExecutorStateChanged(appId, execId, ExecutorState.FAILED,
              Some(e.toString), None))
          }
        }
      }
              
```
下面就直接看这个    manager.start()方法：
```
    1. 它又起了一个线程来启动Executor
    private[worker] def start() {
    workerThread = new Thread("ExecutorRunner for " + fullId) {
      override def run() { fetchAndRunExecutor() }
    }
    workerThread.start()
     2.这又是一个shutdown hook，前面多次讲过这里就不分析了
    // Shutdown hook that kills actors on shutdown.
    shutdownHook = ShutdownHookManager.addShutdownHook { () =>
      // It's possible that we arrive here before calling `fetchAndRunExecutor`, then `state` will
      // be `ExecutorState.RUNNING`. In this case, we should set `state` to `FAILED`.
      if (state == ExecutorState.RUNNING) {
        state = ExecutorState.FAILED
      }
      killProcess(Some("Worker shutting down")) }
  }
              
```
继续了，又是一大堆代码，别着急啊，其实下面很简单，好多内容这篇博客中已经讲过了：
```
private def fetchAndRunExecutor() {
    try {
      // Launch the process
      1. 通过builder创建Executor，像创建driver一样
      val builder = CommandUtils.buildProcessBuilder(appDesc.command, new SecurityManager(conf),
        memory, sparkHome.getAbsolutePath, substituteVariables)
      val command = builder.command()
      val formattedCommand = command.asScala.mkString("\"", "\" \"", "\"")
      logInfo(s"Launch command: $formattedCommand")

      builder.directory(executorDir)
      builder.environment.put("SPARK_EXECUTOR_DIRS", appLocalDirs.mkString(File.pathSeparator))
      // In case we are running this from within the Spark Shell, avoid creating a "scala"
      // parent process for the executor command
      builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")

      // Add webUI log urls
      val baseUrl =
        s"http://$publicAddress:$webUiPort/logPage/?appId=$appId&executorId=$execId&logType="
      builder.environment.put("SPARK_LOG_URL_STDERR", s"${baseUrl}stderr")
      builder.environment.put("SPARK_LOG_URL_STDOUT", s"${baseUrl}stdout")
      2. 真正启动Executor，也就是CoarseGrainedExecutorBackend
      process = builder.start()
      val header = "Spark Executor Command: %s\n%s\n\n".format(
        formattedCommand, "=" * 40)

      // Redirect its stdout and stderr to files
      val stdout = new File(executorDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf)

      val stderr = new File(executorDir, "stderr")
      Files.write(header, stderr, UTF_8)
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf)

      // Wait for it to exit; executor may exit with code 0 (when driver instructs it to shutdown)
      // or with nonzero exit code

      3. 这一步会导致程序的阻塞，直到这个process正常或异常退出
      val exitCode = process.waitFor()
      state = ExecutorState.EXITED
      val message = "Command exited with code " + exitCode
      4. 然后像worker发送ExecutorStateChanged的消息
      worker.send(ExecutorStateChanged(appId, execId, state, Some(message), Some(exitCode)))
    } catch {
      case interrupted: InterruptedException => {
        logInfo("Runner thread for executor " + fullId + " interrupted")
        state = ExecutorState.KILLED
        killProcess(None)
      }
      case e: Exception => {
        logError("Error running executor", e)
        state = ExecutorState.FAILED
        killProcess(Some(e.toString))
      }
    }
  }              
```
接着就是这个真正的Executor的启动了，即CoarseGrainedExecutorBackend
的main方法：

```
 def main(args: Array[String]) {
    var driverUrl: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit()
      }
    }

    if (driverUrl == null || executorId == null || hostname == null || cores <= 0 ||
      appId == null) {
      printUsageAndExit()
    }

    run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath)
  }           
```
它这里的参数信息在个地方构建，一个在SparkDeploySchedulerBackend的start中，另一个在ExecutorRunner在fetchAndRunExecutor构建builder的时候通过substituteVariables构建，大家有时间务必看一下。
接着看这个run方法：
```
private def run(
      driverUrl: String,
      executorId: String,
      hostname: String,
      cores: Int,
      appId: String,
      workerUrl: Option[String],
      userClassPath: Seq[URL]) {
    1. 注册log类
    SignalLogger.register(log)
   2. 这方法先不说，它这里就是先起了一个线程来执行后面的代码，你先这么理解就可以，之后我再详细给大家介绍
    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(hostname)
      // Bootstrap to fetch the driver's Spark properties.
      val executorConf = new SparkConf
      val port = executorConf.getInt("spark.executor.port", 0)
    3. 这个fetcher只是创建了一个RpcEnv用来获得driver的ref就关闭了
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        hostname,
        port,
        executorConf,
        new SecurityManager(executorConf),
        clientMode = true)
      val driver = fetcher.setupEndpointRefByURI(driverUrl)
     4. 它也获得了driver端的spark配置参数
      val props = driver.askWithRetry[Seq[(String, String)]](RetrieveSparkProps) ++
        Seq[(String, String)](("spark.app.id", appId))
      fetcher.shutdown()

      // Create SparkEnv using properties we fetched from the driver.
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }
      5.这一部分可能大家会有疑问，这里如果是yarn模式下，他其实一直在跟新和HDFS交互的密钥信息
      if (driverConf.contains("spark.yarn.credentials.file")) {
        logInfo("Will periodically update credentials from: " +
          driverConf.get("spark.yarn.credentials.file"))
        SparkHadoopUtil.get.startExecutorDelegationTokenRenewer(driverConf)
      }
      6. 创建ExecutorEnv，代码和Driver端的基本一摸一样，我就不废口舌了，也就是初始化关键的变量，也就是上篇博客中的SparkEnv的那些变量
      val env = SparkEnv.createExecutorEnv(
        driverConf, executorId, hostname, port, cores, isLocal = false)

      // SparkEnv will set spark.executor.port if the rpc env is listening for incoming
      // connections (e.g., if it's using akka). Otherwise, the executor is running in
      // client mode only, and does not accept incoming connections.
      val sparkHostPort = env.conf.getOption("spark.executor.port").map { port =>
          hostname + ":" + port
        }.orNull
       7. 这里创建了一个Endpoint用于与driver交互执行task。由此可以看出Executor的真正实现是CoarseGrainedExecutorBackend
      env.rpcEnv.setupEndpoint("Executor", new CoarseGrainedExecutorBackend(
        env.rpcEnv, driverUrl, executorId, sparkHostPort, cores, userClassPath, env))
      workerUrl.foreach { url =>
     8. WorkerWatcher就是用来监视worker的，当与worker断开连接时，它将会关闭这个jvm进程    
     env.rpcEnv.setupEndpoint("WorkerWatcher", new WorkerWatcher(env.rpcEnv, url))
      }
      env.rpcEnv.awaitTermination()
  8. 停止向HDFS获取密钥     SparkHadoopUtil.get.stopExecutorDelegationTokenRenewer()
    }

```
创建了Endpoint就不得不看一下它的OnStart方法，那就直接来看吧：

```
     override def onStart() {
    logInfo("Connecting to driver: " + driverUrl)
    1.获取driver(DriverEndpoint)的endpoint的引用
    rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      driver = Some(ref)
      2. 向driver(DriverEndpoint)注册
      ref.ask[RegisterExecutorResponse](
        RegisterExecutor(executorId, self, hostPort, cores, extractLogUrls))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      3. 将注册返回的消息发送给自己
      case Success(msg) => Utils.tryLogNonFatalError {
        Option(self).foreach(_.send(msg)) // msg must be RegisterExecutorResponse
      }
      case Failure(e) => {
        logError(s"Cannot register with driver: $driverUrl", e)
        System.exit(1)
      }
    }(ThreadUtils.sameThread)
  }
```
ok，到这里就完全分析完了Executor的启动了，然后这里还有一段代码，需要说明一下，还记着前面TaskSchedulerImpl.start的方法阻塞状态被消除了，然后它接下来要执行的是：
driver端的SparkContext的初始化
```
  // constructor
    _taskScheduler.start()
    
    1. 给各个需要appId的组件或变量指定其appId
    _applicationId = _taskScheduler.applicationId()
    _applicationAttemptId = taskScheduler.applicationAttemptId()
    _conf.set("spark.app.id", _applicationId)
    _ui.foreach(_.setAppId(_applicationId))
   2. 另外关于spark初始化还有这一步比较重要的操作，就是blockManager的初始化。关于block操作的整个系统稍后会写一篇博客的
    _env.blockManager.initialize(_applicationId)
       
```
OK，到这里Driver的初始化工作完成了，Worker的Executor也启动了，接下来把Executor的启动结尾讲完，所以下篇博客我们会接着今天executor向driver发送RegisterExecutor消息讲起。
