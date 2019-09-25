**上士闻道，勤而行之，中士闻道，若存若亡；下士闻道，大笑之。不笑不足以为道。**

### RequestSubmitDriver处理
从现在开始，就真正的进入spark core的核心部分了，所以为了能让大家更好的了解相关的知识，我尽量每一次在代码讲解完或者讲解前，画一幅图片加以说明大致的流程，关于代码细节还请看我后面的分析：


![Screen Shot 2016-12-04 at 3.00.38 AM.png](http://upload-images.jianshu.io/upload_images/3736220-abd2a20fd549c35c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)



好了，话接上文，最后我们讲到restServer会向master发送注册driver的消息：
```
  val response = masterEndpoint.askWithRetry[DeployMessages.SubmitDriverResponse](
          DeployMessages.RequestSubmitDriver(driverDescription))
          
```
然后接着就看master中对此消息的处理吧！
```
 case RequestSubmitDriver(description) => {
      1. master需要是ALIVE的
      if (state != RecoveryState.ALIVE) {
        val msg = s"${Utils.BACKUP_STANDALONE_MASTER_PREFIX}: $state. " +
          "Can only accept driver submissions in ALIVE state."
       2. 向restServer返回提交driver失败的消息
        context.reply(SubmitDriverResponse(self, false, None, msg))
      } else {
        logInfo("Driver submitted " + description.command.mainClass)
       3. 根据传入的driverdescription构建driverInfo
        val driver = createDriver(description)
       4. 将driverInfo添加的持久化引擎中，持久化引擎是cluster实现HA的一个重要组件，包括：ZOOKEEPER，FILESYSTEM，CUSTOM三种
        persistenceEngine.addDriver(driver)
       5.将driver信息添加到等待启动的driver列表中，启动后就在waitingDrivers中remove掉
        waitingDrivers += driver
       6.这个drivers变量，是master用来记录的正在运行的driver，在application执行结束后，才从drivers中remove掉
        drivers.add(driver)
        7. 资源分配，启动driver和Executor，这个在Spark Core源码解读（二）－schedule中详细讲解过
        schedule()

        // TODO: It might be good to instead have the submission client poll the master to determine
        //       the current status of the driver. For now it's simply "fire and forget".
       8. 返回一个提交driver成功的消息
        context.reply(SubmitDriverResponse(self, true, Some(driver.id),
          s"Driver successfully submitted as ${driver.id}"))
      }
    }
           
```
下面简单看一下这个前面提到的createDriver方法：
```
private def createDriver(desc: DriverDescription): DriverInfo = {
    val now = System.currentTimeMillis()
    val date = new Date(now)
    new DriverInfo(now, newDriverId(date), desc, date)
  }
           
```
返回的确实是DriverInfo。

### 启动Driver

上面这个处理完了，会直接进行schedule的操作，从而启动driver和Executors，本博客先看Driver的启动，关于在Master端的操作在第二篇博客中已经详细介绍，下面直接看在worker的处理：

#### worker端启动driver
首先在schedule之后，发出的启动driver消息的操作：

master端：
```
  private def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    worker.addDriver(driver)
    driver.worker = Some(worker)
    worker.endpoint.send(LaunchDriver(driver.id, driver.desc))
    driver.state = DriverState.RUNNING
  }       
```
那么看在worker端的处理吧：
worker端：
```

    case LaunchDriver(driverId, driverDesc) => {
      logInfo(s"Asked to launch driver $driverId")
      val driver = new DriverRunner(
        conf,
        driverId,
        workDir,
        sparkHome,
        driverDesc.copy(command = Worker.maybeUpdateSSLSettings(driverDesc.command, conf)),
        self,
        workerUri,
        securityMgr)
      drivers(driverId) = driver
      driver.start()

      coresUsed += driverDesc.cores
      memoryUsed += driverDesc.mem
    }              
```
这里说明几点：
1. driver的启动是通过DriverRunner这个代理类来实现的
2. command参数的构造
下面直接看一下这个构造的方法：

```
  def maybeUpdateSSLSettings(cmd: Command, conf: SparkConf): Command = {
    val prefix = "spark.ssl."
    val useNLC = "spark.ssl.useNodeLocalConf"
    if (isUseLocalNodeSSLConfig(cmd)) {
      val newJavaOpts = cmd.javaOpts
          .filter(opt => !opt.startsWith(s"-D$prefix")) ++
          conf.getAll.collect { case (key, value) if key.startsWith(prefix) => s"-D$key=$value" } :+
          s"-D$useNLC=true"
      cmd.copy(javaOpts = newJavaOpts)
    } else {
      cmd
    }
  }
下面再直接写出isUseLocalNodeSSLConfig这个方法

  def isUseLocalNodeSSLConfig(cmd: Command): Boolean = {
    val pattern = """\-Dspark\.ssl\.useNodeLocalConf\=(.+)""".r
    val result = cmd.javaOpts.collectFirst {
      case pattern(_result) => _result.toBoolean
    }
    result.getOrElse(false)
  }

```
这里详细讲解这个方法在做什么，其实它这里就是验证下spark下的通信是否需要本地[SSL](http://baike.baidu.com/link?url=ZXwCwwukQHFWjFbVYG-I-JAPewuvS2RjUFSI4rAhMOnZWg0gnPO0aXcVfRTBkmoy5rXfsqmCYdonWnMkxa2JJ_)，上面方法的实现首先通过isUseLocalNodeSSLConfig验证是否有匹配到pattern内容中的参数，如果有的话，再进行maybeUpdateSSLSettings中的if方法，if方法就是通过原来的javaOpts和当前的conf来重新构造新的javaOpts。

另外这个command参数是非常重要的，它还包含有我们要启动的driver类及其参数等。
这里的command也就是上篇博客中提到的在restServer中调用buildDriverDescription方法构建的，即
```
val command = new Command(
      "org.apache.spark.deploy.worker.DriverWrapper",
      Seq("{{WORKER_URL}}", "{{USER_JAR}}", mainClass) ++ appArgs, // args to the DriverWrapper
      environmentVariables, extraClassPath, extraLibraryPath, javaOpts)
  
```
ok，然后看DriverRunner的start方法，看它是如何启动driver的：

```
/** Starts a thread to run and manage the driver. */
  private[worker] def start() = {
    1.由此可以看出它通过DriverRunner这个代理，在启动一个线程来运行driver的实现类的
    new Thread("DriverRunner for " + driverId) {
      override def run() {
        try {
   2. 创建driver的工作目录
          val driverDir = createWorkingDirectory()
   3. 获取user的jar包，把它放到本地，也就是包含有spark运行的实现业务的类的jar，在本例中指的是 /path/to/examples.jar 这个jar包
          val localJarFilename = downloadUserJar(driverDir)
   4. 在上面的command中有{{WORKER_URL}}，{{USER_JAR}}，在进行参数转换的时候转变为其具体的值，如workerUrl，localJarFilename
          def substituteVariables(argument: String): String = argument match {
            case "{{WORKER_URL}}" => workerUrl
            case "{{USER_JAR}}" => localJarFilename
            case other => other
          }

          // TODO: If we add ability to submit multiple jars they should also be added here
        5. 这里其实它的实现是构建了一个ProcessBuilder，然后通过它来启动相应的driver类的，关于ProcessBuilder的使用可以看下http://lavasoft.blog.51cto.com/62575/15662
          val builder = CommandUtils.buildProcessBuilder(driverDesc.command, securityManager,
            driverDesc.mem, sparkHome.getAbsolutePath, substituteVariables)
       6. 正式启动driver的操作
            launchDriver(builder, driverDir, driverDesc.supervise)
        }
        catch {
          case e: Exception => finalException = Some(e)
        }
       7. 修改启动后的DriverState状态
        val state =
          if (killed) {
            DriverState.KILLED
          } else if (finalException.isDefined) {
            DriverState.ERROR
          } else {
            finalExitCode match {
              case Some(0) => DriverState.FINISHED
              case _ => DriverState.FAILED
            }
          }

        finalState = Some(state)
       8. 向worker汇报DriverStateChanged的消息
        worker.send(DriverStateChanged(driverId, state, finalException))
      }
    }.start()
  }  
```
接着肯定就是这个launchDriver方法了：

```
  private def launchDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean) {
    1. 指定builder的目录
    builder.directory(baseDir)
    2. 指定process的InputStream和ErrorStream的输出文件
    def initialize(process: Process): Unit = {
      // Redirect stdout and stderr to files
      val stdout = new File(baseDir, "stdout")
      CommandUtils.redirectStream(process.getInputStream, stdout)

      val stderr = new File(baseDir, "stderr")
      val formattedCommand = builder.command.asScala.mkString("\"", "\" \"", "\"")
      val header = "Launch Command: %s\n%s\n\n".format(formattedCommand, "=" * 40)
      Files.append(header, stderr, UTF_8)
      CommandUtils.redirectStream(process.getErrorStream, stderr)
    }
    3. 通过command启动其中的driver类
    runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
  }
```
具体看下runCommandWithRetry方法，这里面的内容很有意思：

```
  def runCommandWithRetry(
      command: ProcessBuilderLike, initialize: Process => Unit, supervise: Boolean): Unit = {
    // Time to wait between submission retries.
    1. 通过这个参数指定重试的submission之间的时间间隔，初始值为1s
    var waitSeconds = 1
    // A run of this many seconds resets the exponential back-off.
    2. 若启动时长超过5s，那么如果失败，只需sleep 1s，即可重试
    val successfulRunDuration = 5
    3. 如果没有被killed，driver会重启
    var keepTrying = !killed

    while (keepTrying) {
      logInfo("Launch Command: " + command.command.mkString("\"", "\" \"", "\""))

      synchronized {
        if (killed) { return }
        4. 启动具体的driver实现类也就是之前command中提到的org.apache.spark.deploy.worker.DriverWrapper
        process = Some(command.start())
       5. 这个initialize就是上面提到的方法中传入的initialize这个方法
        initialize(process.get)
      }

      val processStart = clock.getTimeMillis()
      6. 这个线程会一直等待，直到command.start()中的代码成功执行
      val exitCode = process.get.waitFor()
     6.   若启动时长超过5s，那么如果失败，只需sleep 1s，即可重试
      if (clock.getTimeMillis() - processStart > successfulRunDuration * 1000) {
        waitSeconds = 1
      }
     7. 若启动失败，且需要重启，则等待waitSeconds后在重启
      if (supervise && exitCode != 0 && !killed) {
        logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
        sleeper.sleep(waitSeconds)
        waitSeconds = waitSeconds * 2 // exponential back-off
      }
      8. 是否还继续重启，当然启动成功的话，该值就为false了
      keepTrying = supervise && exitCode != 0 && !killed
      9. 记录最终的exitCode
      finalExitCode = Some(exitCode)
    }
  }
 ```
写到这里就会执行org.apache.spark.deploy.worker.DriverWrapper这个真正的driver类了：

```
object DriverWrapper {
  def main(args: Array[String]) {
    args.toList match {
      /*
       * IMPORTANT: Spark 1.3 provides a stable application submission gateway that is both
       * backward and forward compatible across future Spark versions. Because this gateway
       * uses this class to launch the driver, the ordering and semantics of the arguments
       * here must also remain consistent across versions.
       */
      1. 这些参数都是通过command中的arguments这个变量得到的，代表的意思依次是该woker的ip地址，业务类要使用的jar，业务类，业务类的参数
      case workerUrl :: userJar :: mainClass :: extraArgs =>
        val conf = new SparkConf()
      2. 创建driver端的Rpc系统
        val rpcEnv = RpcEnv.create("Driver",
          Utils.localHostName(), 0, conf, new SecurityManager(conf))
        创建一个监视worker的endpoint
        rpcEnv.setupEndpoint("workerWatcher", new WorkerWatcher(rpcEnv, workerUrl))
       3. 类加载器ClassLoader，关于下面两种前面已经介绍过，再次不多说
        val currentLoader = Thread.currentThread.getContextClassLoader
        val userJarUrl = new File(userJar).toURI().toURL()
        val loader =
          if (sys.props.getOrElse("spark.driver.userClassPathFirst", "false").toBoolean) {
            new ChildFirstURLClassLoader(Array(userJarUrl), currentLoader)
          } else {
            new MutableURLClassLoader(Array(userJarUrl), currentLoader)
          }
        Thread.currentThread.setContextClassLoader(loader)

        // Delegate to supplied main class
        4. 下面就是使用反射的方式启动相应业务类中的main方法，这里可以看出这个业务类是直接运行在driver上的。
        业务类在本例中指的就是org.apache.spark.examples.SparkPi
        val clazz = Utils.classForName(mainClass)
        val mainMethod = clazz.getMethod("main", classOf[Array[String]])
        mainMethod.invoke(null, extraArgs.toArray[String])

        rpcEnv.shutdown()

      case _ =>
        5. 如果不能匹配到上面的参数，则错误推出
        // scalastyle:off println
        System.err.println("Usage: DriverWrapper <workerUrl> <userJar> <driverMainClass> [options]")
        // scalastyle:on println
        System.exit(-1)
    }
  }
}

```
