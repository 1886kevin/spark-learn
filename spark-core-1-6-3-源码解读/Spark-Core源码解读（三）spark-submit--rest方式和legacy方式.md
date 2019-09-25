对于spark-submit的提交，spark1.3.0之后提出了rest的方式来提交应用，原来的就是legacy方式，不是原来就是legacy，是因为legacy的这个单词就是原来的意思。
 
### spark-submit  rest方式
关于spark-submit提交app，这里就直接从官方的一个小例子谈起（注意下面6066是因为我们在master上启动的那个restserver的端口号就是6066）：
```
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://207.184.161.138:6066 \
  /path/to/examples.jar   
```
它这里会使用bin目录下的spark-submit.sh，下面直接看这个shell脚本：

```
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# disable randomized hash for string in Python 3.3+
export PYTHONHASHSEED=0

1. 这里最重要的是这句代码，它指示要启动 org.apache.spark.deploy.SparkSubmit这个类
exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit "$@"
  
```
然后就直接看这个类中的main方法：
```
 ......
 
 def main(args: Array[String]): Unit = {
    1. 构造参数的类，着重看一下这个类
    val appArgs = new SparkSubmitArguments(args)
    if (appArgs.verbose) {
      // scalastyle:off println
      printStream.println(appArgs)
      // scalastyle:on println
    }
    appArgs.action match {
      2. 根据上面提供的参数，匹配执行相应的操作
      case SparkSubmitAction.SUBMIT => submit(appArgs)
      case SparkSubmitAction.KILL => kill(appArgs)
      case SparkSubmitAction.REQUEST_STATUS => requestStatus(appArgs)
    }
  }
 ...... 
```
之后就直接看一下这个构造提交参数的类，这里简单看下其中比较总要的初始化方法：
```
  .....
  
  // Set parameters from command line arguments
  try {
   1. 将提交脚本中的参数做一个转换，如--class, --master等
    parse(args.asJava)
  } catch {
    case e: IllegalArgumentException =>
      SparkSubmit.printErrorAndExit(e.getMessage())
  }
  // Populate `sparkProperties` map from properties file
 2. 合并--conf （如果有的话）中和spark-default.conf中的参数
  mergeDefaultSparkProperties()
 3. 删掉不是spark系统的配置参数
  // Remove keys that don't start with "spark." from `sparkProperties`.
  ignoreNonSparkProperties()
  // Use `sparkProperties` map along with env vars to fill in any missing parameters
  4. 添加上默认的配置参数
  loadEnvironmentArguments()
  5. 验证配置参数的有效性
  validateArguments()
  .......
// Action should be SUBMIT unless otherwise specifiedaction = Option(action).getOrElse(SUBMIT)
  
  ......   

```
关于怎么实现的我就不具体分析了，大家有兴趣自己看一下源代码（因为这个不属于spark运行的主流程，没有必要过多解释）：

```
protected final void parse(List<String> args) {
    1.下面这些只是做了一个字符串的匹配实现，并对匹配完的参数进行处理，这里会处理三种不同种类的参数
    Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.+)");

    int idx = 0;
    for (idx = 0; idx < args.size(); idx++) {
      String arg = args.get(idx);
      String value = null;

      Matcher m = eqSeparatedOpt.matcher(arg);
      if (m.matches()) {
        arg = m.group(1);
        value = m.group(2);
      }

      // Look for options with a value.
      String name = findCliOption(arg, opts);
      if (name != null) {
        if (value == null) {
          if (idx == args.size() - 1) {
            throw new IllegalArgumentException(
                String.format("Missing argument for option '%s'.", arg));
          }
          idx++;
          value = args.get(idx);
        }
     2. 处理能够匹配到的的参数，如“ --class org.apache.spark.examples.SparkPi \
  --master spark://207.184.161.138:6066 \”
        if (!handle(name, value)) {
          break;
        }
        continue;
      }

      // Look for a switch.
      name = findCliOption(arg, switches);
      if (name != null) {
        if (!handle(name, null)) {
          break;
        }
        continue;
      }
    3. 处理不知道的参数，但是被"--"标记，本例子中不存在这种参数
      if (!handleUnknown(arg)) {
        break;
      }
    }

    if (idx < args.size()) {
      idx++;
    }
   4. 处理不是没有被"--"标记的参数，如最后的“  /path/to/examples.jar   ”
    handleExtraArgs(args.subList(idx, args.size()));
  }
  
```
所以这里整理一下根据上面的处理这里得到的有用的信息：
1. 当SparkSubmitArguments初始化的时候，默认会指定action为SUBMIT，
2. handle方法会指定master和mainClass参数
3. handleExtraArgs会将  /path/to/examples.jar   添加到childArgs中，也就是指定了childArgs这个参数的值。

### action : submit
所以action会匹配到SparkSubmitAction.SUBMIT，从而执行submit方法，而在submit中，两个重要的方法：
```
  .....
     1. 用来构造准备提交app的配置参数
     val (childArgs, childClasspath, sysProps, childMainClass) = prepareSubmitEnvironment(args)
    
  ......   
     2. 启动提交程序的客户端，并提交程序
                runMain(childArgs, childClasspath, sysProps, childMainClass, args.verbose)
  ......   
```
先看prepareSubmitEnvironment构造的一个比较重要的参数就是这个childMainClass，会有不同的client程序
```
         1. standalone cluster 和mesos cluster：
         childMainClass = "org.apache.spark.deploy.rest.RestSubmissionClient"
         2. yarn cluster
         childMainClass = "org.apache.spark.deploy.yarn.Client"
```
而这里我们使用的是standalone模式（其他模式原理都是一样的），它这里指定后面的runmain方法要启动的类，然后看这个runmain方法：

```
     private def runMain(
      childArgs: Seq[String],
      childClasspath: Seq[String],
      sysProps: Map[String, String],
      childMainClass: String,
      verbose: Boolean): Unit = {
    // scalastyle:off println
    1. 首先打印一些参数的基本信息
    if (verbose) {
      printStream.println(s"Main class:\n$childMainClass")
      printStream.println(s"Arguments:\n${childArgs.mkString("\n")}")
      printStream.println(s"System properties:\n${sysProps.mkString("\n")}")
      printStream.println(s"Classpath elements:\n${childClasspath.mkString("\n")}")
      printStream.println("\n")
    }
    // scalastyle:on println
   2. 这里会指定加载类对象的classloader，如果开启spark.driver.userClassPathFirst这个开关的话，loader会先使用user指定的jar包，然后在使用系统默认的jar，这里它的实现就是犹在这个ChildFirstURLClassLoader中生命了一个ParentClassLoader，在使用的时候，先使用ChildFirstURLClassLoader的jar（user），然后在使用ParentClassLoader的jar（sys）。
    val loader =
      if (sysProps.getOrElse("spark.driver.userClassPathFirst", "false").toBoolean) {
        new ChildFirstURLClassLoader(new Array[URL](0),
          Thread.currentThread.getContextClassLoader)
      } else {
        new MutableURLClassLoader(new Array[URL](0),
          Thread.currentThread.getContextClassLoader)
      }
    Thread.currentThread.setContextClassLoader(loader)
    
    3. 将jar包添加到loader的classpath下
    for (jar <- childClasspath) {
      addJarToClasspath(jar, loader)
    }
   4. set配置信息
    for ((key, value) <- sysProps) {
      System.setProperty(key, value)
    }
   5. 后面这一大段代码就是通过反射的方式找到mainClass，然后在获得其main方法。
    var mainClass: Class[_] = null

    try {
      mainClass = Utils.classForName(childMainClass)
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace(printStream)
        if (childMainClass.contains("thriftserver")) {
          // scalastyle:off println
          printStream.println(s"Failed to load main class $childMainClass.")
          printStream.println("You need to build Spark with -Phive and -Phive-thriftserver.")
          // scalastyle:on println
        }
        System.exit(CLASS_NOT_FOUND_EXIT_STATUS)
      case e: NoClassDefFoundError =>
        e.printStackTrace(printStream)
        if (e.getMessage.contains("org/apache/hadoop/hive")) {
          // scalastyle:off println
          printStream.println(s"Failed to load hive class.")
          printStream.println("You need to build Spark with -Phive and -Phive-thriftserver.")
          // scalastyle:on println
        }
        System.exit(CLASS_NOT_FOUND_EXIT_STATUS)
    }

    // SPARK-4170
    if (classOf[scala.App].isAssignableFrom(mainClass)) {
      printWarning("Subclasses of scala.App may not work correctly. Use a main() method instead.")
    }

    val mainMethod = mainClass.getMethod("main", new Array[String](0).getClass)
    if (!Modifier.isStatic(mainMethod.getModifiers)) {
      throw new IllegalStateException("The main method in the given main class must be static")
    }

    def findCause(t: Throwable): Throwable = t match {
      case e: UndeclaredThrowableException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: InvocationTargetException =>
        if (e.getCause() != null) findCause(e.getCause()) else e
      case e: Throwable =>
        e
    }
   6. 就是直接调用这个main方法
    try {
      mainMethod.invoke(null, childArgs.toArray)
    } catch {
      case t: Throwable =>
        findCause(t) match {
          case SparkUserAppException(exitCode) =>
            System.exit(exitCode)

          case t: Throwable =>
            throw t
        }
    }
  }

```
下面就直接看这个main方法，也就是RestSubmissionClient的main方法：

```
     def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      sys.error("Usage: RestSubmissionClient [app resource] [main class] [app args*]")
      sys.exit(1)
    }
    val appResource = args(0)
    val mainClass = args(1)
    val appArgs = args.slice(2, args.size)
    val conf = new SparkConf
    val env = filterSystemEnvironment(sys.env)
    run(appResource, mainClass, appArgs, conf, env)
  }
```
接着直接看这个run方法：
```
   def run(
      appResource: String,
      mainClass: String,
      appArgs: Array[String],
      conf: SparkConf,
      env: Map[String, String] = Map()): SubmitRestProtocolResponse = {
    val master = conf.getOption("spark.master").getOrElse {
      throw new IllegalArgumentException("'spark.master' must be set.")
    }
    val sparkProperties = conf.getAll.toMap
    1. 启动提交app的client程序
    val client = new RestSubmissionClient(master)
    2. 构建提交的request
    val submitRequest = client.constructSubmitRequest(
      appResource, mainClass, appArgs, sparkProperties, env)
    3. 正式提交这个request
    client.createSubmission(submitRequest)
  }
     
```
下面直接看这个createSubmission方法：

```
   def createSubmission(request: CreateSubmissionRequest): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request to launch an application in $master.")
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    for (m <- masters if !handled) {
      1. 验证通过spark-submit脚本提交的master的有效性
      validateMaster(m)
      2. 获取提交app的url，形如"http://207.184.161.138:6066/v1/submissions/create"
      val url = getSubmitUrl(m)
      try {
      3. 正式向server发送这个url，这个方法就是执行的连接，写操作了
        response = postJson(url, request.toJson)
     4. 对响应信息的处理
        response match {
          case s: CreateSubmissionResponse =>
            if (s.success) {
              reportSubmissionStatus(s)
              handleRestResponse(s)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }
```
然后直接看我们的server，这里你会有一个疑问，这是哪里来的server？
其实这个server我们早已经创建好了，就是在new  Master的时候创建的：

```
  
    if (restServerEnabled) {
      val port = conf.getInt("spark.master.rest.port", 6066)
      restServer = Some(new StandaloneRestServer(address.host, port, conf, self, masterUrl))
    }
    restServerBoundPort = restServer.map(_.start())   
         
```
所以在Master实例化的时候，已经创建并启动了这个server。
关于这个server是怎么接收消息的，这个做过java web开发的人应该很熟悉了，我这里就不分析了，如果特想知道是怎么实现的，大家想看一下java web中servelet这个模块的内容，然后再看这里就会明白了

这里简单说一下：
在本例中server使用jetty来是想的，在server启动的过程中，会绑定一个ServletContextHandler，然后会将servelet添加到ServletContextHandler，所以数据会通过servelet来进行处理：
首先StandaloneRestServer是继承自RestSubmissionServer这个类的，先看下里面有一段关键代码：

```
  
     protected lazy val contextToServlet = Map[String, RestServlet](
    s"$baseContext/create/*" -> submitRequestServlet,
    s"$baseContext/kill/*" -> killRequestServlet,
    s"$baseContext/status/*" -> statusRequestServlet,
    "/*" -> new ErrorServlet // default handler
  )
         
```
有这里可以看出，我们的实例会最终匹配到submitRequestServlet，最终实现是StandaloneSubmitRequestServlet这个类。然后根据url，他最终会调用handleSubmit这个方法来处理提交的request

```
  protected override def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    requestMessage match {
      1. 一般都会匹配到submitRequest
      case submitRequest: CreateSubmissionRequest =>
      2. 构造driver启动的参数信息
        val driverDescription = buildDriverDescription(submitRequest)
      3. 这里是最最关键的一步就是它向master发送了RequestSubmitDriver的消息。至此整个app提交的过程就完了
        val response = masterEndpoint.askWithRetry[DeployMessages.SubmitDriverResponse](
          DeployMessages.RequestSubmitDriver(driverDescription))
       4. 后面就是一些响应的信息了
        val submitResponse = new CreateSubmissionResponse
        submitResponse.serverSparkVersion = sparkVersion
        submitResponse.message = response.message
        submitResponse.success = response.success
        submitResponse.submissionId = response.driverId.orNull
        val unknownFields = findUnknownFields(requestMessageJson, requestMessage)
        if (unknownFields.nonEmpty) {
          // If there are fields that the server does not know about, warn the client
          submitResponse.unknownFields = unknownFields
        }
        submitResponse
      case unexpected =>
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError(s"Received message of unexpected type ${unexpected.messageType}.")
    }
  }
           
```
ok，至此整个提交app的过程又走通了，这里在结束前还得看一下这个buildDriverDescription中一段比较重要的代码：

```
 val command = new Command(
      "org.apache.spark.deploy.worker.DriverWrapper",
      Seq("{{WORKER_URL}}", "{{USER_JAR}}", mainClass) ++ appArgs, // args to the DriverWrapper
                 
```
这里先看这个org.apache.spark.deploy.worker.DriverWrapper这个类，其实在之后driver的启动就是通过这个类来实现的，因为经常有人会问到到底driver怎么启动的，现在你应该很明白了吧！ 好了，先到这里，后面我们就该正式开始程序的主流程分析了。


补充说明：关于legacy方式：
当你使用下面这个shell脚本提交程序时，注意下面的端口为7077

```
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master spark://207.184.161.138:7077 \
  /path/to/examples.jar   
```
程序运行会发生异常的，因为并没有端口为7077的restServer。这个时候程序运行就会发生一下的warning提示：

```
Warning: Master endpoint spark://207.184.161.138:7077 was not a REST
server. Falling back to legacy submission gateway instead.

```
下面我把发生这个异常的位置列出来，就在SparkSubmit类下面的submit方法中，
我这里截取重要代码片段：



```
// In standalone cluster mode, there are two submission gateways:
     //   (1) The traditional Akka gateway using o.a.s.deploy.Client as a wrapper
     //   (2) The new REST-based gateway introduced in Spark 1.3
     // The latter is the default behavior as of Spark 1.3, but Spark submit will fail over
     // to use the legacy gateway if the master endpoint turns out to be not a REST server.
    1.先看他注释这里是有说明的关于app的提交有两种方式：
       1.1 通过Client这个类来提交
       1.2 通过rest的方式进行提交，上面博客中有分析的很清楚，就是通过RestSubmissionClient这个类来实现
    if (args.isStandaloneCluster && args.useRest) {
      try {
        // scalastyle:off println
        printStream.println("Running Spark using the REST application submission protocol.")
        // scalastyle:on println
        2. 这里就是通过rest方式来提交app的方法
        doRunMain()
      } catch {
        // Fail over to use the legacy submission gateway
        case e: SubmitRestConnectionException =>
          3. 看这里这个warning信息正好和上面的打印信息是一致的
          printWarning(s"Master endpoint ${args.master} was not a REST server. " +
            "Falling back to legacy submission gateway instead.")
          args.useRest = false
          4.这样的话，他就会启动这个参数中的类
          submit(args)
      }
    // In all other modes, just run the main class as prepared
    } else {
      doRunMain()
    }
 
```
而这个args参数中的client类，是在使用prepareSubmitEnvironment方法的时候构建的，下面看重要代码片段：



```
    if (args.isStandaloneCluster) {
      if (args.useRest) {
        childMainClass = "org.apache.spark.deploy.rest.RestSubmissionClient"
        childArgs += (args.primaryResource, args.mainClass)
      } else {
        // In legacy standalone cluster mode, use Client as a wrapper around the user class
         1. 非rest方式，client使用下面这个类
        childMainClass = "org.apache.spark.deploy.Client"
        if (args.supervise) { childArgs += "--supervise" }
        Option(args.driverMemory).foreach { m => childArgs += ("--memory", m) }
        Option(args.driverCores).foreach { c => childArgs += ("--cores", c) }
        childArgs += "launch"
        childArgs += (args.master, args.primaryResource, args.mainClass)
      }
      if (args.childArgs != null) {
        childArgs ++= args.childArgs
      }
    } 
```
所以在之后doRunMain方法中调用那个类就是org.apache.spark.deploy.Client：
下面简单看一下它的main方法，下面我指出重要部分：

```
    /**
 * Executable utility for starting and terminating drivers inside of a standalone cluster.
 */
object Client {
  def main(args: Array[String]) {
    // scalastyle:off println
    if (!sys.props.contains("SPARK_SUBMIT")) {
      println("WARNING: This client is deprecated and will be removed in a future version of Spark")
      println("Use ./bin/spark-submit with \"--master spark://host:port\"")
    }
    // scalastyle:on println

    val conf = new SparkConf()
    val driverArgs = new ClientArguments(args)

    if (!driverArgs.logLevel.isGreaterOrEqual(Level.WARN)) {
      conf.set("spark.akka.logLifecycleEvents", "true")
    }
    conf.set("spark.rpc.askTimeout", "10")
    conf.set("akka.loglevel", driverArgs.logLevel.toString.replace("WARN", "WARNING"))
    Logger.getRootLogger.setLevel(driverArgs.logLevel)
    1. 建立一个rpcEnv
    val rpcEnv =
      RpcEnv.create("driverClient", Utils.localHostName(), 0, conf, new SecurityManager(conf))
     2. 获得master的endpoint
    val masterEndpoints = driverArgs.masters.map(RpcAddress.fromSparkURL).
      map(rpcEnv.setupEndpointRef(Master.SYSTEM_NAME, _, Master.ENDPOINT_NAME))
    3. 然后这个客户端，也新建一个ClientEndpoint的对象，用来和master通信，从而在构建完应用后，可以直接提交
    rpcEnv.setupEndpoint("client", new ClientEndpoint(rpcEnv, driverArgs, masterEndpoints, conf))

    rpcEnv.awaitTermination()
  }
}

```

讲到这里，大家应该很明白了吧！另外这里多些一下@[木木尚月关](http://www.jianshu.com/users/5155fcec7fde) ，因为这个提交刚开始讲的时候，没有考虑的太多。再次感谢🙏！！！









