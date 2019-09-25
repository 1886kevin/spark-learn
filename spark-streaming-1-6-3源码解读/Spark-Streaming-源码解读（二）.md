## Spark Streaming程序的启动

#### 上文中谈到了调用SparkStreamingContext中的start方法。

下面直接看代码实现（下面的代码就是SparkStreamingContext的start方法，我会一步步的拆分来分析）：

```
/**
   * Start the execution of the streams.
   *
   * @throws IllegalStateException if the StreamingContext is already stopped.
   */
  def start(): Unit = synchronized {
    state match {
      1.如果是第一次运行start方法的话，就会进入这个状态，因为在SparkStreamingContext实例化的时候已经明确赋值了。
      case INITIALIZED =>
      2.给startSite变量赋值，startSite中的数据类型是CallSite，关于CallSite的介绍请看第一篇博客。
        startSite.set(DStream.getCreationSite())
```
job的构建要保证互斥！
```
      3.异步的完成job的构建
        StreamingContext.ACTIVATION_LOCK.synchronized {
          StreamingContext.assertNoOtherContextIsActive()
          try {
      4.在job具体运行需要对job的运行条件最一次验证检查
            validate()
```
下面直接看它验证方法里面的内容：

```
 private def validate() {
    4.1 验证中需要确保gragh不能为空，gragh中的OutputStreams不能为null，batchDuration不能为null。
    assert(graph != null, "Graph is null")
    graph.validate()
   4.2 验证中需要确保如果设置了checkpoint目录的值也需要确保checkpointDuration不能为null。
    require(
      !isCheckpointingEnabled || checkpointDuration != null,
      "Checkpoint directory has been set, but the graph checkpointing interval has " +
        "not been set. Please use StreamingContext.checkpoint() to set the interval."
    )

    // Verify whether the DStream checkpoint is serializable
   4.3 从起点开始checkpoint，从这里也可以看到checkpoint 保存的数据为（DStream，time）。
    if (isCheckpointingEnabled) {
      val checkpoint = new Checkpoint(this, Time.apply(0))
      try {
        Checkpoint.serialize(checkpoint, conf)
      } catch {
        case e: NotSerializableException =>
          throw new NotSerializableException(
            "DStream checkpointing has been enabled but the DStreams with their functions " +
              "are not serializable\n" +
              SerializationDebugger.improveException(checkpoint, e).getMessage()
          )
      }
    }
   4.4 检查是否打开了动态分配资源的开关，这个它主要是验证两个配置值。
    spark.dynamicAllocation.enabled  是否打开了动态分配资源的开关
    spark.executor.instances  具体指定需要的资源数的配置参数，这里需要确保它为0，若不为0的话，即便打开了前面的开关也没用。
    if (Utils.isDynamicAllocationEnabled(sc.conf)) {
      logWarning("Dynamic Allocation is enabled for this application. " +
        "Enabling Dynamic allocation for Spark Streaming applications can cause data loss if " +
        "Write Ahead Log is not enabled for non-replayable sources like Flume. " +
        "See the programming guide for details on how to enable the Write Ahead Log")
    }
  }
```
#### 下面才是程序启动的核心代码


 这一句才是程序启动核心代码（他这里就是新启动了一个线程来运行大括弧里面的代码，不太懂scala语言的人对此会有一些疑惑，他到底是怎么执行的，请下面的说明）：
```
            ThreadUtils.runInNewThread("streaming-start") {
              sparkContext.setCallSite(startSite.get)
              sparkContext.clearJobGroup()
              sparkContext.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")
              scheduler.start()
            }
```
先看下这段ThreadUtils中的这个方法（只截取了这个方法的上半部分，后面的部分是处理异常的，有兴趣的自己去看一下）：
```
  def runInNewThread[T](
      threadName: String,
      isDaemon: Boolean = true)(body: => T): T = {
    @volatile var exception: Option[Throwable] = None
    @volatile var result: T = null.asInstanceOf[T]

    val thread = new Thread(threadName) {
      override def run(): Unit = {
        try {
          result = body
        } catch {
          case NonFatal(e) =>
            exception = Some(e)
        }
      }
    }
    thread.setDaemon(isDaemon)
    thread.start()
    thread.join()
```
这个方法采用了Scala语言中的柯里化，然后我们的程序中的大括号中的内容就是所谓的body。也就是下面这几句：

```
            {
     5.1 将Streaming中的callsite添加到Spark core中
              sparkContext.setCallSite(startSite.get)
     5.2 重新置空相应的job参数
              sparkContext.clearJobGroup()
              sparkContext.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")
     5.3 真正的启动job的构建
              scheduler.start()
            }
```
####  收尾工作
```
     6.1 set状态信息为ACTIVE
            state = StreamingContextState.ACTIVE
          } catch {
            case NonFatal(e) =>
              logError("Error starting the context, marking it as stopped", e)
              scheduler.stop(false)
              state = StreamingContextState.STOPPED
              throw e
          }
          StreamingContext.setActiveContext(this)
        }
    6.2 这个就是用来当我们的程序正常或异常结束时，执行addShutdownHook添加的hooktask，这里hooktask就是stopOnShutdown。
        shutdownHookRef = ShutdownHookManager.addShutdownHook(
          StreamingContext.SHUTDOWN_HOOK_PRIORITY)(stopOnShutdown)
        // Registering Streaming Metrics at the start of the StreamingContext
        assert(env.metricsSystem != null)
     6.3 将streaming源注册给metricsSystem，这个上篇博客中有介绍
        env.metricsSystem.registerSource(streamingSource)
     6.4 这个就是将这个程序的信息添加到streaming 的ui界面上
        uiTab.foreach(_.attach())
        logInfo("StreamingContext started")
      case ACTIVE =>
        logWarning("StreamingContext has already been started")
      case STOPPED =>
        throw new IllegalStateException("StreamingContext has already been stopped")
    }
  }
```
####  结尾
ok，今天到这里就结束了，其实还是没有真正的启动job的构建，只是分析了job构建前准备工作，下一篇博客就是真正的构建了，也就是scheduler.start()方法的分析了。

