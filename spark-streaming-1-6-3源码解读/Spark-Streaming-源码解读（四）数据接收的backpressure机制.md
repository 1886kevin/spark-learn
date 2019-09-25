上文中只是详细讲解了怎么讲接收到的buffer放到blockManage中，那SocketReceiver是怎么接收数据的呢？

这里有两种方式：
1. 默认方式
2. backpressure方式

那我们还是从receiver的启动开始讲起，
上一篇博客中有提到receiver的启动，也就是SocketReceiver中的onStart方法：
```
   def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      setDaemon(true)
      override def run() { receive() }
    }.start()
  }
```
这里会启动一个receive方法，下面看这个receive：


```
def receive() {
    var socket: Socket = null
    try {
      logInfo("Connecting to " + host + ":" + port)
      socket = new Socket(host, port)
      logInfo("Connected to " + host + ":" + port)
      val iterator = bytesToObjects(socket.getInputStream())
      while(!isStopped && iterator.hasNext) {
        store(iterator.next)
      }
      if (!isStopped()) {
        restart("Socket data stream had no more data")
      } else {
        logInfo("Stopped receiving")
      }
    } catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case NonFatal(e) =>
        logWarning("Error receiving data", e)
        restart("Error receiving data", e)
    } finally {
      if (socket != null) {
        socket.close()
        logInfo("Closed socket to " + host + ":" + port)
      }
    }
  }
```
这个方法实现的就是不断的接收通过socket发送过来的数据（这里实质上是由Netcat发送过来的数据）

这里主要看一下处理数据的两个重要的方法：
```
      val iterator = bytesToObjects(socket.getInputStream())
      while(!isStopped && iterator.hasNext) {
        store(iterator.next)
      }
```
先看bytesToObjects这个方法，是从代码的开始处传进来的，在这里：
```
 def socketTextStream(
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[String] = withNamedScope("socket text stream") {
    socketStream[String](hostname, port, SocketReceiver.bytesToLines, storageLevel)
  }
```
传进来的这个方法就是SocketReceiver.bytesToLines，那我们先看一下它的具体实现：

它其实就是SocketReceiver的伴生对象里实现的方法：
```
private[streaming]
object SocketReceiver  {

  /**
   * This methods translates the data from an inputstream (say, from a socket)
   * to '\n' delimited strings and returns an iterator to access the strings.
   */
  如同注释上所说，这个方法就是用来不停的处理接收数据的输入流
  def bytesToLines(inputStream: InputStream): Iterator[String] = {
    val dataInputStream = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))
    new NextIterator[String] {
      protected override def getNext() = {
        val nextValue = dataInputStream.readLine()
        if (nextValue == null) {
          finished = true
        }
        nextValue
      }

      protected override def close() {
        dataInputStream.close()
      }
    }
  }
```
上面的方法让receiver可以接收到数据，那数据该怎么处理呢？ 它这里就是使用了store这个方法：
```
      while(!isStopped && iterator.hasNext) {
        store(iterator.next)
      }
```
通过这段代码，我们可以看到store这个代码放在while循环里，所以它是不断的存数据的，真的是这样吗？ 我们继续看下去：
```
  def store(dataItem: T) {
    supervisor.pushSingle(dataItem)
  }
```
其中上述这个supervisor指的是我们上篇博客中在启动receiver的函数中初始化的。

```
  val supervisor = new ReceiverSupervisorImpl(
              receiver, SparkEnv.get, serializableHadoopConf.value, checkpointDirOption)
```
接下来就看ReceiverSupervisorImpl里的这个pushSingle方法：
```
  /** Push a single record of received data into block generator. */
  def pushSingle(data: Any) {
    defaultBlockGenerator.addData(data)
  }

```
而这个defaultBlockGenerator我们也在上篇博客中有提到，接下来看它的addData方法：

```
  def addData(data: Any): Unit = {
    if (state == Active) {
      这个方法是起到调节接收数据的作用的
      waitToPush()
      synchronized {
        if (state == Active) {
          currentBuffer += data
        } else {
          throw new SparkException(
            "Cannot add data as BlockGenerator has not been started or has been stopped")
        }
      }
    } else {
      throw new SparkException(
        "Cannot add data as BlockGenerator has not been started or has been stopped")
    }
  }
```
这里的这个currentBuffer就是上篇博客中，需要生成block中的buffer数据。
### 获得接收数据的允许（门牌）
下面详细看这个waitToPush方法：

```
    def waitToPush() {
    rateLimiter.acquire()
  }
```
这里需要说明一点，首先这个方法在BlockGenerator中，
```
private[streaming] class BlockGenerator(
    listener: BlockGeneratorListener,
    receiverId: Int,
    conf: SparkConf,
    clock: Clock = new SystemClock()
  ) extends RateLimiter(conf) with Logging {

```
而这个BlockGenerator又是继承的RateLimiter这个类
```
private[receiver] abstract class RateLimiter(conf: SparkConf) extends Logging {

  // treated as an upper limit
  private val maxRateLimit = conf.getLong("spark.streaming.receiver.maxRate", Long.MaxValue)
  private lazy val rateLimiter = GuavaRateLimiter.create(maxRateLimit.toDouble)

  def waitToPush() {
    rateLimiter.acquire()
  }
```
这里需要注意，非常重磅性的关键的点，也就是这个maxRateLimit，它指的就是一秒钟可以接收到多少次数据，默认情况下，它是不变的，而backpressure机制就是通过改变这个值来控制接收速率的。

然后再看这个rateLimiter，继续跟踪其实现

```
  public static RateLimiter create(double permitsPerSecond) {
    return create(SleepingTicker.SYSTEM_TICKER, permitsPerSecond);
  }

  @VisibleForTesting
  static RateLimiter create(SleepingTicker ticker, double permitsPerSecond) {
    RateLimiter rateLimiter = new Bursty(ticker);
    rateLimiter.setRate(permitsPerSecond);
    return rateLimiter;
  }

```
它这里rateLimiter是Bursty的方式啊！！！！！！！Bursty只是实现rateLimiter的一种方式而已，spark默认采用这种方式。

记住了这两点，继续看我们waitToPush这个方法，现在你应该应该rateLimiter是个什么东西了
```
    def waitToPush() {
    rateLimiter.acquire()
  }
```
其实你可以认为acquire方法就是在询问当前可否接收数据。下面来看这个方法：

```
   public void acquire() {
    acquire(1);
  }
  
  public void acquire(int permits) {
    1. 这里的检查是检察permits是否为正数，不为正数直接抛出异常
    checkPermits(permits);
    long microsToWait;
    synchronized (mutex) {
    2. 申请是否被允许放入数据，这里你可以简单的理解成去申请门牌的方式
      microsToWait = reserveNextTicket(permits, readSafeMicros());
    }
   3. 如果不允许放入数据，线程需要sleep  microsToWait这么长的时间
    ticker.sleepMicrosUninterruptibly(microsToWait);
  }

```
这里你肯定十分好奇它是怎么获得允许的，下面我们就一步步看它的执行。
首先readSafeMicros这个方法：
```
   private long readSafeMicros() {
    return TimeUnit.NANOSECONDS.toMicros(ticker.read() - offsetNanos);
  }
```
而这里offsetNanos是在这里指明的，因为Bursty是继承自RateLimiter，而Bursty在创建的时候是调用的这个构造函数。
```
  private RateLimiter(SleepingTicker ticker) {
    this.ticker = ticker;
    this.offsetNanos = ticker.read();
  }
```
而这个ticker.read()返回的就是当前的时间：
```
   private static final Ticker SYSTEM_TICKER = new Ticker() {
    @Override
    public long read() {
      return Platform.systemNanoTime();
    }
  };
  
  Platform中的systemNanoTime方法
    /** Calls {@link System#nanoTime()}. */
  static long systemNanoTime() {
    return System.nanoTime();
  }
  ```
所有现在聪明的你应该明白了，readSafeMicros获取的就是（当前时间减去开始时间的时间间隔）

然后接着reserveNextTicket(permits, readSafeMicros())看这个方法：


```
private long reserveNextTicket(double requiredPermits, long nowMicros) {
    1. 这里这个方法就是将当前时间同步到当前时刻，当然这个后面还有一些别的操作
    resync(nowMicros);
    2. 当前时间到下一次允许（获得门牌）的时间间隔
    long microsToNextFreeTicket = nextFreeTicketMicros - nowMicros;
    3. 获得将要分配的允许（门牌）
    double storedPermitsToSpend = Math.min(requiredPermits, this.storedPermits);
    4. 需要的允许（门牌） －   将要分配给的允许（门牌）
    double freshPermits = requiredPermits - storedPermitsToSpend;
    5. storedPermitsToWaitTime方法在Bursty中的实现总是返回0。
    6. 这个变量就是指的它这里给予了这么多的门牌需要多少时间，也就是程序使用完这些门牌的时间
    long waitMicros = storedPermitsToWaitTime(this.storedPermits, storedPermitsToSpend)
        + (long) (freshPermits * stableIntervalMicros);
    7. 下一次获取门牌的时间
    this.nextFreeTicketMicros = nextFreeTicketMicros + waitMicros;
    8. 减去已分配的门牌，刷新当前可分配的允许（门牌）
    this.storedPermits -= storedPermitsToSpend;
    return microsToNextFreeTicket;
  }
```
讲到这里你应该明白它是怎样来获得允许的了，另外这里还有再说明一下这个resync方法：


```
  private void resync(long nowMicros) {
    // if nextFreeTicket is in the past, resync to now
    
    if (nowMicros > nextFreeTicketMicros) {
      如果当前时间大于下一次分配给允许（门牌）的时间，当然这里也要更新可分配的允许（门牌），因为这段时间间隔也会产生新的允许（门牌）最后更新nextFreeTicketMicros为当前时间
      storedPermits = Math.min(maxPermits,
          storedPermits + (nowMicros - nextFreeTicketMicros) / stableIntervalMicros);
      nextFreeTicketMicros = nowMicros;
    }
  }

```
到这里你应该很明白它是怎么获取允许（门牌）接收数据的了，
那么它怎么来调节数据的接收的呢？默认情况下它不调节，但是当开启backpressure机制的时候，它就会进行调解了，下面我们就讲解它是如何调节的。

### 接收数据的调节－backpressure
这个该从何谈起呢？ 我们的job生成还没有讲呢。这里就先从job的完成开始吧！因为每一次调节都是上一个job完成后才开始的。

先看JobScheduler启动创建的这个loop：
```
   eventLoop = new EventLoop[JobSchedulerEvent]("JobScheduler") {
      override protected def onReceive(event: JobSchedulerEvent): Unit = processEvent(event)

      override protected def onError(e: Throwable): Unit = reportError("Error in job scheduler", e)
    }
    eventLoop.start()
```
然后当spark streaming程序中的job完成时，会发送一个JobCompleted消息
```
    _eventLoop = eventLoop
          if (_eventLoop != null) {
            _eventLoop.post(JobCompleted(job, clock.getTimeMillis()))
          }

```
接收到这个消息后会调用processEvent方法处理这个消息


```
 private def processEvent(event: JobSchedulerEvent) {
    try {
      event match {
        case JobStarted(job, startTime) => handleJobStart(job, startTime)
        case JobCompleted(job, completedTime) => handleJobCompletion(job, completedTime)
        case ErrorReported(m, e) => handleError(m, e)
      }
    } catch {
      case e: Throwable =>
        reportError("Error in job scheduler", e)
    }
  }

```
然后模式匹配到JobCompleted，调用handleJobCompletion方法


```
 private def handleJobCompletion(job: Job, completedTime: Long) {
    val jobSet = jobSets.get(job.time)
    jobSet.handleJobCompletion(job)
    job.setEndTime(completedTime)
    listenerBus.post(StreamingListenerOutputOperationCompleted(job.toOutputOperationInfo))
    logInfo("Finished job " + job.id + " from job set of time " + jobSet.time)
    if (jobSet.hasCompleted) {
      jobSets.remove(jobSet.time)
      jobGenerator.onBatchCompletion(jobSet.time)
      logInfo("Total delay: %.3f s for time %s (execution: %.3f s)".format(
        jobSet.totalDelay / 1000.0, jobSet.time.toString,
        jobSet.processingDelay / 1000.0
      ))
      listenerBus.post(StreamingListenerBatchCompleted(jobSet.toBatchInfo))
    }
    job.result match {
      case Failure(e) =>
        reportError("Error running job " + job, e)
      case _ =>
    }
  }
```
然后这里listenerBus.post(StreamingListenerBatchCompleted(jobSet.toBatchInfo)会给总线发送一个StreamingListenerBatchCompleted的消息。
再然后会将这个消息流入RateController中调用onBatchCompleted这个方法：


```
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    val elements = batchCompleted.batchInfo.streamIdToInputInfo

    for {
      processingEnd <- batchCompleted.batchInfo.processingEndTime
      workDelay <- batchCompleted.batchInfo.processingDelay
      waitDelay <- batchCompleted.batchInfo.schedulingDelay
      elems <- elements.get(streamUID).map(_.numRecords)
    } computeAndPublish(processingEnd, elems, workDelay, waitDelay)
  }
```
到这里你会不会很激动，它这里会统计上一个batch中的信息加以处理。然后再跟踪computeAndPublish这个方法：
```
 private def computeAndPublish(time: Long, elems: Long, workDelay: Long, waitDelay: Long): Unit =
    Future[Unit] {
      val newRate = rateEstimator.compute(time, elems, workDelay, waitDelay)
      newRate.foreach { s =>
        rateLimit.set(s.toLong)
        publish(getLatestRate())
      }
    }

  def getLatestRate(): Long = rateLimit.get()
 
```
这里它就计算出了新的newRate，（关于这个计算方法我就不介绍了，很简单，大家看一下就可以了）然后将这个新得到的newRate发布出去。

```
  private[streaming] class ReceiverRateController(id: Int, estimator: RateEstimator)
      extends RateController(id, estimator) {
    override def publish(rate: Long): Unit =
      ssc.scheduler.receiverTracker.sendRateUpdate(id, rate)
  }
```
这里它会通过receiverTracker发布这个更新，

```
 /** Update a receiver's maximum ingestion rate */
  def sendRateUpdate(streamUID: Int, newRate: Long): Unit = synchronized {
    if (isTrackerStarted) {
      endpoint.send(UpdateReceiverRateLimit(streamUID, newRate))
    }
  } 
```
这里的这个endpoint就是receiverTracker启动的时候赋值的，如下：
```
endpoint = ssc.env.rpcEnv.setupEndpoint(
        "ReceiverTracker", new ReceiverTrackerEndpoint(ssc.env.rpcEnv))
```

也就是向ReceiverTrackerEndpoint发这个UpdateReceiverRateLimit消息

```
override def receive: PartialFunction[Any, Unit] = {
      
      ......
      
      case UpdateReceiverRateLimit(streamUID, newRate) =>
        for (info <- receiverTrackingInfos.get(streamUID); eP <- info.endpoint) {
          eP.send(UpdateRateLimit(newRate))
        }
        
      ......
      
```
看它的处理，看到这你可能有点郁闷了，这个eP又是什么啊？其实它就是ReceiverSupervisorImpl初始化的时候创建的这个endpoint

```
/** RpcEndpointRef for receiving messages from the ReceiverTracker in the driver */
  private val endpoint = env.rpcEnv.setupEndpoint(
    "Receiver-" + streamId + "-" + System.currentTimeMillis(), new ThreadSafeRpcEndpoint {
      override val rpcEnv: RpcEnv = env.rpcEnv

      override def receive: PartialFunction[Any, Unit] = {
        case StopReceiver =>
          logInfo("Received stop signal")
          ReceiverSupervisorImpl.this.stop("Stopped by driver", None)
        case CleanupOldBlocks(threshTime) =>
          logDebug("Received delete old batch signal")
          cleanupOldBlocks(threshTime)
        case UpdateRateLimit(eps) =>
          logInfo(s"Received a new rate limit: $eps.")
          registeredBlockGenerators.foreach { bg =>
            bg.updateRate(eps)
          }
      }
    })      
```
所以这里它会直接处理UpdateRateLimit这个消息，它这里的处理就是去BlockGenerator中更新rate了。当然到这里还要继续走下去，因为现在这里的rate还和前面的acquire的时候处理的变量没什么联系。好，我们就接着往下走：

```
private[receiver] def updateRate(newRate: Long): Unit =
    if (newRate > 0) {
      if (maxRateLimit > 0) {
        rateLimiter.setRate(newRate.min(maxRateLimit))
      } else {
        rateLimiter.setRate(newRate)
      }
    }
```
看到这里是不是有点小激动了，哈哈，终于看到rateLimiter了，然后我们在往下走


```
  public final void setRate(double permitsPerSecond) {
    Preconditions.checkArgument(permitsPerSecond > 0.0
        && !Double.isNaN(permitsPerSecond), "rate must be positive");
    synchronized (mutex) {
      resync(readSafeMicros());
      double stableIntervalMicros = TimeUnit.SECONDS.toMicros(1L) / permitsPerSecond;
      this.stableIntervalMicros = stableIntervalMicros;
      doSetRate(permitsPerSecond, stableIntervalMicros);
    }
  }   
```
看到上面这些方法应该很熟悉了吧，比如resync，readSafeMicros还有这个变量stableIntervalMicros。那么接着往下看吧！看这个doSetRate方法吧！

```
    abstract void doSetRate(double permitsPerSecond, double stableIntervalMicros);
```
他是个虚函数，还记得上文中有提到的，这里的RateLimiter的实现类是Bursty。然后直接看Bursty这个类里的方法吧！


```
@Override
    void doSetRate(double permitsPerSecond, double stableIntervalMicros) {
      double oldMaxPermits = this.maxPermits;
      /*
       * We allow the equivalent work of up to one second to be granted with zero waiting, if the
       * rate limiter has been unused for as much. This is to avoid potentially producing tiny
       * wait interval between subsequent requests for sufficiently large rates, which would
       * unnecessarily overconstrain the thread scheduler.
       */
      maxPermits = permitsPerSecond; // one second worth of permits
      storedPermits = (oldMaxPermits == 0.0)
          ? 0.0 // initial state
          : storedPermits * maxPermits / oldMaxPermits;
    }
```
好了，到这里你应该看明白了吧！终于出现了我们希望出现的storedPermits，storedPermits而这就是上文中acquire方法实现的时候需要的变量。好了，到这里就终于完成了这个数据接收的backpressure机制分析。

### 最后 这里有两个配置参数

```
spark.streaming.backpressure.enabled     是否开启backpressure

spark.streaming.backpressure.initialRate	初始的rate值（也就是一秒钟可以获取多少允许（门牌））
```

今天又没有分析job的生成，明天肯定分析了因为就剩这部分了，对了，除了这部分，还有WAL容错机制。之后都会分析的。
