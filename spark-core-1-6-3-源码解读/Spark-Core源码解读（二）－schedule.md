在上一篇博客中，就剩下了Master.scala中的schedule这个方法，这个方法很重要，在很多时候都会调用这个方法，这里先画一个简图：


![Screen Shot 2016-12-02 at 9.31.55 PM.png](http://upload-images.jianshu.io/upload_images/3736220-1b797afcf64716e0.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

由上图中可以看出，主要三种方式会最终导致schedule：
1. worker注册之后
2.driver提交之后
3.application注册之后
其实只要是涉及到会有资源变化的操作，最终都会触发调用schedule的操作，这里仅先列举出涉及主流程的三种方式（如想更加详细的了解，需查看Master类是如何调用该方法的，放心，凡是涉及到主流程的之后都会为大家分析）

那么这个方法主要有什么作用呢？它主要的作用就是在集群上，分配计算资源，并启动driver和Executor。

### schedule
这里就废话不多说，直接看这个方法：

```
 private def schedule(): Unit = {
    1. 务必保证Master的状态为ALIVE
    if (state != RecoveryState.ALIVE) {
      return
    }
    // Drivers take strict precedence over executors
    2. 上面这句话的言外之意就是：在启动Executor之前，必须先启动Driver
    3. 用Random.shuffle的方式，打乱并过滤出可用的worker，并避免driver总分配在一个或一小组worker上
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0
    for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
     4. 上面这些话的意思就是，将driver按顺序的在可满足其计算资源的worker上启动，由下面的代码也可以看出，是先启动完Driver，然后再启动Executor的。
      var launched = false
      var numWorkersVisited = 0
      while (numWorkersVisited < numWorkersAlive && !launched) {
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          launchDriver(worker, driver)
          waitingDrivers -= driver
          launched = true
        }
        curPos = (curPos + 1) % numWorkersAlive
      }
    }
    startExecutorsOnWorkers()
  }

```
下面先看这个启动driver的方法launchDriver：
```
   private def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    worker.addDriver(driver)
    driver.worker = Some(worker)
   1. 直接向worker发送启动driver的消息了，关于worker端的操作，这里先不细说，后面肯定会详细分析的
    worker.endpoint.send(LaunchDriver(driver.id, driver.desc))
    driver.state = DriverState.RUNNING
  }
```
然后再看这个启动Executor的方法startExecutorsOnWorkers：
```
   private def startExecutorsOnWorkers(): Unit = {
    // Right now this is a very simple FIFO scheduler. We keep trying to fit in the first app
    // in the queue, then the second app, etc.
    for (app <- waitingApps if app.coresLeft > 0) {
      val coresPerExecutor: Option[Int] = app.desc.coresPerExecutor
      // Filter out workers that don't have enough resources to launch an executor
      1. 对于一个App，获取满足其资源要求的worker列表，并按资源的从大到小顺序排列
      val usableWorkers = workers.toArray.filter(_.state == WorkerState.ALIVE)
        .filter(worker => worker.memoryFree >= app.desc.memoryPerExecutorMB &&
          worker.coresFree >= coresPerExecutor.getOrElse(1))
        .sortBy(_.coresFree).reverse
       2. 分配Executor的过程，马上会详细分析
       val assignedCores = scheduleExecutorsOnWorkers(app, usableWorkers, spreadOutApps)

      // Now that we've decided how many cores to allocate on each worker, let's allocate them
      3. 根据获取的资源，启动Executor
      for (pos <- 0 until usableWorkers.length if assignedCores(pos) > 0) {
        allocateWorkerResourceToExecutors(
          app, assignedCores(pos), coresPerExecutor, usableWorkers(pos))
      }
    }
  } 
```
接下来就直接看这个分配Executor的过程（关于这个方法，写了很多的注释其实它要做的事情很简单，大家务必看完我写的中文注释，英文注释很详细，但是往往废话太多）：

```
  方法的注释，有两层意思：
  1. 分配策略有两种：一种是尽量将Executors分配到一个或少量worker；另一种是为了负载均衡，尽量将Executors分配到最多的worker
  2. 关于第二种情况，他提出了一个问题：如果尽量将Executors分配到最多的worker，那可能会导致一种情况就是有的worker上的计算资源都达不到Executor运行的需要
  解决方案：先过滤出满足Executor运行需要的worker，然后再一次向worker申请计算资源（默认是每次申请一个core）
  好了，接下来的步骤，英文注释已经讲的很清楚了，如果有什么不懂的，直接给我留言：
   /**
   * Schedule executors to be launched on the workers.
   * Returns an array containing number of cores assigned to each worker.
   *
   * There are two modes of launching executors. The first attempts to spread out an application's
   * executors on as many workers as possible, while the second does the opposite (i.e. launch them
   * on as few workers as possible). The former is usually better for data locality purposes and is
   * the default.
   *
   * The number of cores assigned to each executor is configurable. When this is explicitly set,
   * multiple executors from the same application may be launched on the same worker if the worker
   * has enough cores and memory. Otherwise, each executor grabs all the cores available on the
   * worker by default, in which case only one executor may be launched on each worker.
   *
   * It is important to allocate coresPerExecutor on each worker at a time (instead of 1 core
   * at a time). Consider the following example: cluster has 4 workers with 16 cores each.
   * User requests 3 executors (spark.cores.max = 48, spark.executor.cores = 16). If 1 core is
   * allocated at a time, 12 cores from each worker would be assigned to each executor.
   * Since 12 < 16, no executors would launch [SPARK-8881].
   */
  private def scheduleExecutorsOnWorkers(
      app: ApplicationInfo,
      usableWorkers: Array[WorkerInfo],
      spreadOutApps: Boolean): Array[Int] = {
    val coresPerExecutor = app.desc.coresPerExecutor
    val minCoresPerExecutor = coresPerExecutor.getOrElse(1)
    val oneExecutorPerWorker = coresPerExecutor.isEmpty
    val memoryPerExecutor = app.desc.memoryPerExecutorMB
    val numUsable = usableWorkers.length
    val assignedCores = new Array[Int](numUsable) // Number of cores to give to each worker
    val assignedExecutors = new Array[Int](numUsable) // Number of new executors on each worker
    var coresToAssign = math.min(app.coresLeft, usableWorkers.map(_.coresFree).sum)

    /** Return whether the specified worker can launch an executor for this app. */
    def canLaunchExecutor(pos: Int): Boolean = {
      val keepScheduling = coresToAssign >= minCoresPerExecutor
      val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

      // If we allow multiple executors per worker, then we can always launch new executors.
      // Otherwise, if there is already an executor on this worker, just give it more cores.
      val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
      if (launchingNewExecutor) {
        val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
        val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
        val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
        keepScheduling && enoughCores && enoughMemory && underLimit
      } else {
        // We're adding cores to an existing executor, so no need
        // to check memory and executor limits
        keepScheduling && enoughCores
      }
    }

    // Keep launching executors until no more workers can accommodate any
    // more executors, or if we have reached this application's limits
    var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
    while (freeWorkers.nonEmpty) {
      freeWorkers.foreach { pos =>
        var keepScheduling = true
        while (keepScheduling && canLaunchExecutor(pos)) {
          coresToAssign -= minCoresPerExecutor
          assignedCores(pos) += minCoresPerExecutor

          // If we are launching one executor per worker, then every iteration assigns 1 core
          // to the executor. Otherwise, every iteration assigns cores to a new executor.
          if (oneExecutorPerWorker) {
            assignedExecutors(pos) = 1
          } else {
            assignedExecutors(pos) += 1
          }

          // Spreading out an application means spreading out its executors across as
          // many workers as possible. If we are not spreading out, then we should keep
          // scheduling executors on this worker until we use all of its resources.
          // Otherwise, just move on to the next worker.
          if (spreadOutApps) {
            keepScheduling = false
          }
        }
      }
      freeWorkers = freeWorkers.filter(canLaunchExecutor)
    }
    assignedCores
  }
```
然后接下来就是这些Executor的启动方法了：

```
  private def allocateWorkerResourceToExecutors(
      app: ApplicationInfo,
      assignedCores: Int,
      coresPerExecutor: Option[Int],
      worker: WorkerInfo): Unit = {
    // If the number of cores per executor is specified, we divide the cores assigned
    // to this worker evenly among the executors with no remainder.
    // Otherwise, we launch a single executor that grabs all the assignedCores on this worker.
    val numExecutors = coresPerExecutor.map { assignedCores / _ }.getOrElse(1)
    val coresToAssign = coresPerExecutor.getOrElse(assignedCores)
    for (i <- 1 to numExecutors) {
      val exec = app.addExecutor(worker, coresToAssign)
      1. 这里就直接启动这个Executor了
      launchExecutor(worker, exec)
      app.state = ApplicationState.RUNNING
    }
  }
 
```
最后还要看一下这个launchExecutor方法：

```
   private def launchExecutor(worker: WorkerInfo, exec: ExecutorDesc): Unit = {
    logInfo("Launching executor " + exec.fullId + " on worker " + worker.id)
    worker.addExecutor(exec)
    1. 首先向worker发送一个启动Executor的消息
    worker.endpoint.send(LaunchExecutor(masterUrl,
      exec.application.id, exec.id, exec.application.desc, exec.cores, exec.memory))
    2. 然后还要向driver发送一个获得了这个计算资源的消息，当driver获得app需要的资源会启动这个app
    exec.application.driver.send(
      ExecutorAdded(exec.id, worker.id, worker.hostPort, exec.cores, exec.memory))
  }
  
```
这篇博客只是大概的分析一下这个schedule方法，后续还会更加详细的分析整个Application运行的整体流程的。

