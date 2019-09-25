**古之善为士者，微妙玄通，深不可识。夫唯不可识，故强为之容。**

接着，我们要讲一下shuffle read的操作了，这个又要从哪讲起呢？就从stage 1说起吧！从前面的博客中分析，可得知stage 1再进行计算的时候，最后操作的数据是stage 1中第一个RDD中的compute方法中操作的数据，那么这些数据是怎么来的呢？

我们先看stage 1中的第一个RDD是怎么来的：

```
    
    1. 这一行代码，取自程序的业务逻辑代码
    val wordCounts = pairs.reduceByKey(_+_)
    2. 下面是调用这个reduceByKey方法的实现
    def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    reduceByKey(defaultPartitioner(self), func)
  }
  
    def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
  }
  
  3. 最终调用的是combineByKeyWithClassTag方法：
  def combineByKeyWithClassTag[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
    if (keyClass.isArray) {
      if (mapSideCombine) {
        throw new SparkException("Cannot use map-side combining with array keys.")
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw new SparkException("Default partitioner cannot partition array keys.")
      }
    }
    val aggregator = new Aggregator[K, V, C](
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))
    if (self.partitioner == Some(partitioner)) {
      self.mapPartitions(iter => {
        val context = TaskContext.get()
        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
      }, preservesPartitioning = true)
    } else {
    4. 最终执行的是这段代码，也就是新建了一个ShuffleRDD的对象
      new ShuffledRDD[K, V, C](self, partitioner)
        .setSerializer(serializer)
        .setAggregator(aggregator)
        .setMapSideCombine(mapSideCombine)
    }
  }
```
接下来，我们就直接看这个ShuffledRDD中的compute方法：

```
     override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    1. 这里的dep这个指的指的是当前这个ShuffledRDD的dependency
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    2. 根据相应的shuffleHandle可以获得具体的ShuffleReader，这个在获得ShuffleWriter的时候，有详细介绍过，可以回顾一下上篇博客中的知识。
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }
```
下面看这个getReader方法：
```
      override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    1.很显然它这里就是new了一个BlockStoreShuffleReader的对象
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }
```
那么接着看SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context).read()这里的这个read方法，也就是调用的BlockStoreShuffleReader的read方法，再介绍read方法前，先看一下下面这段代码：
```
      val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024)
```
关于这段代码，先解释一下 mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition)这个参数，这个参数返回的结果为Seq[(BlockManagerId, Seq[(BlockId, Long)])]，也就是当前这个reduce task的数据源，来自于哪些节点的哪些block，及相应的block的数据大小。

SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024)指每一次从map端读取的最大的数据量。

接着我们来分析这个ShuffleBlockFetcherIterator的过程，它在初始化的过程中，会调用一个初始化的方法initialize()；

```
  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    1.业务完成的callback方法，这里主要是清理results。
    context.addTaskCompletionListener(_ => cleanup())

    // Split local and remote blocks.
    2. 获取向其它节点获取block的request，另外也会过滤出本地的block
    val remoteRequests = splitLocalRemoteBlocks()
    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    3. 根据上面的request，向其它node获取block，但是block数据量不能超过一个可配置的最大值
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    4. 获取本地的block
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }

```
下面看这几个方法的实现，splitLocalRemoteBlocks：


```
    private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    1. 这一步的操作是，为了增加并行度，可以同时向5个node获取数据
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    val remoteRequests = new ArrayBuffer[FetchRequest]

    // Tracks total number of blocks (including zero sized blocks)
    var totalBlocks = 0
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      if (address.executorId == blockManager.blockManagerId.executorId) {
        // Filter out zero-sized blocks
        1. 过滤出本地的blocks
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        numBlocksToFetch += localBlocks.size
      } else {
        2. 构建request，获取FetchRequest的链表
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          // Skip empty blocks
          if (size > 0) {
            curBlocks += ((blockId, size))
            remoteBlocks += blockId
            numBlocksToFetch += 1
            curRequestSize += size
          } else if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          }
          3. 满足这个条件就构建一个request
          if (curRequestSize >= targetRequestSize) {
            // Add this FetchRequest
            remoteRequests += new FetchRequest(address, curBlocks)
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            logDebug(s"Creating fetch request of $curRequestSize at $address")
            curRequestSize = 0
          }
        }
        // Add in the final request
       4. 将最终剩余的blocks，也构成一个FetchRequest
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks")
    remoteRequests
  }

  
```

然后是fetchUpToMaxBytes这个方法：

```
  private def fetchUpToMaxBytes(): Unit = {
    // Send fetch requests up to maxBytesInFlight
    while (fetchRequests.nonEmpty &&
      (bytesInFlight == 0 || bytesInFlight + fetchRequests.front.size <= maxBytesInFlight)) {
      当满足上面的条件的时候，不停的去发送上面那个方法构建的request
      sendRequest(fetchRequests.dequeue())
    }
  }
  
```
接着看这个sendRequest：

```
  private[this] def sendRequest(req: FetchRequest) {
    logDebug("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))
    bytesInFlight += req.size
    1. 获得要fetch的blocks的信息
    // so we can look up the size of each blockID
    val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    val blockIds = req.blocks.map(_._1.toString)
    val address = req.address
    2. 使用shuffleClient来读取相应node上的数据，下面具体是通过NettyBlockTransferService来实现的
    shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
      new BlockFetchingListener {
        在fetch成功或者失败后，将结果信息保存在results中
        override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
          // Only add the buffer to results queue if the iterator is not zombie,
          // i.e. cleanup() has not been called yet.
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()
            results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf))
            shuffleMetrics.incRemoteBytesRead(buf.size)
            shuffleMetrics.incRemoteBlocksFetched(1)
          }
          logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
        }

        override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
          logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
          results.put(new FailureFetchResult(BlockId(blockId), address, e))
        }
      }
    )
  }
  
```
最后是fetchLocalBlocks方法：

```
  private[this] def fetchLocalBlocks() {
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val blockId = iter.next()
      try {
        val buf = blockManager.getBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)
        buf.retain()
        1.将本地的block信息放入到results中
        results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }
  }
       
```

上面的内容讲完了，接着我们就该看真正将要分析的内容了，也就是那个read方法：

```
 /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    1. 是我们刚刚讲的内容。
    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024)

    // Wrap the streams for compression based on configuration
   2. 把上面得到的数据进行压缩
    val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
      blockManager.wrapForCompression(blockId, inputStream)
    }
    3. 实例化序列化器
    val ser = Serializer.getSerializer(dep.serializer)
    val serializerInstance = ser.newInstance()

    // Create a key/value iterator for each stream
    4. 根据前面的iterator创建一个 key/value iterator 
    val recordIter = wrappedStreams.flatMap { wrappedStream =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    5. 每读取一次数据就会跟新一次ShuffleRead的测量系统
    val readMetrics = context.taskMetrics.createShuffleReadMetricsForDependency()
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map(record => {
        readMetrics.incRecordsRead(1)
        record
      }),
      context.taskMetrics().updateShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
      6.按照我们的程序是会执行这条if语句
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    7. 我们的程序根本没有指定keyOrdering，所以他最终会匹配到None
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = Some(ser))
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.internalMetricsToAccumulators(
          InternalAccumulator.PEAK_EXECUTION_MEMORY).add(sorter.peakMemoryUsedBytes)
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }
  }
        
```
接着我们再看一下这个combineCombinersByKey方法：
```
dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)           
```
即：
```
    def combineCombinersByKey(
      iter: Iterator[_ <: Product2[K, C]],
      context: TaskContext): Iterator[(K, C)] = {
    1. 看到这里你应该很熟悉了吧！在shuffle write的时候，也是使用的这个数据结构
    val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
    2. 把这些数据读到本地节点上
    combiners.insertAll(iter)
    3. 更新相应的测量信息，这个自己看一下
    updateMetrics(context, combiners)
    4. 下面也会详细说一下这个iterator
    combiners.iterator
  }         
```
下面看这个insertAll的方法：

```
def insertAll(entries: Iterator[Product2[K, V]]): Unit = {
    1. 看到这个方法，你是不是觉得很熟悉，他特别像上篇博客中shuffle write的时候，insertAll的操作
     2.也就是在读数据的时候，也有可能对数据进行spill
    if (currentMap == null) {
      throw new IllegalStateException(
        "Cannot insert new elements into a map after calling iterator")
    }
    // An update function for the map that we reuse across entries to avoid allocating
    // a new closure each time
    var curEntry: Product2[K, V] = null
    val update: (Boolean, C) => C = (hadVal, oldVal) => {
      if (hadVal) mergeValue(oldVal, curEntry._2) else createCombiner(curEntry._2)
    }
    3. 这里非常重要的一点是这个next，这个next最终会在哪里停止呢？
    while (entries.hasNext) {
      curEntry = entries.next()
      val estimatedSize = currentMap.estimateSize()
      if (estimatedSize > _peakMemoryUsedBytes) {
        _peakMemoryUsedBytes = estimatedSize
      }
      if (maybeSpill(currentMap, estimatedSize)) {
        currentMap = new SizeTrackingAppendOnlyMap[K, C]
      }
      currentMap.changeValue(curEntry._1, update)
      addElementsRead()
    }
  }          
```
带着上面那个疑问，next的调用会最终调用到上面刚刚讲过的那个类的next方法：
```
override def next(): (BlockId, InputStream) = {
    numBlocksProcessed += 1
    val startFetchWait = System.currentTimeMillis()
    currentResult = results.take()
    val result = currentResult
    val stopFetchWait = System.currentTimeMillis()
    shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

    result match {
      case SuccessFetchResult(_, _, size, _) => bytesInFlight -= size
      case _ =>
    }
    // Send fetch requests up to maxBytesInFlight
   1. 看到这里你就应该能明白了，它在不断的去抓取数据，也就是说只有抓取到该数据的所有信息他才会继续往下运行。
    fetchUpToMaxBytes()

    result match {
      case FailureFetchResult(blockId, address, e) =>
        throwFetchFailedException(blockId, address, e)

      case SuccessFetchResult(blockId, address, _, buf) =>
        try {
          (result.blockId, new BufferReleasingInputStream(buf.createInputStream(), this))
        } catch {
          case NonFatal(t) =>
            throwFetchFailedException(blockId, address, t)
        }
    }
  }
```
那么他怎么来操作最终的数据的呢，这就要回到刚刚要解释的那句代码了
即    combiners.iterator
```
 override def iterator: Iterator[(K, C)] = {
    if (currentMap == null) {
      throw new IllegalStateException(
        "ExternalAppendOnlyMap.iterator is destructive and should only be called once.")
    }
    if (spilledMaps.isEmpty) {
      CompletionIterator[(K, C), Iterator[(K, C)]](currentMap.iterator, freeCurrentMap())
    } else {
      1. 选取这一种情况，也就是currentMap中不为null，且读数据的时候产生了spill的文件
      new ExternalIterator()
    }
  }
```
下面就看这个ExternalIterator实例化的时候，做了一些什么：

```
    
    // A queue that maintains a buffer for each stream we are currently merging
    // This queue maintains the invariant that it only contains non-empty buffers
    private val mergeHeap = new mutable.PriorityQueue[StreamBuffer]
    
    // Input streams are derived both from the in-memory map and spilled maps on disk
    // The in-memory map is sorted in place, while the spilled maps are already in sorted order
    1. 打乱map中的键值对，使其按照key的hashcode排序
    private val sortedMap = CompletionIterator[(K, C), Iterator[(K, C)]](
      currentMap.destructiveSortedIterator(keyComparator), freeCurrentMap())
    2. 将读取map中数据和读取每个spillfile中的数据的iterator组合在一球
     private val inputStreams = (Seq(sortedMap) ++ spilledMaps).map(it => it.buffered)
    3. 将数据全部读取出来，放到mergeHeap中
    inputStreams.foreach { it =>
      val kcPairs = new ArrayBuffer[(K, C)]
      readNextHashCode(it, kcPairs)
      if (kcPairs.length > 0) {
        mergeHeap.enqueue(new StreamBuffer(it, kcPairs))
      }
    }
    
 ```
下面在看这个类ExternalIterator里面的next方法：

```
    
    override def next(): (K, C) = {
      if (mergeHeap.length == 0) {
        throw new NoSuchElementException
      }
      // Select a key from the StreamBuffer that holds the lowest key hash
      由于PriorityQueue的特点，直接提取的一个key最小的buffer
      val minBuffer = mergeHeap.dequeue()
      val minPairs = minBuffer.pairs
      val minHash = minBuffer.minKeyHash
      val minPair = removeFromBuffer(minPairs, 0)
      val minKey = minPair._1
      var minCombiner = minPair._2
      assert(hashKey(minPair) == minHash)

      // For all other streams that may have this key (i.e. have the same minimum key hash),
      // merge in the corresponding value (if any) from that stream
      2. 根据上面提取的key值，依次从各个buffer中找到相同的key之下的value值进行combine
      val mergedBuffers = ArrayBuffer[StreamBuffer](minBuffer)
      while (mergeHeap.length > 0 && mergeHeap.head.minKeyHash == minHash) {
        val newBuffer = mergeHeap.dequeue()
        minCombiner = mergeIfKeyExists(minKey, minCombiner, newBuffer)
       3.  最终得到一个没有上一个key值的buffer列表
        mergedBuffers += newBuffer
      }

      // Repopulate each visited stream buffer and add it back to the queue if it is non-empty
      4. 最终把这个没有上一个key值的buffer列表，从新添加到mergeHeap中
      mergedBuffers.foreach { buffer =>
        if (buffer.isEmpty) {
          readNextHashCode(buffer.iterator, buffer.pairs)
        }
        if (!buffer.isEmpty) {
          mergeHeap.enqueue(buffer)
        }
      }
     5. 返回最终的key，及其合成值
      (minKey, minCombiner)
    }    
 ```
到这里其实就应该很清楚了，最终的计算就是每一次针对一个（key，value）来计算。所以，到这里就应该可以结束了，下面我再把这个逻辑图给大家画出来：

![Screen Shot 2016-12-13 at 5.24.55 PM.png](http://upload-images.jianshu.io/upload_images/3736220-354667b2fc560b04.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

写到这里整个spark core的核心部分就写完了！这里还有两个重要的核心组件没有分析，一个是RPC的具体实现，另一个是的Block管理。这两个有时间，我再给大家分析，如果你一直跟着我的博客学spark的话，我认为你自己应该能看得懂了。 接下来，我会继续走读spark sql的源码！
