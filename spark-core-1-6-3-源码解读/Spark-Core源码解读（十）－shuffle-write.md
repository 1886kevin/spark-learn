**一阴一阳之谓道，继之者善也，成之者性也。**

今天这篇博客，写一下我们的业务代码，具体是怎么执行的，因为前面那个SparkPi的业务是没有shuffle的，正常生产环境下，Spark程序是不可能没有shuffle操作的，所以这里我自己写了一个小程序：
```
object SimpleWordCount {

  def main(args: Array[String]) {
    //Driver
    val conf = new SparkConf()
    conf.setAppName("Simple Work Count")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    //Excutor 中具体执行的逻辑业务
    //类型推导
    val lines = sc.textFile("/Users/chenkevin/spark-1.6.0-bin-hadoop2.6/spark",1)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word=>(word,1))
    val wordCounts = pairs.reduceByKey(_+_)
        wordCounts.collect().foreach(wordCounts => println(wordCounts._1 + ":" + wordCounts._2))
    sc.stop()
  }
}
```
上面的/Users/chenkevin/spark-1.6.0-bin-hadoop2.6/spark文件中的内容是：
```
hello spark
hello hadoop
hello flink
spark is awesome

```
上面这个程序就是统计文件中的单词的个数。先划分一下stage，代码中有一个reduceByKey算子，他计算生成的RDD对前面的RDD是宽依赖的，所有整个业务逻辑是由两部分组成的：
```
    stage 0 :
    val lines = sc.textFile("/Users/chenkevin/spark-1.6.0-bin-hadoop2.6/spark",1)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word=>(word,1))

    stage 1 :
    val wordCounts = pairs.reduceByKey(_+_)
        wordCounts.collect()
```

根据这两个生成的RDD依次是：

```
    stage 0 :
    HadoopRDD ->MapPartitionsRDD->MapPartitionsRDD-> 
    stage 1 :
    ShuffledRDD->runJob
 
```
下面有一副简图，可以对应一下：
![Screen Shot 2016-12-09 at 6.10.36 PM.png](http://upload-images.jianshu.io/upload_images/3736220-ee52f353eff291dc.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

下面就直接分析stage 0，根据前面博客的分析，我们知道stage 0运行的是一个ShuffleMapTask的任务，我们就直接看它在做些什么：

```
override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()
    1.解压有driver端发送过来的RDD 及 这个RDD之后的那个RDD的依赖信息，关于这点，你可以回过头来看一下提交stage的时候的代码
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    metrics = Some(context.taskMetrics)
    2. shuffleWriter用于写计算后的数据
    var writer: ShuffleWriter[Any, Any] = null
    try {
    3. 采用哪一种shuffle机制，目前有Hash，Sorted，Tungsten三种，默认采用的Sorted的方式。这个是在创建SparkEnv的时候指定的
      val manager = SparkEnv.get.shuffleManager
    4. 获取相应的writer
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
     5. 将计算后的数据写入内存或者磁盘，当然也有可能spill到磁盘。 
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      6.关闭writer，并返回这个map是否完成的status信息
      writer.stop(success = true).get
   
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }

```
下面先看这个 manager.getWriter，首先它会传入一个参数dep.shuffleHandle，先看这个参数是怎么得来的，他就是Dependency中的一个变量：
```
  这里注意这个_rdd是map端的RDD
  val shuffleHandle: ShuffleHandle = _rdd.context.env.shuffleManager.registerShuffle(
    shuffleId, _rdd.partitions.size, this)
```
这里它像这个shuffleManager注册了这一次shuffle：

```
 override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    1. 这一种suffle会在reduce端进行map端数据的合成操作
    if (SortShuffleWriter.shouldBypassMergeSort(SparkEnv.get.conf, dependency)) {
      // If there are fewer than spark.shuffle.sort.bypassMergeThreshold partitions and we don't
      // need map-side aggregation, then write numPartitions files directly and just concatenate
      // them at the end. This avoids doing serialization and deserialization twice to merge
      // together the spilled files, which would happen with the normal code path. The downside is
      // having multiple files open at a time and thus more memory allocated to buffers.
      new BypassMergeSortShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    2. 下面这是实用tungsten计划的shuffle，会使用off-heap的内存
    } else if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      // Otherwise, try to buffer map outputs in a serialized form, since this is more efficient:
      new SerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
     3. 这才是默认情况下使用的shuffle方式
      // Otherwise, buffer map outputs in a deserialized form:
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }
  
```
接着看writer是怎么获得的：

```
/** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {
    numMapsForShuffle.putIfAbsent(
      handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    val env = SparkEnv.get
    1. 根据前面的handle的结果，这里会匹配到BaseShuffleHandle
    handle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        new UnsafeShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          context.taskMemoryManager(),
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf)
      case bypassMergeSortHandle: BypassMergeSortShuffleHandle[K @unchecked, V @unchecked] =>
        new BypassMergeSortShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          bypassMergeSortHandle,
          mapId,
          context,
          env.conf)
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new SortShuffleWriter(shuffleBlockResolver, other, mapId, context)
    }
  }
  
```
所以这里直接返回new SortShuffleWriter(shuffleBlockResolver, other, mapId, context)。

首先，要写的数据是通过rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]]，它的返回类型是Iterator，所以这里write方法是通过操作这个Iterator来实现的。关于这个方法后面我们在看，这里先看这个write的实现：

```
override def write(records: Iterator[Product2[K, V]]): Unit = {
    1. 如果数据需要在map端combine，则需要传入dep.aggregator，下面的这个dep.keyOrdering是空值，因为在spark的sortedshuffle中，数据是不排序的。这里的这个partitioner是用来为后续RDD构造partitions的
    sorter = if (dep.mapSideCombine) {
      require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
      new ExternalSorter[K, V, C](
        context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
    } else {
      // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
      // care whether the keys get sorted in each partition; that will be done on the reduce side
      // if the operation being run is sortByKey.
      2. 如果数据不需要在map端combine，则aggregator传None就行
      new ExternalSorter[K, V, V](
        context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
    }
    3.  将数据先放入缓存中，如果缓存不够用spill到磁盘，在这一步也会对相同key值的数据进行combine操作
    sorter.insertAll(records)

    // Don't bother including the time to open the merged output file in the shuffle write time,
    // because it just opens a single file, so is typically too fast to measure accurately
    // (see SPARK-3570).
    val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
    val tmp = Utils.tempFileWith(output)
    val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
   4. 将数据写入data文件中
    val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
   5. 将数据写入index文件中
    shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
  }
  
```
到这里你可能对上面3，4，5的操作有些疑惑，不知道它们具体是怎么做的，放心我绝对会分析透彻的，因为学spark搞不懂shuffle的话，那没有意义。

### 3
下面先看3，insertAll：
```
def insertAll(records: Iterator[Product2[K, V]]): Unit = {
    // TODO: stop combining if we find that the reduction factor isn't high
    3.1.在new sorter的时候，它的那个aggregator的变量我们是由传值的
    val shouldCombine = aggregator.isDefined
    3.2. 返回为true，也就是需要在map端对数据进行combine
    if (shouldCombine) {
      // Combine values in-memory first using our AppendOnlyMap
    3.3. mergeValue其实就是我们用reduceByKey的时候参数值，即_+_
      val mergeValue = aggregator.get.mergeValue
    3.4. createCombiner其实是和mergeValue一样的，都是_+_，但是在使用createCombiner中只传了一个参数，所以另一个参数默认采用默认值，这里为0；
      val createCombiner = aggregator.get.createCombiner
      var kv: Product2[K, V] = null
    3.5. 这个update也是一个函数，他用来操作键值对的数据，如果key相同则合并其value的值
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
     3.6. 对每一条数据进行操作
      while (records.hasNext) {
        3.6.1 只是记录了读取的记录的数量
        addElementsRead()
        kv = records.next()
        3.6.2 更新map中的数据，其中private var map = new PartitionedAppendOnlyMap[K, C]，如果需要进行combine就讲数据放在map中，然后在map中对数据进行更新操作
        map.changeValue((getPartition(kv._1), kv._1), update)
        3.6.3 看是否需要把内存中的数据spill到磁盘上
        maybeSpillCollection(usingMap = true)
      }
    } else {
      // Stick values into our buffer
      while (records.hasNext) {
        addElementsRead()
        val kv = records.next()
        buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
        maybeSpillCollection(usingMap = false)
      }
    }
  }  
```
别着急啊！ 这里务必每一行代码都看明白，接着看  3.6.2中方法的操作：

```
  override def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    val newValue = super.changeValue(key, updateFunc)
    super.afterUpdate()
    newValue
  }
```
先看这个super.changeValue：

```
  def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    assert(!destroyed, destructionMessage)
   1. 如果传入的key为空值的话，队列自动增加长度，它这里面实现的很巧妙：因为队列自动增加后，就肯定会多出来一个值，如果你不给它赋值的话，它就为null，但是这个值又不占队列中具体的位置，只要在最后的时候保留一个没有赋值的位置即可。
    val k = key.asInstanceOf[AnyRef]
    if (k.eq(null)) {
      if (!haveNullValue) {
        incrementSize()
      }
      nullValue = updateFunc(haveNullValue, nullValue)
      haveNullValue = true
      return nullValue
    }
   2. Re-hash a value to deal better with hash functions that don't differ in the lower bits.
    这一段英文比较易懂，主要是中文哥们不知道怎么说啊！！！
    var pos = rehash(k.hashCode) & mask
    var i = 1
    while (true) {
      val curKey = data(2 * pos)
      if (k.eq(curKey) || k.equals(curKey)) {
        1. 如果有相同的值的话，则根据updateFunc更新这个key对应的value
        val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        return newValue
      } else if (curKey.eq(null)) {
       2. 如果没有的话，则将这个值添加到列表中，并增加列表长度，注意这里这个列表可不是线性的，准确的说，它是一个hash列表
        val newValue = updateFunc(false, null.asInstanceOf[V])
        data(2 * pos) = k
        data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
        incrementSize()
        return newValue
      } else {
        3. 如果pos和其它的key重合，则继续计算其位置
        val delta = i
        pos = (pos + delta) & mask
        i += 1
      }
    }
    4. 这一段代码永远不会到达，但是你需要按照他这样写一下，因为compiler根本不知道你的执行逻辑，相反你不写的话就会编译错误
    null.asInstanceOf[V] // Never reached but needed to keep compiler happy
  }
```
这个方法讲完了，接着看super.afterUpdate()方法：

```
  protected def afterUpdate(): Unit = {
    numUpdates += 1
    if (nextSampleNum == numUpdates) {
      takeSample()
    }
  }

```
更新数据更新的次数，数据更新达到这一个采用点，则去采样：

```
  private def takeSample(): Unit = {
    1. SizeEstimator.estimate是用来计算存储当前对象所需的内存空间的，将采样的数据加入样本中
    samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates))
    // Only use the last two samples to extrapolate
    2. 样本中仅保存上一次更新的存储当前对象所需的内存空间和最后一次更新的存储当前对象所需的内存空间，如果大于两个样本数据，则删除最前面的样本数据
     if (samples.size > 2) {
      samples.dequeue()
    }
    val bytesDelta = samples.toList.reverse match {
      case latest :: previous :: tail =>
        (latest.size - previous.size).toDouble / (latest.numUpdates - previous.numUpdates)
      // If fewer than 2 samples, assume no change
      case _ => 0
    }
    3. 每一次在前面提到的map中添加一个对象需要消耗多少内存
    bytesPerUpdate = math.max(0, bytesDelta)
    4. 计算下次取样的时 map更新的次数
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }
```
到这里3.6.2这个方法就讲完了，讲了半天其实insertAll方法做的就是把进行聚合的数据放在Map中：

![Screen Shot 2016-12-11 at 9.54.31 PM.png](http://upload-images.jianshu.io/upload_images/3736220-c21aa02aee1f4b0b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

下面讲  3.6.3这个方法：

```
 private def maybeSpillCollection(usingMap: Boolean): Unit = {
    var estimatedSize = 0L
    if (usingMap) {
       1. 先估计map有可能占用的内存空间
      estimatedSize = map.estimateSize()
       2. 下面马上详细说明这个方法
       if (maybeSpill(map, estimatedSize)) {
        map = new PartitionedAppendOnlyMap[K, C]
      }
    } else {
      3. 这是用于非map的情况，spill的原理和map的一样
      estimatedSize = buffer.estimateSize()
      if (maybeSpill(buffer, estimatedSize)) {
        buffer = new PartitionedPairBuffer[K, C]
      }
    }
    4.这个变量由内部测量系统收集内存占用信息
    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }
  }

```
下面马上分析maybeSpill：

```
 protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false
    1. 写入的数据是32的倍数且当前内存空间大于myMemoryThreshold这个值，则进行判断是否shouldSpill，这只是条件的一种
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
    2. 要申请的空间
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
    3. 申请到的空间，经常会小于你要申请的空间，这里是通过UnifiedMemoryManager来实现的，1.6.x之后默认都是采用这个。  
      val granted =
        taskMemoryManager.acquireExecutionMemory(amountToRequest, MemoryMode.ON_HEAP, null)
      myMemoryThreshold += granted
      // If we were granted too little memory to grow further (either tryToAcquire returned 0,
      // or we already had more memory than myMemoryThreshold), spill the current collection
      4. 如果当前使用的内存大于设定的myMemoryThreshold这个值，则应该把数据spill到磁盘上
      shouldSpill = currentMemory >= myMemoryThreshold
    }
    5. 这里还有一种条件就是写的数据条数大于numElementsForceSpillThreshold，也应该进行spill
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
    // Actually spill
    if (shouldSpill) {
    6. 若满足条件，则进行spill，并释放内存，至于之后的操作，从上面你会看到他继续又new了一个map来写新数据
      _spillCount += 1
      logSpillage(currentMemory)
    7. 具体的spill操作
      spill(collection)
      _elementsRead = 0
      _memoryBytesSpilled += currentMemory
      releaseMemory()
    }
    shouldSpill
  }
```
接着就看这个spill的操作吧！

```
 override protected[this] def spill(collection: WritablePartitionedPairCollection[K, C]): Unit = {
    // Because these files may be read during shuffle, their compression must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more context.
    1. 即创建了临时的Block，有创建了临时的file
    val (blockId, file) = diskBlockManager.createTempShuffleBlock()

    // These variables are reset after each flush
    var objectsWritten: Long = 0
    var spillMetrics: ShuffleWriteMetrics = null
    var writer: DiskBlockObjectWriter = null
    
    def openWriter(): Unit = {
      assert (writer == null && spillMetrics == null)
      spillMetrics = new ShuffleWriteMetrics
      writer = blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)
    }
    2. 构造一个DiskWriter
    openWriter()

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // How many elements we have in each partition
    val elementsPerPartition = new Array[Long](numPartitions)

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is closed at the end of this process, and cannot be reused.
    def flush(): Unit = {
      val w = writer
      writer = null
      w.commitAndClose()
      _diskBytesSpilled += spillMetrics.shuffleBytesWritten
      batchSizes.append(spillMetrics.shuffleBytesWritten)
      spillMetrics = null
      objectsWritten = 0
    }

    var success = false
    try {
      1.下面这个方法是把数据重新按key值中的partitionId来排序，我们这里的key是一个Tuple，即（partitionId，k），下面我详细介绍
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      while (it.hasNext) {
        val partitionId = it.nextPartition()
        require(partitionId >= 0 && partitionId < numPartitions,
          s"partition Id: ${partitionId} should be in the range [0, ${numPartitions})")
        it.writeNext(writer)
        elementsPerPartition(partitionId) += 1
        objectsWritten += 1
       2. 当写的对象达到一定个数时，就spill到另一个文件中
        if (objectsWritten == serializerBatchSize) {
          flush()
          openWriter()
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else if (writer != null) {
        val w = writer
        writer = null
        3.处理因为异常没有flush的数据
        w.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (!success) {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        if (writer != null) {
          writer.revertPartialWritesAndClose()
        }
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }
    4. 最终把写的文件翻入到spills中
    spills.append(SpilledFile(file, blockId, batchSizes.toArray, elementsPerPartition))
  }
```

其实整个过程很简单，他就是在不停的写数据，当写入内存的数据超过一定值，则把它spill到磁盘，就这么简单。然后这里尤其注意
collection.partitionedDestructiveSortedIterator(comparator)，这是我们内存map中的数据，其数据的内容((partitionId,key),value)：

```
def partitionedDestructiveSortedIterator(keyComparator: Option[Comparator[K]])
    : Iterator[((Int, K), V)] = {
  1. 首先它这里会构造一个按partition和key进行比较的比较器，这个比较器先按partition来排序，然后再用key的hashcode进行排序，记住这里spark系统是进行key排序了的，而不是没有排序，只是它这里是按照key的hashcode进行的排序。
    val comparator = keyComparator.map(partitionKeyComparator).getOrElse(partitionComparator)
   2. 然后通过这个比较器再对这个map进行排序
    destructiveSortedIterator(comparator)
  }

```
接着看这个destructiveSortedIterator方法：
```
 def destructiveSortedIterator(keyComparator: Comparator[K]): Iterator[(K, V)] = {
    destroyed = true
    // Pack KV pairs into the front of the underlying array
    1. 首先先将原来map中的数据摧毁，其实也就是把key的非null值对应的数据，移到最前面
    var keyIndex, newIndex = 0
    while (keyIndex < capacity) {
      if (data(2 * keyIndex) != null) {
        data(2 * newIndex) = data(2 * keyIndex)
        data(2 * newIndex + 1) = data(2 * keyIndex + 1)
        newIndex += 1
      }
      keyIndex += 1
    }
    assert(curSize == newIndex + (if (haveNullValue) 1 else 0))
    2. 然后再对这个map进行排序
    new Sorter(new KVArraySortDataFormat[K, AnyRef]).sort(data, 0, newIndex, keyComparator)
   3. 最后构造一个Iterator来获取它的值
    new Iterator[(K, V)] {
      var i = 0
      var nullValueReady = haveNullValue
      def hasNext: Boolean = (i < newIndex || nullValueReady)
      def next(): (K, V) = {
        if (nullValueReady) {
          nullValueReady = false
          (null.asInstanceOf[K], nullValue)
        } else {
          val item = (data(2 * i).asInstanceOf[K], data(2 * i + 1).asInstanceOf[V])
          i += 1
          item
        }
      }
    }
  }
```
下面看一下这个spill的过程：
![Screen Shot 2016-12-11 at 10.31.36 PM.png](http://upload-images.jianshu.io/upload_images/3736220-9cf015a0eecc50c1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


### 4
下面看4:
```
val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
```
也就是把spillfiles和内存中的数据都存在data文件中：

```
 def writePartitionedFile(
      blockId: BlockId,
      outputFile: File): Array[Long] = {
   1. 下面我们看spills中有数据的情况，看懂了下面的上面的你肯定没问题
    // Track location of each range in the output file
    val lengths = new Array[Long](numPartitions)

    if (spills.isEmpty) {
      // Case where we only have in-memory data
      val collection = if (aggregator.isDefined) map else buffer
      val it = collection.destructiveSortedWritablePartitionedIterator(comparator)
      while (it.hasNext) {
        val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
          context.taskMetrics.shuffleWriteMetrics.get)
        val partitionId = it.nextPartition()
        while (it.hasNext && it.nextPartition() == partitionId) {
          it.writeNext(writer)
        }
        writer.commitAndClose()
        val segment = writer.fileSegment()
        lengths(partitionId) = segment.length
      }
    } else {
      // We must perform merge-sort; get an iterator by partition and write everything directly.
      2.在调用partitionedIterator方法后，会对应的partition中的已经combine过的数据
      for ((id, elements) <- this.partitionedIterator) {
        if (elements.hasNext) {
          val writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize,
            context.taskMetrics.shuffleWriteMetrics.get)
      3. 将partition中的数据写入data文件     
           for (elem <- elements) {
            writer.write(elem._1, elem._2)
          }
          writer.commitAndClose()
          val segment = writer.fileSegment()
          记录每个partion占用的offset
          lengths(id) = segment.length
        }
      }
    }

    4.更改task测量系统中的参数值
    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)
    context.internalMetricsToAccumulators(
      InternalAccumulator.PEAK_EXECUTION_MEMORY).add(peakMemoryUsedBytes)

    lengths
  }
  
```
下面看这个this.partitionedIterator，看它是怎么合成的：


```
def partitionedIterator: Iterator[(Int, Iterator[Product2[K, C]])] = {
    val usingMap = aggregator.isDefined
    val collection: WritablePartitionedPairCollection[K, C] = if (usingMap) map else buffer
     1.我们只看spill不为null的情况
    if (spills.isEmpty) {
      // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
      // we don't even need to sort by anything other than partition ID
      if (!ordering.isDefined) {
        // The user hasn't requested sorted keys, so only sort by partition ID, not key
        groupByPartition(collection.partitionedDestructiveSortedIterator(None))
      } else {
        // We do need to sort by both partition ID and key
        groupByPartition(collection.partitionedDestructiveSortedIterator(Some(keyComparator)))
      }
    } else {
      // Merge spilled and in-memory data
      2. 通过调用merge方法来实现把上文中spillFile中的数据和当前map中的数据进行merge
      merge(spills, collection.partitionedDestructiveSortedIterator(comparator))
    }
  }  
```


下面直接看这个merge：

```
 private def merge(spills: Seq[SpilledFile], inMemory: Iterator[((Int, K), C)])
      : Iterator[(Int, Iterator[Product2[K, C]])] = {
    1.通过构建SpillReader可以通过readNextPartition得到Iterator，通过Iterater又可以获取每条记录的信息，这里其实是生成了一个SpillReader列表 
    val readers = spills.map(new SpillReader(_))
    2. 通过使用buffered方法，构建一个带BufferedIterator接口的Iterator
    2.1 用来提供了head方法的查询
    2.2 获取next值的时候，获取的是(key,value)的值
    val inMemBuffered = inMemory.buffered
    3. 这里尤其需要注意的是map中的数据是每一次都是针对同一个partition在操作，也就是同一个Spills和map中属于同一个partition的数据
    (0 until numPartitions).iterator.map { p =>
    4. 内存中的数据可以通过构建IteratorForPartition来获取当前partition下的Iterator
      val inMemIterator = new IteratorForPartition(p, inMemBuffered)
    5. 如果都是 Iterator那就可以合并操作了
       val iterators = readers.map(_.readNextPartition()) ++ Seq(inMemIterator)
     6. 在前面构建Sorter的时候其实我们已经定义了aggregator，所以由此可以看出map端的combile还会在根据key值聚合一次
      if (aggregator.isDefined) {
        // Perform partial aggregation across partitions
     7. 所以就直接进到这里面来啦
        (p, mergeWithAggregation(
          iterators, aggregator.get.mergeCombiners, keyComparator, ordering.isDefined))
      } else if (ordering.isDefined) {
        // No aggregator given, but we have an ordering (e.g. used by reduce tasks in sortByKey);
        // sort the elements without trying to merge them
        (p, mergeSort(iterators, ordering.get))
      } else {
        (p, iterators.iterator.flatten)
      }
    }
  }
```
那接着看mergeWithAggregation这个方法吧：


```
 private def mergeWithAggregation(
      iterators: Seq[Iterator[Product2[K, C]]],
      mergeCombiners: (C, C) => C,
      comparator: Comparator[K],
      totalOrder: Boolean)
      : Iterator[Product2[K, C]] =
  {
    1. 这个方法中只分析符合if条件的操作，因为上面的搞明白了，else语句小case
    if (!totalOrder) {
      // We only have a partial ordering, e.g. comparing the keys by hash code, which means that
      // multiple distinct keys might be treated as equal by the ordering. To deal with this, we
      // need to read all keys considered equal by the ordering at once and compare them.
       1. 这个iterators参数指的是spills和map中的iterators的集合，再进行完mergeSort方法后，在调用next的时候，它会根据iterators操作的数据按key生序来提取数据，记住这一步尤其重要。
      2. buffered方法就是提供了一个可以查看这群iterators的head的方法
      3. 下面我们会详细介绍一下mergeSort方法
      new Iterator[Iterator[Product2[K, C]]] {
        val sorted = mergeSort(iterators, comparator).buffered

        // Buffers reused across elements to decrease memory allocation
        val keys = new ArrayBuffer[K]
        val combiners = new ArrayBuffer[C]

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Iterator[Product2[K, C]] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          keys.clear()
          combiners.clear()
          val firstPair = sorted.next()
          keys += firstPair._1
          combiners += firstPair._2
          val key = firstPair._1
          4. 然后就是对key值的value，进行聚合操作操作了，它这里实现的特别巧妙，它会根据前面通过mergeSort生成的Iterater分别去查找每一个spill和map中Iterater去获取该key值的value，如果有，则进行聚合。关于这里的这个比较器也是比较的key的hashcode，所以这样的话每一次就能获得每个partition下的所有（key，value）数据了
          while (sorted.hasNext && comparator.compare(sorted.head._1, key) == 0) {
            val pair = sorted.next()
            var i = 0
            var foundKey = false
            while (i < keys.size && !foundKey) {
              if (keys(i) == pair._1) {
                combiners(i) = mergeCombiners(combiners(i), pair._2)
                foundKey = true
              }
              i += 1
            }
            if (!foundKey) {
              keys += pair._1
              combiners += pair._2
            }
          }

          // Note that we return an iterator of elements since we could've had many keys marked
          // equal by the partial order; we flatten this below to get a flat iterator of (K, C).
          keys.iterator.zip(combiners.iterator)
        }
     2. 关于这个flatMap操作就是把操作各个partition数据的iterater连接到一起
      }.flatMap(i => i)
    } else {
      // We have a total ordering, so the objects with the same key are sequential.
      new Iterator[Product2[K, C]] {
        val sorted = mergeSort(iterators, comparator).buffered

        override def hasNext: Boolean = sorted.hasNext

        override def next(): Product2[K, C] = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          val elem = sorted.next()
          val k = elem._1
          var c = elem._2
          while (sorted.hasNext && sorted.head._1 == k) {
            val pair = sorted.next()
            c = mergeCombiners(c, pair._2)
          }
          (k, c)
        }
      }
    }
  }
```

上面在进行聚合操作的时候，提到的spill和map中Iterater，其实是经过（partition，key）排序的，会获得（key，value）的iterater。下面看这个mergeSort方法：

```
            
      private def mergeSort(iterators: Seq[Iterator[Product2[K, C]]], comparator: Comparator[K])
      : Iterator[Product2[K, C]] =
  {
    val bufferedIters = iterators.filter(_.hasNext).map(_.buffered)
    type Iter = BufferedIterator[Product2[K, C]]
    1. 这里主要是PriorityQueue中的compare方法来实现的，因为前面我们是按照把spill的数据按照key值进行了排序，所以PriorityQueue操作的各个Iterater其实都是有顺序的，所以在使用dequeue方法时正好可以获得其最小key值对应的数据。这里对key 值的排序其实是按照它的hashCode进行排序的。一定要注意理解这部分的代码。
    val heap = new mutable.PriorityQueue[Iter]()(new Ordering[Iter] {
      // Use the reverse of comparator.compare because PriorityQueue dequeues the max
      override def compare(x: Iter, y: Iter): Int = -comparator.compare(x.head._1, y.head._1)
    })
    heap.enqueue(bufferedIters: _*)  // Will contain only the iterators with hasNext = true
    new Iterator[Product2[K, C]] {
      override def hasNext: Boolean = !heap.isEmpty

      override def next(): Product2[K, C] = {
        if (!hasNext) {
          throw new NoSuchElementException
        }
        val firstBuf = heap.dequeue()
        val firstPair = firstBuf.next()
        if (firstBuf.hasNext) {
          heap.enqueue(firstBuf)
        }
        firstPair
      }
    }
  }

 
```

好，到这里shuffle 的写操作就完全ok了，下面关于这一块的运行，我也画了一幅图：

![Screen Shot 2016-12-11 at 11.07.57 PM.png](http://upload-images.jianshu.io/upload_images/3736220-49172364dc7a2dde.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

接上图，我的屏幕不够大了：
![Screen Shot 2016-12-11 at 11.24.36 PM.png](http://upload-images.jianshu.io/upload_images/3736220-1c53d7d2f8d9ecbd.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

最后还要看一下5，即将数据写入index文件中：
下看一下5的位置：
      
```
 4. 也就是上面我们分析的写入data中的文件，返回的是一个Array[long]的数组，其中纪录了每个partition的长度。
  val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
 5. 其实没什么好讲的，就是把上面这个数组写入index文件中     shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)

```

所以最终每一个task的文件最终产生两个文件，而一个spark程序shuffle产生的数据就取决于并行度或者说reducer的数量，即2*recuder个。ok，shuffle write的过程今天就全部讲完了。大家务必把这一章搞明白，因为这一章是你成为spark高手，必须掌握的一部分，另外，spark性能调优主要的就是调整shuffle write 操作产生的数据，如果这一章你搞不明白，你讲永远停留在使用层面。下一章肯定就是讲shuffle read了，read相对来说就很简单了！

