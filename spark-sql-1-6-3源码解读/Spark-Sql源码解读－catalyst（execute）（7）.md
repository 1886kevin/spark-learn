这一篇博客，应该也是catalyst的最后一篇博客了，也就是业务的具体执行，闲言少叙，直接来看这个execute方法，也就是执行前面所构造的那个sparkplan：

```
 /**
   * Returns the result of this query as an RDD[InternalRow] by delegating to doExecute
   * after adding query plan information to created RDDs for visualization.
   * Concrete implementations of SparkPlan should override doExecute instead.
   */
  final def execute(): RDD[InternalRow] = {
    if (children.nonEmpty) {
      1. 前面这里就是对数据处理的一些检查，大家自己看一下
      val hasUnsafeInputs = children.exists(_.outputsUnsafeRows)
      val hasSafeInputs = children.exists(!_.outputsUnsafeRows)
      assert(!(hasSafeInputs && hasUnsafeInputs),
        "Child operators should output rows in the same format")
      assert(canProcessSafeRows || canProcessUnsafeRows,
        "Operator must be able to process at least one row format")
      assert(!hasSafeInputs || canProcessSafeRows,
        "Operator will receive safe rows as input but cannot process safe rows")
      assert(!hasUnsafeInputs || canProcessUnsafeRows,
        "Operator will receive unsafe rows as input but cannot process unsafe rows")
    }
    RDDOperationScope.withScope(sparkContext, nodeName, false, true) {
      2. 然后它这里需要做一些准备工作
      prepare()
      3. 而后就是去执行具体的业务逻辑。
      doExecute()
    }
  }    
```
### prepare
接着先看这个prepare方法，也就是一些准备操作，它具体是在干什么？


```
  /**
   * Prepare a SparkPlan for execution. It's idempotent.
   */
  final def prepare(): Unit = {
    if (prepareCalled.compareAndSet(false, true)) {
      1.这里它就是具体去做这个准备的工作
      doPrepare()
      2. 使其子节点也去准备，这就是一个递归的操作
      children.foreach(_.prepare())
    }
  }

```
然后看这个doPrepare方法，这里这个doPrepare对于某些特定的操作来说需要去具体实现的，下面是需要实现这个方法的操作：

![Screen Shot 2016-12-21 at 11.11.23 PM.png](http://upload-images.jianshu.io/upload_images/3736220-8fc17002ad92d7a0.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
接着我们看这个Exchange操作的的doPrepare的实现：



```
   override protected def doPrepare(): Unit = {
    // If an ExchangeCoordinator is needed, we register this Exchange operator
    // to the coordinator when we do prepare. It is important to make sure
    // we register this operator right before the execution instead of register it
    // in the constructor because it is possible that we create new instances of
    // Exchange operators when we transform the physical plan
    // (then the ExchangeCoordinator will hold references of unneeded Exchanges).
    // So, we should only call registerExchange just before we start to execute
    // the plan.
   这个exchangeCoordinator就是上一篇博客中来常见的，这里我们需要注册一下个Exchange。
    coordinator match {
      case Some(exchangeCoordinator) => exchangeCoordinator.registerExchange(this)
      case None =>
    }
  }
```
### doExecute
到这里准备工作就完成了，下面看doExecute的实现，这里这个实现也是在具体的操作中来实现的，这里我们就拿这个Exchange来说（因为它最特别，又最难以理解，搞定了它，别的操作对你来说都是小case）：

```
  protected override def doExecute(): RDD[InternalRow] = attachTree(this , "execute") {
    coordinator match {
      1. 因为上面有使用exchangeCoordinator，我们就直接走这个流程。
      case Some(exchangeCoordinator) =>
        val shuffleRDD = exchangeCoordinator.postShuffleRDD(this)
        assert(shuffleRDD.partitions.length == newPartitioning.numPartitions)
        shuffleRDD
      case None =>
        val shuffleDependency = prepareShuffleDependency()
        preparePostShuffleRDD(shuffleDependency)
    }
  }   

```
这里它直接使用了这个postShuffleRDD的方法：


```
   def postShuffleRDD(exchange: Exchange): ShuffledRowRDD = {
    1.通过这里你可以得知这个操作中肯定把生成的RDD放到了这里面，而这也是实现exchange操作的关键代码。
    doEstimationIfNecessary()

    if (!postShuffleRDDs.containsKey(exchange)) {
      throw new IllegalStateException(
        s"The given $exchange is not registered in this coordinator.")
    }

    postShuffleRDDs.get(exchange)
  }
```
再讲doEstimationIfNecessary之前，我们先另外讲两个方法：

#### prepareShuffleDependency

```
/**
   * Returns a [[ShuffleDependency]] that will partition rows of its child based on
   * the partitioning scheme defined in `newPartitioning`. Those partitions of
   * the returned ShuffleDependency will be the input of shuffle.
   */
  private[sql] def prepareShuffleDependency(): ShuffleDependency[Int, InternalRow, InternalRow] = {
    1.首先先获得前面依赖的RDD
    val rdd = child.execute()

    val part: Partitioner = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) => new HashPartitioner(numPartitions)
          2. 然后再构建我们使用的Partitioner，这里以HashPartitioning为例，这里就构造了这个HashPartitioner(numPartitions)
      case HashPartitioning(expressions, numPartitions) => new HashPartitioner(numPartitions)
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
        // partition bounds. To get accurate samples, we need to copy the mutable keys.
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          val mutablePair = new MutablePair[InternalRow, Null]()
          iter.map(row => mutablePair.update(row.copy(), null))
        }
        // We need to use an interpreted ordering here because generated orderings cannot be
        // serialized and this ordering needs to be created on the driver in order to be passed into
        // Spark core code.
        implicit val ordering = new InterpretedOrdering(sortingExpressions, child.output)
        new RangePartitioner(numPartitions, rddForSampling, ascending = true)
      case SinglePartition =>
        new Partitioner {
          override def numPartitions: Int = 1
          override def getPartition(key: Any): Int = 0
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }
        2. 这里就是获取一个Partition的key的提取器。
    def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
        // Distributes elements evenly across output partitions, starting from a random partition.
        var position = new Random(TaskContext.get().partitionId()).nextInt(numPartitions)
        (row: InternalRow) => {
          // The HashPartitioner will handle the `mod` by the number of partitions
          position += 1
          position
        }
      3.这里匹配的就是这个case，生成一个newMutableProjection，而这个newMutableProjection是和每一条记录中的数据相匹配的。
      case HashPartitioning(expressions, _) => newMutableProjection(expressions, child.output)()
      case RangePartitioning(_, _) | SinglePartition => identity
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }
    4. 然后接下来就是根据上面那个方法获取每一条记录的key，改变partition中数据的类型为（partitionID，row）
    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      5. 这个needToCopyObjectsBeforeShuffle是很重要的一个方法，下面就是根据下面的生成新的RDD。
      if (needToCopyObjectsBeforeShuffle(part, serializer)) {
        rdd.mapPartitionsInternal { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          iter.map { row => (part.getPartition(getPartitionKey(row)), row.copy()) }
        }
      } else {
        rdd.mapPartitionsInternal { iter =>
          val getPartitionKey = getPartitionKeyExtractor()
          val mutablePair = new MutablePair[Int, InternalRow]()
          iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
        }
      }
    }

    // Now, we manually create a ShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
        6. 最终就构建了这个dependency，这里注意这个partitioner可不是HashPartioner，不过他的数据是通过HashPartioner来构建的。这里的这个PartitionIdPassthrough只是一个最简单的Partioner。
     7. 通过前面的分析，通过这样的我们得到最终处理的数据，是满足exchange之后的操作相匹配的。
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions),
        Some(serializer))

    dependency
  }

```
这里我们再看一下这里面的needToCopyObjectsBeforeShuffle方法，这个方法就是说明在shuffle之前是否需要copy数据。下面看这个方法：

```
/**
   * Determines whether records must be defensively copied before being sent to the shuffle.
   * Several of Spark's shuffle components will buffer deserialized Java objects in memory. The
   * shuffle code assumes that objects are immutable and hence does not perform its own defensive
   * copying. In Spark SQL, however, operators' iterators return the same mutable `Row` object. In
   * order to properly shuffle the output of these operators, we need to perform our own copying
   * prior to sending records to the shuffle. This copying is expensive, so we try to avoid it
   * whenever possible. This method encapsulates the logic for choosing when to copy.
   *
   * In the long run, we might want to push this logic into core's shuffle APIs so that we don't
   * have to rely on knowledge of core internals here in SQL.
   *
   * See SPARK-2967, SPARK-4479, and SPARK-7375 for more discussion of this issue.
   *
   * @param partitioner the partitioner for the shuffle
   * @param serializer the serializer that will be used to write rows
   * @return true if rows should be copied before being shuffled, false otherwise
   */
  1. 上面一大堆废话，其实他想说的就是基于RDD的数据，是不可变得，但是基于Spark sql生成的RDD的数据是可变的，原理就像DStream一样啊。所以对于那个可变的，它需要进行copy，但是copy又是一种极其消耗内存的操作，所以它会极大的内存的使用率。
  private def needToCopyObjectsBeforeShuffle(
      partitioner: Partitioner,
      serializer: Serializer): Boolean = {
    // Note: even though we only use the partitioner's `numPartitions` field, we require it to be
    // passed instead of directly passing the number of partitions in order to guard against
    // corner-cases where a partitioner constructed with `numPartitions` partitions may output
    // fewer partitions (like RangePartitioner, for example).
    2. 这句注释的意思就是它会使用partitioner中的numPartitions来当作操作的分区数，因为这里有可能会产生小于这个值的Partitions，它这里使用这个值，其实就是为了保险一点。
    val conf = child.sqlContext.sparkContext.conf
    val shuffleManager = SparkEnv.get.shuffleManager
    val sortBasedShuffleOn = shuffleManager.isInstanceOf[SortShuffleManager]
    3. 这个值只得是reduce端最大的分区数
    val bypassMergeThreshold = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200)
    if (sortBasedShuffleOn) {
      val bypassIsSupported = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]
      if (bypassIsSupported && partitioner.numPartitions <= bypassMergeThreshold) {
        // If we're using the original SortShuffleManager and the number of output partitions is
        // sufficiently small, then Spark will fall back to the hash-based shuffle write path, which
        // doesn't buffer deserialized records.
        // Note that we'll have to remove this case if we fix SPARK-6026 and remove this bypass.
        4. 如果产生的分区数很小的话，就直接让他重新计算，而不是使用copy的方式保留数据
        false
      } else if (serializer.supportsRelocationOfSerializedObjects) {
        // SPARK-4550 and  SPARK-7081 extended sort-based shuffle to serialize individual records
        // prior to sorting them. This optimization is only applied in cases where shuffle
        // dependency does not specify an aggregator or ordering and the record serializer has
        // certain properties. If this optimization is enabled, we can safely avoid the copy.
        //
        // Exchange never configures its ShuffledRDDs with aggregators or key orderings, so we only
        // need to check whether the optimization is enabled and supported by our serializer.
       5. 这里需要判断这个开关有没有打开，具体的原理可以参看SPARK-4550 and  SPARK-7081这两个说明。
        false
      } else {
        // Spark's SortShuffleManager uses `ExternalSorter` to buffer records in memory, so we must
        // copy.
        6. 如果使用的是ExternalSorter来缓存数据的话，则必须使用copy的方式
        true
      }
    } else if (shuffleManager.isInstanceOf[HashShuffleManager]) {
      // We're using hash-based shuffle, so we don't need to copy.
      7. 在使用hash shuffle的时候不需要copy
      false
    } else {
      // Catch-all case to safely handle any future ShuffleManager implementations.
      8. 其它所有情况
      true
    }
  }

```
#### estimatePartitionStartIndices
```
/**
   * Estimates partition start indices for post-shuffle partitions based on
   * mapOutputStatistics provided by all pre-shuffle stages.
   */
  1. 这个方法的意思，就是根据前面rdd计算得到的数据，来根据shuffle后的分区数来来决定这个rdd处理的数据属于哪个分区，这里其实就是指定shuffle后的各个分区获取哪些数据。
  private[sql] def estimatePartitionStartIndices(
      mapOutputStatistics: Array[MapOutputStatistics]): Array[Int] = {
    // If we have mapOutputStatistics.length < numExchange, it is because we do not submit
    // a stage when the number of partitions of this dependency is 0.
    2. 这是因为提交的操作里面partition如果为0的话，就不提交这个任务。
    assert(mapOutputStatistics.length <= numExchanges)

    // If minNumPostShufflePartitions is defined, it is possible that we need to use a
    // value less than advisoryTargetPostShuffleInputSize as the target input size of
    // a post shuffle task.
   2. 指定shuffle后的分区的大小，默认会匹配到case Node，除非在配置文件中配置spark.sql.adaptive.minNumPostShufflePartitions这个参数
    val targetPostShuffleInputSize = minNumPostShufflePartitions match {
      case Some(numPartitions) =>
        val totalPostShuffleInputSize = mapOutputStatistics.map(_.bytesByPartitionId.sum).sum
        // The max at here is to make sure that when we have an empty table, we
        // only have a single post-shuffle partition.
        // There is no particular reason that we pick 16. We just need a number to
        // prevent maxPostShuffleInputSize from being set to 0.
        val maxPostShuffleInputSize =
          math.max(math.ceil(totalPostShuffleInputSize / numPartitions.toDouble).toLong, 16)
        math.min(maxPostShuffleInputSize, advisoryTargetPostShuffleInputSize)

      case None => advisoryTargetPostShuffleInputSize
    }

    logInfo(
      s"advisoryTargetPostShuffleInputSize: $advisoryTargetPostShuffleInputSize, " +
      s"targetPostShuffleInputSize $targetPostShuffleInputSize.")

    // Make sure we do get the same number of pre-shuffle partitions for those stages.
    3. 这里值必须是唯一的，因为前面通过exchange操作，已经重新对他的child根据同样的partitioner进行了分区
    val distinctNumPreShufflePartitions =
      mapOutputStatistics.map(stats => stats.bytesByPartitionId.length).distinct
    // The reason that we are expecting a single value of the number of pre-shuffle partitions
    // is that when we add Exchanges, we set the number of pre-shuffle partitions
    // (i.e. map output partitions) using a static setting, which is the value of
    // spark.sql.shuffle.partitions. Even if two input RDDs are having different
    // number of partitions, they will have the same number of pre-shuffle partitions
    // (i.e. map output partitions).
    assert(
      distinctNumPreShufflePartitions.length == 1,
      "There should be only one distinct value of the number pre-shuffle partitions " +
        "among registered Exchange operator.")
    4. 然后就取出这个唯一的值
    val numPreShufflePartitions = distinctNumPreShufflePartitions.head

    val partitionStartIndices = ArrayBuffer[Int]()
    // The first element of partitionStartIndices is always 0.
    partitionStartIndices += 0

    var postShuffleInputSize = 0L

    var i = 0
    5. 通过下面来计算要shuffle的数据是否满足在targetPostShuffleInputSize这个值以内，如果不满足，则记录下来，在构建RDD的时候重新进行分区。也就是partitionStartIndices这个参数，关于他的使用，接下来会谈到
    while (i < numPreShufflePartitions) {
      // We calculate the total size of ith pre-shuffle partitions from all pre-shuffle stages.
      // Then, we add the total size to postShuffleInputSize.
      var j = 0
      while (j < mapOutputStatistics.length) {
        postShuffleInputSize += mapOutputStatistics(j).bytesByPartitionId(i)
        j += 1
      }

      // If the current postShuffleInputSize is equal or greater than the
      // targetPostShuffleInputSize, We need to add a new element in partitionStartIndices.
      if (postShuffleInputSize >= targetPostShuffleInputSize) {
        if (i < numPreShufflePartitions - 1) {
          // Next start index.
          partitionStartIndices += i + 1
        } else {
          // This is the last element. So, we do not need to append the next start index to
          // partitionStartIndices.
        }
        // reset postShuffleInputSize.
        postShuffleInputSize = 0L
      }

      i += 1
    }

    partitionStartIndices.toArray
  }

```
ok，介绍完了这两个方法，下面再说真正的实现方法，即doEstimationIfNecessary。

### doEstimationIfNecessary

这里必须先搞明白前面两个方法，要不这个方法真心看不懂。下面就来说这个方法：
```
  @GuardedBy("this")
  private def doEstimationIfNecessary(): Unit = synchronized {
    // It is unlikely that this method will be called from multiple threads
    // (when multiple threads trigger the execution of THIS physical)
    // because in common use cases, we will create new physical plan after
    // users apply operations (e.g. projection) to an existing DataFrame.
    // However, if it happens, we have synchronized to make sure only one
    // thread will trigger the job submission.
    if (!estimated) {
      // Make sure we have the expected number of registered Exchange operators.
      assert(exchanges.length == numExchanges)

      val newPostShuffleRDDs = new JHashMap[Exchange, ShuffledRowRDD](numExchanges)

      // Submit all map stages
      val shuffleDependencies = ArrayBuffer[ShuffleDependency[Int, InternalRow, InternalRow]]()
      val submittedStageFutures = ArrayBuffer[SimpleFutureAction[MapOutputStatistics]]()
      var i = 0
      while (i < numExchanges) {
        val exchange = exchanges(i)
        1. 这个方法前面已经讲的很透彻了，构建这个操作的依赖
        val shuffleDependency = exchange.prepareShuffleDependency()
        shuffleDependencies += shuffleDependency
        if (shuffleDependency.rdd.partitions.length != 0) {
          // submitMapStage does not accept RDD with 0 partition.
          // So, we will not submit this dependency.
         2. 也就是根据前面的依赖，根据这个依赖来提交一个stage运行。
          submittedStageFutures +=
            exchange.sqlContext.sparkContext.submitMapStage(shuffleDependency)
        }
        i += 1
      }

      // Wait for the finishes of those submitted map stages.
      val mapOutputStatistics = new Array[MapOutputStatistics](submittedStageFutures.length)
      var j = 0
      while (j < submittedStageFutures.length) {
        // This call is a blocking call. If the stage has not finished, we will wait at here.
        3. 等待这个stage处理的结果，并获取其mapOutputStatistics信息
        mapOutputStatistics(j) = submittedStageFutures(j).get()
        j += 1
      }

      // Now, we estimate partitionStartIndices. partitionStartIndices.length will be the
      // number of post-shuffle partitions.
      val partitionStartIndices =
        if (mapOutputStatistics.length == 0) {
          None
        } else {
          Some(estimatePartitionStartIndices(mapOutputStatistics))
        }

      var k = 0
      while (k < numExchanges) {
        val exchange = exchanges(k)
        4. 最终根据estimatePartitionStartIndices处理的结果来构建一个rdd
        val rdd =
          exchange.preparePostShuffleRDD(shuffleDependencies(k), partitionStartIndices)
        newPostShuffleRDDs.put(exchange, rdd)

        k += 1
      }

      // Finally, we set postShuffleRDDs and estimated.
      assert(postShuffleRDDs.isEmpty)
      assert(newPostShuffleRDDs.size() == numExchanges)
      5. 然后最终把获得的这些rdd放到postShuffleRDDs变量中，
      其实真正的shuffle操作是发生在这些newPostShuffleRDDs中的
      postShuffleRDDs.putAll(newPostShuffleRDDs)
      estimated = true
    }
  }
  
```
### partitionStartIndices的使用
到这里整个流程就结束了，下面还得再谈一下这个方法
```
   exchange.preparePostShuffleRDD(shuffleDependencies(k), partitionStartIndices)
```
直接来看这个方法的实现：

```
  private[sql] def preparePostShuffleRDD(
      shuffleDependency: ShuffleDependency[Int, InternalRow, InternalRow],
      specifiedPartitionStartIndices: Option[Array[Int]] = None): ShuffledRowRDD = {
    // If an array of partition start indices is provided, we need to use this array
    // to create the ShuffledRowRDD. Also, we need to update newPartitioning to
    // update the number of post-shuffle partitions.
    这里如果这个specifiedPartitionStartIndices中是有值的话，则需要指定一个新的newPartitioning，也就是前面估计的时候，有些分区的值是超过那个targetPostShuffleInputSize的值的，也就是有一个过大的partition，这样的话，在接下来就会根据这个信息，来对partition进行合并。（它这里的处理是保证数据均衡）
    specifiedPartitionStartIndices.foreach { indices =>
      assert(newPartitioning.isInstanceOf[HashPartitioning])
      newPartitioning = UnknownPartitioning(indices.length)
    }
    new ShuffledRowRDD(shuffleDependency, specifiedPartitionStartIndices)
  }
       
```
下面就在看一下这个ShuffledRowRDD几个重要方法：

```
1. 这里是获得shuffle前的数据分区
 private[this] val numPreShufflePartitions = dependency.partitioner.numPartitions
 2. 这里是我们在估计的时候获得的indices
  private[this] val partitionStartIndices: Array[Int] = specifiedPartitionStartIndices match {
    case Some(indices) => indices
    case None =>
      // When specifiedPartitionStartIndices is not defined, every post-shuffle partition
      // corresponds to a pre-shuffle partition.
      (0 until numPreShufflePartitions).toArray
  }
 3. 这个用来合并的Partitioner
  private[this] val part: Partitioner =
    new CoalescedPartitioner(dependency.partitioner, partitionStartIndices)
  4. 这里就是依赖
  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override val partitioner: Option[Partitioner] = Some(part)
  5. 这个操作的实现，根据indices来获得这个新的rdd的分区。从这里你就可以看出来，他每个分区是由shuffle前的分区来构建的，这里它是通过CoalescedPartitioner来构建的
  override def getPartitions: Array[Partition] = {
    assert(partitionStartIndices.length == part.numPartitions)
    Array.tabulate[Partition](partitionStartIndices.length) { i =>
      val startIndex = partitionStartIndices(i)
      val endIndex =
        if (i < partitionStartIndices.length - 1) {
          partitionStartIndices(i + 1)
        } else {
          numPreShufflePartitions
        }
      new ShuffledRowRDDPartition(i, startIndex, endIndex)
    }
  }
        
```
下面我们看一下这个CoalescedPartitioner：
```
   class CoalescedPartitioner(val parent: Partitioner, val partitionStartIndices: Array[Int])
  extends Partitioner {
  1.这个方法就是根据parent的分区来构建的一个新的分区映射，比如这个parent有3个分区，indices的值为［0，2］那么最终的分区情况就是［0，1］［2］对应的result的结果就是［0，0，1］
  @transient private lazy val parentPartitionMapping: Array[Int] = {
    val n = parent.numPartitions
    val result = new Array[Int](n)
    for (i <- 0 until partitionStartIndices.length) {
      val start = partitionStartIndices(i)
      val end = if (i < partitionStartIndices.length - 1) partitionStartIndices(i + 1) else n
      for (j <- start until end) {
        result(j) = i
      }
    }
    result
  }

  override def numPartitions: Int = partitionStartIndices.length
  2. 而这方法指出在执行shuffle操作的时候，最终映射的partition为多少，还举上面的例子，假设读取的key值为0，1的数据都会存放在partition 0中，读取的key为2的数据会放在partition 1中
  override def getPartition(key: Any): Int = {
    parentPartitionMapping(parent.getPartition(key))
  }

  override def equals(other: Any): Boolean = other match {
    case c: CoalescedPartitioner =>
      c.parent == parent && Arrays.equals(c.partitionStartIndices, partitionStartIndices)
    case _ =>
      false
  }

  override def hashCode(): Int = 31 * parent.hashCode() + Arrays.hashCode(partitionStartIndices)
}
           
```
ok，到这里整个Exchange的操作就全部讲完了，catalyst最难的部分，你也掌握了。

### spark sql执行过程

上面最难的操作已经讲的很明白了，至于别的操作大家自己去看，这里对于每一个sparkplan的操作在使用execute的时候，都会去使用doExchange来处理，而doExchange又在调用child的execute方法，他这是一步步的在干什么。其实它这里执行execute方法的时候就是在构建一个RDD，而父rdd依赖于子rdd来构建，到这里你有没有想到什么？

spark streaming里的DStream的pipeline的构建最终通过构建rdd的pipeline，现在你应该也明白了吧。spark sql中这个方法execute的执行也是在构建rdd的pipeline。所以最终只要产生了这个rdd的pipeline，我们就可以执行job的提交操作了，而后续的工作就又都交给spark core来执行了。

到这里整个spark sql的流程，就讲完了，我博客的工作也应该可以告一段落了。在此谢谢大家的帮助。

### 参考资料
1. Spark 1.6.3源码
2. https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf
3. http://infolab.stanford.edu/~hyunjung/cs346/qpnotes.html
4. http://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf
5. http://blog.hydronitrogen.com/2016/02/22/in-the-code-spark-sql-query-planning-and-execution/#Exchange
6. http://blog.csdn.net/oopsoom/article/details/38257749
7. http://sqlblog.com/blogs/paul_white/archive/2012/04/28/query-optimizer-deep-dive-part-1.aspx
