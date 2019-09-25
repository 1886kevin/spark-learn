前面的几篇博客依次介绍了SqlParser，Analyzer，Optimizer，他们其实都是对sql语句处理，最终得到的其实就是优化的sql语句，在spark sql框架下用Optimized Plan来具体表示，而这一篇博客要讲的就是把OptimizedPlan转化为PhysicalPlan，其实就是把sql语句转化为可以直接操作真实数据的操作及数据和RDD的绑定。下图用来表示这个过程：

![Screen Shot 2016-12-19 at 7.01.48 PM.png](http://upload-images.jianshu.io/upload_images/3736220-c31c2a0156a11a21.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

### SparkPlanner
这里直接处理的是从优化器优化得到的optimizedPlan，将其转化为PhysicalPlan：
```
lazy val sparkPlan: SparkPlan = {
    SQLContext.setActive(sqlContext)
    sqlContext.planner.plan(optimizedPlan).next()
  }

```
SparkPlanner的apply方法，会返回一个Iterator[PhysicalPlan]。SparkPlanner继承了SparkStrategies，SparkStrategies继承了QueryPlanner。  SparkStrategies包含了一系列特定的Strategies，这些Strategies是继承自QueryPlanner中定义的Strategy，它定义接受一个Logical Plan，生成一系列的Physical Plan。

```
class SparkPlanner(val sqlContext: SQLContext) extends SparkStrategies {
  val sparkContext: SparkContext = sqlContext.sparkContext
  1. 指定运行时的partitions的个数
  def numPartitions: Int = sqlContext.conf.numShufflePartitions 
  2. 需要执行的策略
  def strategies: Seq[Strategy] =
    sqlContext.experimental.extraStrategies ++ (
      DataSourceStrategy ::
      DDLStrategy ::
      TakeOrderedAndProject ::
      Aggregation ::
      LeftSemiJoin ::
      EquiJoinSelection ::
      InMemoryScans ::
      BasicOperators ::
      BroadcastNestedLoop ::
      CartesianProduct ::
      DefaultJoin :: Nil)
      ......
}
```
QueryPlanner其实需要介绍的就是它实现的那个plan方法，这里大致我们也看一下它这个类：


```
abstract class QueryPlanner[PhysicalPlan <: TreeNode[PhysicalPlan]] {
  /** A list of execution strategies that can be used by the planner */
  def strategies: Seq[GenericStrategy[PhysicalPlan]]

  /**
   * Returns a placeholder for a physical plan that executes `plan`. This placeholder will be
   * filled in automatically by the QueryPlanner using the other execution strategies that are
   * available.
   */
  protected def planLater(plan: LogicalPlan): PhysicalPlan = this.plan(plan).next()

  def plan(plan: LogicalPlan): Iterator[PhysicalPlan] = {
    // Obviously a lot to do here still...
    val iter = strategies.view.flatMap(_(plan)).toIterator
    assert(iter.hasNext, s"No plan for $plan")
    iter
  }
}
```

### SparkPlanner 关键流程

由上面可以得知SparkPlanner可以提供的策略包括11种，下面我详细介绍其中的几种极其关键执行流程，再介绍这部分前，先举个例子：


```
val df = hc.read.json("/Users/chenkevin/spark-1.6.0/examples/src/main/resources/people.json")
    //下面的操作相当于select * from table

    df.registerTempTable("src")
    val query = hc.sql("select * from src where age > 10")

```
其中people.json中的内容为：
```
{"name":"Michael"}
{"name":"Andy", "age":30}
{"name":"Justin", "age":19}
```
它的logical plan是：
```
Project [age#0L,name#1]
+- Filter (age#0L > 10)
   +- Relation[age#0L,name#1] JSONRelation
```

这里它是会根据strategies依次调用每个strategy的apply方法。
### DataSourceStrategy

这里先看第一个策略DataSourceStrategy，这里我就直接根据本博客的例子来分析，这里我就给出这个策略中的apply的关键部分：

```
 def apply(plan: LogicalPlan): Seq[execution.SparkPlan] = plan match {
    
     ......
     
    // Scanning non-partitioned HadoopFsRelation
    case PhysicalOperation(projects, filters, l @ LogicalRelation(t: HadoopFsRelation, _)) =>
      // See buildPartitionedTableScan for the reason that we need to create a shard
      // broadcast HadoopConf.
      val sharedHadoopConf = SparkHadoopUtil.get.conf
      val confBroadcast =
        t.sqlContext.sparkContext.broadcast(new SerializableConfiguration(sharedHadoopConf))
      1. 这里最关键的是这个函数，它的作用就是再修剪Optimized LogicalPlan，并和datasource（mysql，json文件，Parquet文件，这里是json文件）绑定。
这里它修剪的就是project和filter。
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, f) => t.buildInternalScan(a.map(_.name).toArray, f, t.paths, confBroadcast)) :: Nil
   
      ...... 

}
``` 
下面直接看这个pruneFilterProject：


```
  // Based on Public API.
  protected def pruneFilterProject(
      relation: LogicalRelation,
      projects: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      scanBuilder: (Seq[Attribute], Array[Filter]) => RDD[InternalRow]) = {
    pruneFilterProjectRaw(
      relation,
      projects,
      filterPredicates,
      (requestedColumns, _, pushedFilters) => {
        scanBuilder(requestedColumns, pushedFilters.toArray)
      })
  }
  
```
这里它直接调用的就是pruneFilterProjectRaw方法下面看这个方法做了什么：



```
  // Based on Catalyst expressions. The `scanBuilder` function accepts three arguments:
  //
  //  1. A `Seq[Attribute]`, containing all required column attributes. Used to handle relation
  //     traits that support column pruning (e.g. `PrunedScan` and `PrunedFilteredScan`).
  //
  //  2. A `Seq[Expression]`, containing all gathered Catalyst filter expressions, only used for
  //     `CatalystScan`.
  //
  //  3. A `Seq[Filter]`, containing all data source `Filter`s that are converted from (possibly a
  //     subset of) Catalyst filter expressions and can be handled by `relation`.  Used to handle
  //     relation traits (`CatalystScan` excluded) that support filter push-down (e.g.
  //     `PrunedFilteredScan` and `HadoopFsRelation`).
  //
  // Note that 2 and 3 shouldn't be used together.
  protected def pruneFilterProjectRaw(
    relation: LogicalRelation,
    projects: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    scanBuilder: (Seq[Attribute], Seq[Expression], Seq[Filter]) => RDD[InternalRow]) = {
    1.这里获得的这个projectSet，就是Optimized LogicalPlan中project的参数，即age#0L,name#1
    val projectSet = AttributeSet(projects.flatMap(_.references))
    2. 这里获取的就是过滤条件的参数，在这个例子中是age#0L
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))
   3. 过滤出谓词的如between，like，在这个例子中是age#0L>0，
   4. 所谓谓词，就是取值为 TRUE、FALSE 或 UNKNOWN 的表达式。谓词用于 WHERE子句和 HAVING子句的搜索条件中，还用于 FROM 子句的联接条件以及需要布尔值的其他构造中。
    val candidatePredicates = filterPredicates.map { _ transform {
      case a: AttributeReference => relation.attributeMap(a) // Match original case of attributes.
    }}
    4. 提取出没有被处理的处理的表达式，及其可转化的pushdown谓词。这里是（(age#0L > 10)，GreaterThan(age,10)）
    val (unhandledPredicates, pushedFilters) =
      selectFilters(relation.relation, candidatePredicates)

    // A set of column attributes that are only referenced by pushed down filters.  We can eliminate
    // them from requested columns.
    5. 提取出仅用于pushdown的参数，这里是nil，因为它不需要再pushdown
    val handledSet = {
      val handledPredicates = filterPredicates.filterNot(unhandledPredicates.contains)
      val unhandledSet = AttributeSet(unhandledPredicates.flatMap(_.references))
      AttributeSet(handledPredicates.flatMap(_.references)) --
        (projectSet ++ unhandledSet).map(relation.attributeMap)
    }

    // Combines all Catalyst filter `Expression`s that are either not convertible to data source
    // `Filter`s or cannot be handled by `relation`.
   6. 合并不能转化为数据源中的表达式及没有被处理的filter，这里是Some((age#0L > 10))
    val filterCondition = unhandledPredicates.reduceLeftOption(expressions.And)
    7. 获取元数据信息
    val metadata: Map[String, String] = {
      val pairs = ArrayBuffer.empty[(String, String)]

      if (pushedFilters.nonEmpty) {
        pairs += (PUSHED_FILTERS -> pushedFilters.mkString("[", ", ", "]"))
      }
     8. 这里因为我们使用的是json文件，所以会获取它的路径。
      relation.relation match {
        case r: HadoopFsRelation => pairs += INPUT_PATHS -> r.paths.mkString(", ")
        case _ =>
      }

      pairs.toMap
    }

    if (projects.map(_.toAttribute) == projects &&
        projectSet.size == projects.size &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      9. 当这个project的参数，足以来处理filter的条件，就直接，构建这个scan。
      10. 提取这些满足条件的参数，也就是列名
      val requestedColumns = projects
        // Safe due to if above.
        .asInstanceOf[Seq[Attribute]]
        // Match original case of attributes.
        .map(relation.attributeMap)
        // Don't request columns that are only referenced by pushed filters.
        .filterNot(handledSet.contains)
      11. 创建这个scan，其实就是创建一个PhysicalRDD，而这个PhysicalRDD是间接继承自SparkPlan
      12. 另外这里也创建了一个真正的rdd，可以通过PhysicalRDD获取
      val scan = execution.PhysicalRDD.createFromDataSource(
        projects.map(_.toAttribute),
        scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
        relation.relation, metadata)
      filterCondition.map(execution.Filter(_, scan)).getOrElse(scan)
    } else {
      // Don't request columns that are only referenced by pushed filters.
      13. 这里就是去掉仅仅用于pushdown的fileter，用于计算的还是要保留的。
      val requestedColumns =
        (projectSet ++ filterSet -- handledSet).map(relation.attributeMap).toSeq

      val scan = execution.PhysicalRDD.createFromDataSource(
        requestedColumns,
        scanBuilder(requestedColumns, candidatePredicates, pushedFilters),
        relation.relation, metadata)
      execution.Project(
        projects, filterCondition.map(execution.Filter(_, scan)).getOrElse(scan))
    }
  }
    
```
### 创建RDD
它是怎么创建RDD的呢？也就是在创建scan的时候，传入的那个方法参数scanBuilder，我们先定位到最初匹配的位置：

```
  1. 这里匹配的关系是 HadoopFsRelation
  case PhysicalOperation(projects, filters, l @ LogicalRelation(t: HadoopFsRelation, _)) =>
      // See buildPartitionedTableScan for the reason that we need to create a shard
      // broadcast HadoopConf.
      val sharedHadoopConf = SparkHadoopUtil.get.conf
      val confBroadcast =
        t.sqlContext.sparkContext.broadcast(new SerializableConfiguration(sharedHadoopConf))
       2. 最终是通过buildInternalScan方法来实现的
      pruneFilterProject(
        l,
        projects,
        filters,
        (a, f) => t.buildInternalScan(a.map(_.name).toArray, f, t.paths, confBroadcast)) :: Nil
    
```
下面我们就定位到这个方法中，这个方法位于org.apache.spark.sql.sources.interfaces



```
  final private[sql] def buildInternalScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[String],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[InternalRow] = {
    1. 根据上面的处理中得到的inputPaths，来获取数据的位置。
    2. 他先认为这个路径是一个目录，读取该目录下的文件进行处理
    3. 而后才会处理它是是一个文件
    val inputStatuses = inputPaths.flatMap { input =>
      val path = new Path(input)

      // First assumes `input` is a directory path, and tries to get all files contained in it.
      fileStatusCache.leafDirToChildrenFiles.getOrElse(
        path,
        // Otherwise, `input` might be a file path
        fileStatusCache.leafFiles.get(path).toArray
      ).filter { status =>
        val name = status.getPath.getName
        !name.startsWith("_") && !name.startsWith(".")
      }
    }
    2. 然后就是通过这个文件来构建rdd
    buildInternalScan(requiredColumns, filters, inputStatuses, broadcastedConf)
  }    
```
上面的buildInternalScan方法是使用的JSONRelation中的buildInternalScan方法：


```
override private[sql] def buildInternalScan(
      requiredColumns: Array[String],
      filters: Array[Filter],
      inputPaths: Array[FileStatus],
      broadcastedConf: Broadcast[SerializableConfiguration]): RDD[InternalRow] = {
     1. 获取需要处理的列的Schema信息，其实就是其对应的类型。
    val requiredDataSchema = StructType(requiredColumns.map(dataSchema(_)))
    2. 这里就是根据requiredDataSchema将json格式的数据转化为InternalRow
    val rows = JacksonParser.parse(
      inputRDD.getOrElse(createBaseRdd(inputPaths)),
      requiredDataSchema,
      sqlContext.conf.columnNameOfCorruptRecord,
      options)
    2. 最后将rdd中InternalRow转化为UnsafeRow，因为UnsafeRow的数据是直接放在内存上的，而不是java对象
    rows.mapPartitions { iterator =>
      val unsafeProjection = UnsafeProjection.create(requiredDataSchema)
      iterator.map(unsafeProjection)
    }
  }
  
```
下面再看一下这个createBaseRdd
```
 private def createBaseRdd(inputPaths: Array[FileStatus]): RDD[String] = {
    val job = new Job(sqlContext.sparkContext.hadoopConfiguration)
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)

    val paths = inputPaths.map(_.getPath)

    if (paths.nonEmpty) {
      FileInputFormat.setInputPaths(job, paths: _*)
    }
    3. 聪明的你到这里肯定又明白了，它这里是直接通过spark core来构建RDD，那么以后的操作肯定也是基于spark core，所以spark sql就是在spark core的基础上增加了一个可以处理sql语句的功能
    sqlContext.sparkContext.hadoopRDD(
      conf.asInstanceOf[JobConf],
      classOf[TextInputFormat],
      classOf[LongWritable],
      classOf[Text]).map(_._2.toString) // get the text line
  }  
```
下面要做的就是操作这个rdd了，下面看我们的例子，在BasicOperators策略时，他也会操作上面处理过的plan，仅对我们的这个例子做匹配，至于别的大家自己去看。

```

object BasicOperators extends Strategy {
    def numPartitions: Int = self.numPartitions

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      ......
            case logical.Limit(IntegerLiteral(limit), child) =>
        execution.Limit(limit, planLater(child)) :: Nil
        
      ......
}
```
接着就看这个Limit的类：

```

/**
 * Take the first limit elements. Note that the implementation is different depending on whether
 * this is a terminal operator or not. If it is terminal and is invoked using executeCollect,
 * this operator uses something similar to Spark's take method on the Spark driver. If it is not
 * terminal or is invoked using execute, we first take the limit on each partition, and then
 * repartition all the data to a single partition to compute the global limit.
 */
1. 上面的注释就是说我们的具体执行，是要直接在单独的driver上，还是在集群上
case class Limit(limit: Int, child: SparkPlan)
  extends UnaryNode {
  // TODO: Implement a partition local limit, and use a strategy to generate the proper limit plan:
  // partition local limit -> exchange into one partition -> partition local limit again

  /** We must copy rows when sort based shuffle is on */
  1. 这里指定是否使用sort based shuffle
  private def sortBasedShuffleOn = SparkEnv.get.shuffleManager.isInstanceOf[SortShuffleManager]
 2. 输出的参数
  override def output: Seq[Attribute] = child.output
   override def outputPartitioning: Partitioning = SinglePartition
 3. 如果你使用这个方法的话，它仅仅在driver上执行，直接返回结果
  override def executeCollect(): Array[InternalRow] = child.executeTake(limit)
 4. 下面就是具体的执行了
  protected override def doExecute(): RDD[InternalRow] = {
    val rdd: RDD[_ <: Product2[Boolean, InternalRow]] = if (sortBasedShuffleOn) {
      child.execute().mapPartitionsInternal { iter =>
        iter.take(limit).map(row => (false, row.copy()))
      }
    } else {
      child.execute().mapPartitionsInternal { iter =>
        val mutablePair = new MutablePair[Boolean, InternalRow]()
        iter.take(limit).map(row => mutablePair.update(false, row))
      }
    }
    val part = new HashPartitioner(1)
    val shuffled = new ShuffledRDD[Boolean, InternalRow, InternalRow](rdd, part)
    shuffled.setSerializer(new SparkSqlSerializer(child.sqlContext.sparkContext.getConf))
    shuffled.mapPartitionsInternal(_.take(limit).map(_._2))
  }
}

```
同志们好好看一下这个doExecute，你觉得他合理吗？当然不合理，它的它的partitioner的并行度为“1”，这在生产环境下一般情况下都不是这样的，那要怎么改呢？这个策略下是由提供这个方法的如BasicOperators中匹配到的logical.Repartition方法来实现的，关于这个方法，在真正执行的时候再谈，因为它涉及到接下来要讲的一个关键步骤prepareForExecution，我们将在下篇博客中详细介绍，而后在来分析业务的真正执行。

关于这个过程中其余部分的策略，大家自己分析吧，也是这种调调，只不过是不同的操作而已。

### 参考资料
1. Spark 1.6.3源码
2. https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf
3. http://infolab.stanford.edu/~hyunjung/cs346/qpnotes.html
4. http://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf
5. http://blog.hydronitrogen.com/2016/02/22/in-the-code-spark-sql-query-planning-and-execution/#Exchange
6. http://blog.csdn.net/oopsoom/article/details/38257749
7. http://sqlblog.com/blogs/paul_white/archive/2012/04/28/query-optimizer-deep-dive-part-1.aspx
