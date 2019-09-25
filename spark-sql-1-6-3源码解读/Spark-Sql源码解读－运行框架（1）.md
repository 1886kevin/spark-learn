在介绍 sparkSQL 之前,我们首先来看看,传统的关系型数据库是怎么运行的。当我们提交了一个很简单的查询:
```
SELECT a1,a2,a3 FROM tableA Where condition
```

![Screen Shot 2016-12-16 at 6.49.36 PM.png](http://upload-images.jianshu.io/upload_images/3736220-38c1a39b9f9a7e01.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

可以看得出来,该语句是由 Projection(a1,a2,a3)、Data Source(tableA)、Filter(condition)组成,分别对应 sql 查询过程中的 Result、Data Source、Operation,也就是说 SQL 语句按 Result-->Data Source-->Operation 的次序来描述的。那么,SQL 语句在实际的运行过程中是怎么处理的呢?一般的数据库系统先将读入的 SQL 语句(Query)先进行解析(Parse),分辨出 SQL 语句中哪些词是关键词(如 SELECT、FROM、WHERE),哪些是表达式、哪些是 Projection、哪些是 Data Source 等等。这一步就可以判断 SQL 语句是否规范,不规范就报错,规范就继续下一步过程绑定(Bind),这个过程将 SQL 语句和数据库的数据字典(列、表、视图等等)进行绑定,如果相关的 Projection、Data Source等等都是存在的话,就表示这个 SQL 语句是可以执行的;而在执行前,一般的数据库会提供几个执行计划,这些计划一般都有运行统计数据,数据库会在这些计划中选择一个最优计划(Optimize),最终执行该计划(Execute),并返回结果。当然在实际的执行过程中,是按 Operation-->Data Source-->Result 的次序来进行的,和 SQL 语句的次序刚好相反;在执行过程有时候甚至不需要读取物理表就可以返回结果,比如重新运行刚运行过的 SQL语句,可能直接从数据库的缓冲池中获取返回结果。
以上过程看上去非常简单,但实际上会包含很多复杂的操作细节在里面。而这些操作细节都和 Tree 有关,在数据库解析(Parse)SQL 语句的时候,会将 SQL 语句转换成一个树型结构来进行处理,如下面一个查询,会形成一个含有多个节点(TreeNode)的 Tree,然后在后续的处理过程中对该 Tree 进行一系列的操作。

![Screen Shot 2016-12-16 at 6.52.30 PM.png](http://upload-images.jianshu.io/upload_images/3736220-21e9332564067474.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

下图给出了对 Tree 的一些可能的操作细节,对于 Tree 的处理过程中所涉及更多的细节,可以查看相关的数据库论文。

![Screen Shot 2016-12-16 at 9.39.15 PM.png](http://upload-images.jianshu.io/upload_images/3736220-048552f251ce794d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

OK,上面简单介绍了关系型数据库的运行过程,那么,sparkSQL 是不是也采用类似的方式处理呢?答案是肯定的。下面我们先来看看 sparkSQL 中的两个重要概念 Tree 和 Rule、然后在看看sparkSQL 的优化器 Catalyst。

### 1.1 Tree 和 Rule

sparkSQL 对 SQL 语句的处理和关系型数据库对 SQL 语句的处理采用了类似的方法,首先会将 SQL 语句进行解析(Parse),然后形成一个 Tree,在后续的如绑定、优化等处理过程都是对 Tree 的操作,而操作的方法是采用 Rule,通过模式匹配,对不同类型的节点采用不同的操作。

#### 1.1.1 Tree

* Tree 的相关代码定义在sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/trees
* Logical Plans、Expressions、Physical Operators 都可以使用 Tree 表示
* Tree 的具体操作是通过 TreeNode 来实现的
* sparkSQL 定义了 catalyst.trees 的日志,通过这个日志可以形象的表示出树的结构
* TreeNode 可以使用 scala 的集合操作方法(如 foreach, map, flatMap,collect 等)进行操作
* 有了 TreeNode,通过 Tree 中各个 TreeNode 之间的关系,可以对 Tree 进行遍历操作,如使用 transformDown、transformUp 将 Rule 应用到给定的树段,然后用结果替代旧的树段;也可以使用 transformChildrenDown、transformChildrenUp 对一个给定的节点进行操作,通过迭代将 Rule 应用到该节点以及子节点。

```
/**
   * Returns a copy of this node where `rule` has been recursively applied to the tree.
   * When `rule` does not apply to a given node it is left unchanged.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformDown or transformUp should be used.
   * @param rule the function use to transform this nodes children
   */
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDown(rule)
  }
```

* TreeNode 可以细分成三种类型的 Node:
   1. UnaryNode 一元节点,即只有一个子节点。如 Limit、Filter 操作
   2. BinaryNode 二元节点,即有左右子节点的二叉节点。如 Jion、Union 操作
   3. LeafNode 叶子节点,没有子节点的节点。主要用户命令类操作,如SetCommand.

```
/**
 * A logical plan node with no children.
 */
abstract class LeafNode extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Nil
}
/**
 * A logical plan node with single child.
 */
abstract class UnaryNode extends LogicalPlan {
  def child: LogicalPlan
  override def children: Seq[LogicalPlan] = child :: Nil
}
/**
 * A logical plan node with a left and right child.
 */
abstract class BinaryNode extends LogicalPlan {
  def left: LogicalPlan
  def right: LogicalPlan
  override def children: Seq[LogicalPlan] = Seq(left, right)
}
```
#### 1.1.2 Rule

* Rule 的相关代码定义在sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/rules
* Rule 在 sparkSQL 的 Analyzer、Optimizer、SparkPlan 等各个组件中都有应用到
* Rule 是一个抽象类,具体的 Rule 实现是通过RuleExecutor 完成
* Rule 通过定义 batch 和 batchs,可以简便的、模块化地对 Tree 进行 transform 操作
* Rule 通过定义 Once 和 FixedPoint,可以对 Tree 进行一次操作或多次操作(如对某些 Tree 进行多次迭代操作的时候,达到 FixedPoint 次数迭代或达到前后两次的树结构没变化才停止操作,具体参看 RuleExecutor.execute)

```
abstract class RuleExecutor[TreeType <: TreeNode[_]] extends Logging {

  /**
   * An execution strategy for rules that indicates the maximum number of executions. If the
   * execution reaches fix point (i.e. converge) before maxIterations, it will stop.
   */
  abstract class Strategy { def maxIterations: Int }

  /** A strategy that only runs once. */
  case object Once extends Strategy { val maxIterations = 1 }

  /** A strategy that runs until fix point or maxIterations times, whichever comes first. */
  case class FixedPoint(maxIterations: Int) extends Strategy

  /** A batch of rules. */
  protected case class Batch(name: String, strategy: Strategy, rules: Rule[TreeType]*)

  /** Defines a sequence of rule batches, to be overridden by the implementation. */
  protected val batches: Seq[Batch]


  /**
   * Executes the batches of rules defined by the subclass. The batches are executed serially
   * using the defined execution strategy. Within each batch, rules are also executed serially.
   */
  def execute(plan: TreeType): TreeType = {
     
     ......
     
  }
}
```
拿个简单的例子,在处理由解析器(SqlParse)生成的 LogicPlan Tree 的时候,在Analyzer 中就定义了多种 Rules 应用到 LogicPlan Tree 上。

应用示意图:


![Screen Shot 2016-12-16 at 10.32.04 PM.png](http://upload-images.jianshu.io/upload_images/3736220-3d26127c45880810.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Analyzer 中使用的 Rules,定义了 batches,由多个 batch 构成,如Substitution、Resolution、Nondeterministic、UDF（关于UDF，是后来才引进的一个功能，在spark 1.1.1的时候是没有的）、Cleanup 等构成;每个batch又有不同的rule构成,如Resolution由ResolveReferences 、ResolveRelations、ResolveGroupingAnalytics、ResolveSortReferences 等构成;每个 rule 又有自己相对应的处理函数,可以具体参看 Analyzer 中的 ResolveReferences 、ResolveRelations、ResolveSortReferences 、ResolveGroupingAnalytics 函数;同时要注意的是,不同的 rule应用次数是不同的:如 Nondeterministic 这个 batch 中 rule 只应用了一次(Once),而 Resolution 这个 batch 中的 rule 应用了多次(fixedPoint =FixedPoint(100),也就是说最多应用 100 次,除非前后迭代结果一致退出)。如Analyzer.scala中的Batches：
```
lazy val batches: Seq[Batch] = Seq(
    Batch("Substitution", fixedPoint,
      CTESubstitution,
      WindowsSubstitution),
    Batch("Resolution", fixedPoint,
      ResolveRelations ::
      ResolveReferences ::
      ResolveGroupingAnalytics ::
      ResolvePivot ::
      ResolveUpCast ::
      ResolveSortReferences ::
      ResolveGenerate ::
      ResolveFunctions ::
      ResolveAliases ::
      ExtractWindowExpressions ::
      GlobalAggregates ::
      ResolveAggregateFunctions ::
      HiveTypeCoercion.typeCoercionRules ++
      extendedResolutionRules : _*),
    Batch("Nondeterministic", Once,
      PullOutNondeterministic,
      ComputeCurrentTime),
    Batch("UDF", Once,
      HandleNullInputsForUDF),
    Batch("Cleanup", fixedPoint,
      CleanupAliases)
  )

```
在整个 sql 语句的处理过程中,Tree 和 Rule 相互配合,完成了解析、绑定(在 sparkSQL中称为 Analysis)、优化、物理计划等过程,最终生成可以执行的物理计划。知道了 sparkSQL 的各个过程的基本处理方式,下面来看看 sparkSQL 的运行过程。

### 1.2 sql语句处理流程
Spark Sql 处理sql语句的大致流程为：

1. SQL 语句经过 SqlParser 解析成 Unresolved LogicalPlan;
2. 使用 analyzer 结合数据数据字典(catalog)进行绑定,生成 resolved LogicalPlan;
3. 使用 optimizer 对 resolved LogicalPlan 进行优化,生成 optimized LogicalPlan;
4. 使用 SparkPlan 将 LogicalPlan 转换成 PhysicalPlan;
5. 使用 prepareForExecution()将 PhysicalPlan 转换成可执行物理计划;
6. 使用 execute()执行可执行物理计划;
7. 生成 RDD。

在整个运行过程中涉及到多个 sparkSQL 的组件,如 SqlParser、analyzer、optimizer、SparkPlan 等等,其功能和实现在下一篇博客中详解。

![Screen Shot 2016-12-16 at 10.52.04 PM.png](http://upload-images.jianshu.io/upload_images/3736220-a0f67934adbdca48.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

OK，好了，Spark Sql我没有直接将代码的原因是，我觉得上面这些理论大家先掌握了，然后再看代码比较好一些。否则，你会觉得有好多地方难以理解。

### 参考资料
1. Spark 1.6.3源码
2. https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf
3. http://infolab.stanford.edu/~hyunjung/cs346/qpnotes.html
4. http://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf
5. http://blog.hydronitrogen.com/2016/02/22/in-the-code-spark-sql-query-planning-and-execution/#Exchange
6. http://blog.csdn.net/oopsoom/article/details/38257749
7. http://sqlblog.com/blogs/paul_white/archive/2012/04/28/query-optimizer-deep-dive-part-1.aspx
