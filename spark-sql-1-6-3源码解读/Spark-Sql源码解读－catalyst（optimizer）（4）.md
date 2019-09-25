前面讲了Spark Sql的运行框架，也讲了一下catalyst的整体的运行流程，然后又详细讲解了Sql语句的转换为Unresolved LogicalPlan的详细步骤。接着讲解catalyst的Analyzer，就是用来将Unresolved LogicalPlan转化为Resolved LogicalPlan的。下面讲解优化器optimizer。而优化器是用来将Resolved LogicalPlan转化为optimized LogicalPlan的。
下面先看一下QueryExecution中执行优化的代码：

```
  1.这里的这个withCachedData就是上篇博客处理完后的logical plan
  lazy val optimizedPlan: LogicalPlan = sqlContext.optimizer.execute(withCachedData)

```
接着看这个优化器，这时候你会惊喜的发现和上篇博客中Analyzer的结构基本上是一摸一样的：

```
 abstract class Optimizer(conf: CatalystConf) extends RuleExecutor[LogicalPlan] {
  val batches =
    // SubQueries are only needed for analysis and can be removed before execution.
    1. 删除子查询的处理
    Batch("Remove SubQueries", FixedPoint(100),
      EliminateSubQueries) ::
    2. 合并的处理
    Batch("Aggregate", FixedPoint(100),
      2.1 重写了包含有Distinct的合成操作
      DistinctAggregationRewriter(conf),
      2.2 用agg操作代替distinct操作
      ReplaceDistinctWithAggregate,
      2.3 去除一些group操作中foldable的表达式，如Coalesce，IsNull，如果他们的foldable为true的话，可以去除。
      RemoveLiteralFromGroupExpressions) ::
    Batch("Operator Optimizations", FixedPoint(100),
      // Operator push down
      SetOperationPushDown,
      把一个Project下推到Sample中
      SamplePushDown,
      3. 谓词下推
      PushPredicateThroughJoin,
      PushPredicateThroughProject,
      PushPredicateThroughGenerate,
      PushPredicateThroughAggregate,
      4. 列剪枝
      ColumnPruning,
      // Operator combine
     5. 操作的合并
      ProjectCollapsing,
      CombineFilters,
      CombineLimits,
      // Constant folding
      6.1 空格处理
      NullPropagation,
      6.2 关键字in的优化，替代为InSet
      OptimizeIn,
      6.3 常量叠加
      ConstantFolding,
      6.4 表达式的简化
      LikeSimplification,
      BooleanSimplification,
      RemoveDispensableExpressions,
      SimplifyFilters,
      SimplifyCasts,
      SimplifyCaseConversionExpressions) ::
     7. 精度优化
    Batch("Decimal Optimizations", FixedPoint(100),
      DecimalAggregates) ::
     8. 关系的转化
    Batch("LocalRelation", FixedPoint(100),
      ConvertToLocalRelation) :: Nil
}
case class DefaultOptimizer(conf: CatalystConf) extends Optimizer(conf)
```
如果要理解这些的话，就需要牛X的sql优化经验了，这个如果大家有兴趣的话，可以去看一下sql优化方面的[材料](http://sqlblog.com/blogs/paul_white/archive/2012/04/28/query-optimizer-deep-dive-part-1.aspx)，这里我就不加以解释了。因为这个如果要在写的话，又是另一个专题了。这里我只举一个例子说明一下一个重要的方法transform
```
object ReplaceDistinctWithAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Distinct(child) => Aggregate(child.output, child.output, child)
  }
}
  
```
因为在优化器下所有的rule都是通过transform来实现的。下面我们根据上面的rule就看一这个方法：
```
  def transform(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    transformDown(rule)
  }
  
```
接着它调用了transformDown方法：

```
  def transformDown(rule: PartialFunction[BaseType, BaseType]): BaseType = {
    1. 这里注意它是先处理的跟节点，所以说它是pre－order的
    val afterRule = CurrentOrigin.withOrigin(origin) {
      rule.applyOrElse(this, identity[BaseType])
    }

    // Check if unchanged and then possibly return old copy to avoid gc churn.
    if (this fastEquals afterRule) {
     2. 这里有意思的地方，如果没有改变的话，它就直接返回rule，而不是每一次都会有产生一个afterRule，从而最终有可能导致gc
      transformChildren(rule, (t, r) => t.transformDown(r))
    } else {
      3. 如果改变了就处理它的子节点
      afterRule.transformChildren(rule, (t, r) => t.transformDown(r))
    }
  }  
```
所以从这里你可以看出这里它又是使用先序便利的方式来实现的。看明白了这个transform方法相信你对别的rule的处理应该没什么问题。当然如果你想过多的了解更多的rule的用法，可以看一下这篇[博客](http://blog.csdn.net/oopsoom/article/details/38121259).这里有一些更多的介绍，只不过它是基于spark 1.1.x的。它这里面多少有介绍一些常用的rule。

这里因为优化器和分析器最终都是调用RuleExecutor的execute方法，过程都是一样的，所以这里我就没有过多的分析。

### 参考资料
1. Spark 1.6.3源码
2. https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf
3. http://infolab.stanford.edu/~hyunjung/cs346/qpnotes.html
4. http://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf
5. http://blog.hydronitrogen.com/2016/02/22/in-the-code-spark-sql-query-planning-and-execution/#Exchange
6. http://blog.csdn.net/oopsoom/article/details/38257749
7. http://sqlblog.com/blogs/paul_white/archive/2012/04/28/query-optimizer-deep-dive-part-1.aspx
