前面讲了Spark Sql的运行框架，也讲了一下catalyst的整体的运行流程，然后又详细讲解了Sql语句的转换为Unresolved LogicalPlan的详细步骤。下面接着讲解catalyst的Analyzer，所谓Analyzer，就是用来将Unresolved LogicalPlan转化为Resolved LogicalPlan的。处理图如下：

![Screen Shot 2016-12-17 at 5.58.51 PM.png](http://upload-images.jianshu.io/upload_images/3736220-2b7a13975c599feb.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

### Analyzer
Analyzer 的功能就是对来自 SqlParser 的 Unresolved LogicalPlan 中的UnresolvedAlias 项和 UnresolvedRelation 项,对照 catalog 和 FunctionRegistry生成Resolved LogicalPlan。下面进行分析之前，先看一下Analyzer的核心代码。

```
class Analyzer(
    catalog: Catalog,
    registry: FunctionRegistry,
    conf: CatalystConf,
    maxIterations: Int = 100)
  extends RuleExecutor[LogicalPlan] with CheckAnalysis {

  ....
  1.     FixedPoint：相当于迭代次数的上限，默认为100。
  val fixedPoint = FixedPoint(maxIterations)

  2.      Batch: 批次，这个对象是由一系列Rule组成的，采用一个策略（策略其实是迭代几次的别名，现包含两种：Once和FixedPoint）
  3.     Rule：理解为一种规则，这种规则会应用到Logical Plan 从而将UnResolved 转变为Resolved，在Batch中的第三个参数就是Rule，下面就是一些实现的Rule。
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
  ......
}

```
Analyzer解析主要是根据这些Batch里面定义的策略和Rule来对Unresolved的逻辑计划进行解析的。而对于真正的处理操作，其实是在Analyzer的继承类RuleExecutor的execute来实现的下面就讲这些具体的过程：
首先我先把这个execute的调用流程写出来:
```
 1     
 val query = sql("SELECT * FROM src")
 2  
 def sql(sqlText: String): DataFrame = {
     这里我们已经获得了Unresolved LogicalPlan
     DataFrame(this, parseSql(sqlText))
  }
 3
 private[sql] object DataFrame {
  def apply(sqlContext: SQLContext, logicalPlan: LogicalPlan): DataFrame = {
    new DataFrame(sqlContext, logicalPlan)
  }
}
 4
  def this(sqlContext: SQLContext, logicalPlan: LogicalPlan) = {
    this(sqlContext, {
      val qe = sqlContext.executePlan(logicalPlan)
      if (sqlContext.conf.dataFrameEagerAnalysis) {
        qe.assertAnalyzed()  // This should force analysis and throw errors if there are any
      }
      qe
    })
  }
 5
 protected[sql] def executePlan(plan: LogicalPlan) =
    new sparkexecution.QueryExecution(this, plan) 

 6 最后在QueryExecution这个类的初始化的时候，使用了execute这个方法：
 class QueryExecution(val sqlContext: SQLContext, val logical: LogicalPlan) {

  ......

  lazy val analyzed: LogicalPlan = sqlContext.analyzer.execute(logical)
  
  ......
}
 
```
###  sqlContext.analyzer.execute
下面我们就详细讲解这个部分：
```
/**
   * Executes the batches of rules defined by the subclass. The batches are executed serially
   * using the defined execution strategy. Within each batch, rules are also executed serially.
   */
  1. 先说这段注释，执行被这个类的子类（Analyzer）定义的batches（也就上文中提到的那些batches），batches中的batch会一个接一个的被调用
  2. 使用batch中定义的策略（Once，FixedPoint），rules中的rule也会一个接一个的被调用
  def execute(plan: TreeType): TreeType = {
    var curPlan = plan

    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        2.下面先看这个foldLeft方法
        curPlan = batch.rules.foldLeft(curPlan) {
          case (plan, rule) =>
            val startTime = System.nanoTime()
            val result = rule(plan)
            val runTime = System.nanoTime() - startTime
            RuleExecutor.timeMap.addAndGet(rule.ruleName, runTime)

            if (!result.fastEquals(plan)) {
              logTrace(
                s"""
                  |=== Applying Rule ${rule.ruleName} ===
                  |${sideBySide(plan.treeString, result.treeString).mkString("\n")}
                """.stripMargin)
            }

            result
        }
        iteration += 1
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            logInfo(s"Max iterations (${iteration - 1}) reached for batch ${batch.name}")
          }
          continue = false
        }

        if (curPlan.fastEquals(lastPlan)) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }

      if (!batchStartPlan.fastEquals(curPlan)) {
        logDebug(
          s"""
          |=== Result of Batch ${batch.name} ===
          |${sideBySide(plan.treeString, curPlan.treeString).mkString("\n")}
        """.stripMargin)
      } else {
        logTrace(s"Batch ${batch.name} has no effect.")
      }
    }

    curPlan
  }
}
```

foldLeft方法：
```
  
  def foldLeft[B](z: B)(op: (B, A) => B): B = {
    var result = z
    this.seq foreach (x => result = op(result, x))
    result
  }

 
```
由此可见，flodLeft是柯里化的一种实现，而这里的op只得就是上面代码中flodLeft后面大括号中的方法，这个result就是curPlan，而这句代码的最终实现就是依次是用rules中的rule和curPlan来当作传入参数。下面我们在接着分析大括号中的内容：

```
  
  {
          case (plan, rule) =>
            val startTime = System.nanoTime()
            1. 这里很显然他是调用了这个rule的apply方法
            val result = rule(plan)
            val runTime = System.nanoTime() - startTime
            RuleExecutor.timeMap.addAndGet(rule.ruleName, runTime)

            if (!result.fastEquals(plan)) {
              logTrace(
                s"""
                  |=== Applying Rule ${rule.ruleName} ===
                  |${sideBySide(plan.treeString, result.treeString).mkString("\n")}
                """.stripMargin)
            }

            result
  }
 
```
有上篇博客中可以得知通过SqlParser生成的tree，


![Screen Shot 2016-12-17 at 11.50.34 PM.png](http://upload-images.jianshu.io/upload_images/3736220-8323432244f1e29e.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


下面我们看这个逻辑处理的过程，对于不产生tree变化的rule，我这里就直接忽略了，下面是第一个会导致tree变化的rule，

###  ResolveRelations

```
/**
   * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
   */
  1. 从这个注释中可以得出，下面这个rule的作用就是通过table从catalog中获取相应的relation。
  object ResolveRelations extends Rule[LogicalPlan] {
    2. 获取table的relation
    def getTable(u: UnresolvedRelation): LogicalPlan = {
      try {
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"Table not found: ${u.tableName}")
      }
    }
     2. 下面就是应用这个方法来实现，这里的resolveOperators关键字很重要
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case i @ InsertIntoTable(u: UnresolvedRelation, _, _, _, _) =>
        i.copy(table = EliminateSubQueries(getTable(u)))
      case u: UnresolvedRelation =>
        try {
          getTable(u)
        } catch {
          case _: AnalysisException if u.tableIdentifier.database.isDefined =>
            // delay the exception into CheckAnalysis, then it could be resolved as data source.
            u
        }
    }
  }
 
```
下面就看这个resolveOperators方法：

```
/**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.  This function is similar to `transformUp`, but skips sub-trees that have already
   * been marked as analyzed.
   *
   * @param rule the function use to transform this nodes children
   */
  1. 注释的意思就是说，它会优先处理它的子节点，然后再处理自己，其实说白了就是树的递归调用。唯一需要注意的是，他会忽略掉已经处理过的子节点.
  def resolveOperators(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    if (!analyzed) {
      2. 处理它的子节点，这个transformChildren大家自己看一下，其实很简单，就是一个递归调用
      val afterRuleOnChildren = transformChildren(rule, (t, r) => t.resolveOperators(r))
      3. 如果处理后的LogicalPlan和这一次的相等就说明他没有子节点了，则处理它自己
      if (this fastEquals afterRuleOnChildren) {
        CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(this, identity[LogicalPlan])
        }
      } else {
       4.反之则处理它自己 
        CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(afterRuleOnChildren, identity[LogicalPlan])
        }
      }
    } else {
      this
    }
  } 
```
下面我们就看原来那段代码，他是怎么进行处理的：

```
object ResolveRelations extends Rule[LogicalPlan] {
    def getTable(u: UnresolvedRelation): LogicalPlan = {
      try {
        4. 也就是直接从catalog中读取relation信息，关于这个方法我就不说了
        catalog.lookupRelation(u.tableIdentifier, u.alias)
      } catch {
        case _: NoSuchTableException =>
          u.failAnalysis(s"Table not found: ${u.tableName}")
      }
    }
   1. 也就是应用这个apply方法进行处理
    def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      2.插入操作，先不说，自己去看
      case i @ InsertIntoTable(u: UnresolvedRelation, _, _, _, _) =>
        i.copy(table = EliminateSubQueries(getTable(u)))
      3. 然后就是这个getTable操作了
      case u: UnresolvedRelation =>
        try {
          getTable(u)
        } catch {
          case _: AnalysisException if u.tableIdentifier.database.isDefined =>
            // delay the exception into CheckAnalysis, then it could be resolved as data source.
            u
        }
    }
  }

```
下面直接把结果给大家贴出来：
1. 最初的tree：
![Screen Shot 2016-12-18 at 12.42.30 AM.png](http://upload-images.jianshu.io/upload_images/3736220-2fc507d7d51ea388.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
2 经过前面那个rule处理过的tree：
![Screen Shot 2016-12-18 at 12.43.59 AM.png](http://upload-images.jianshu.io/upload_images/3736220-3599076bda8630f7.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
你会看到它这里多了一个metastoreRelation，也就是完成了和这个table的绑定。

###  ResolveReferences

我们就直接看这个类，再写一下我们处理sql语句select * from src，由上面那个rule，也就完成了和table的绑定，下面就应该是这个＊的处理了，关于这个类没有的部分我就不写了，只写出关键代码：

```
object ResolveReferences extends Rule[LogicalPlan] {
  .....
   2. 看到这里你会发现他又使用了这个resolveOperators方法，所以你应该明白后续操作了
  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
      case p: LogicalPlan if !p.childrenResolved => p

      // If the projection list contains Stars, expand it.
      case p @ Project(projectList, child) if containsStar(projectList) =>
        Project(
          projectList.flatMap {
            case s: Star => s.expand(child, resolver)
            1. 后面都是case，因为这里最终只是匹配到了上面这个case，下面的大家自己看
            ......
            
          },child)
          
```
接着直接看这个他匹配到的方法case s: Star => s.expand(child, resolver)，这里他是调用的UnresolvedStar中的expand方法：

```
override def expand(input: LogicalPlan, resolver: Resolver): Seq[NamedExpression] = {

    // First try to expand assuming it is table.*.
    val expandedAttributes: Seq[Attribute] = target match {
      // If there is no table specified, use all input attributes.
      1. 通过前面我们得知这里的这个target其实就是我们上面图中得到的      UnresolvedStar(None)
      2. 所以最终匹配的是这里
      case None => input.output
      // If there is a table, pick out attributes that are part of this table.
      case Some(t) => if (t.size == 1) {
        input.output.filter(_.qualifiers.exists(resolver(_, t.head)))
      } else {
        List()
      }
    }
    if (expandedAttributes.nonEmpty) return expandedAttributes    
    
    ......
    3. 对于后面的代码，他处理的是形如SELECT record.* from (SELECT struct(a,b,c) as record ...)这样的sql语句。      
```
下面我们先看这这个 input.output中的内容

![Screen Shot 2016-12-18 at 1.05.45 AM.png](http://upload-images.jianshu.io/upload_images/3736220-ff4d8a2f5415b9b3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

然后再看下我们的tree的变化：

![Screen Shot 2016-12-18 at 1.06.09 AM.png](http://upload-images.jianshu.io/upload_images/3736220-7c6d0888c432368a.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

### 再谈execute
你会发现确实解析成功了。ok，到这里基本上就大功告成了，但是，我们还要回过头来看一下execute这个方法：

```
def execute(plan: TreeType): TreeType = {
    var curPlan = plan
    1. 遍历每一个batch
    batches.foreach { batch =>
      val batchStartPlan = curPlan
      var iteration = 1
      var lastPlan = curPlan
      var continue = true

      // Run until fix point (or the max number of iterations as specified in the strategy.
      while (continue) {
        2.调用每一个rules中的rule
        curPlan = batch.rules.foldLeft(curPlan) {
          case (plan, rule) =>
            val startTime = System.nanoTime()
            3. 调用rule递归的对整个tree进行处理
            val result = rule(plan)
            val runTime = System.nanoTime() - startTime
            RuleExecutor.timeMap.addAndGet(rule.ruleName, runTime)
            4. 如果处理后的plan有变化，打印相关信息
            if (!result.fastEquals(plan)) {
              logTrace(
                s"""
                  |=== Applying Rule ${rule.ruleName} ===
                  |${sideBySide(plan.treeString, result.treeString).mkString("\n")}
                """.stripMargin)
            }

            result
        }
        5. batch的迭代次数加一
        iteration += 1
        6. 处理次数超过最大值，则停止
        if (iteration > batch.strategy.maxIterations) {
          // Only log if this is a rule that is supposed to run more than once.
          if (iteration != 2) {
            logInfo(s"Max iterations (${iteration - 1}) reached for batch ${batch.name}")
          }
          continue = false
        }
        7. 如果处理后的plan没变化，则停止
        if (curPlan.fastEquals(lastPlan)) {
          logTrace(
            s"Fixed point reached for batch ${batch.name} after ${iteration - 1} iterations.")
          continue = false
        }
        lastPlan = curPlan
      }
      8. 根据处理的plan有没有变化打印相关信息
      if (!batchStartPlan.fastEquals(curPlan)) {
        logDebug(
          s"""
          |=== Result of Batch ${batch.name} ===
          |${sideBySide(plan.treeString, curPlan.treeString).mkString("\n")}
        """.stripMargin)
      } else {
        logTrace(s"Batch ${batch.name} has no effect.")
      }
    }

    curPlan
  }
     
```
ok，到这里整个的分析过程就完成了，其实它这里就完成了一项任务，就是把sql语句和数据库进行绑定。大家一定要明白这个过程，因为接下来的几个组件都是按照这种规则来实现的。

关于它实现的一点畅想：
它这里为什么要用rule来处理plan呢，如果改成plan来匹配rule的方式会不会更好一些？

### 参考资料
1. Spark 1.6.3源码
2. https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf
3. http://infolab.stanford.edu/~hyunjung/cs346/qpnotes.html
4. http://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf
5. http://blog.hydronitrogen.com/2016/02/22/in-the-code-spark-sql-query-planning-and-execution/#Exchange
6. http://blog.csdn.net/oopsoom/article/details/38257749
7. http://sqlblog.com/blogs/paul_white/archive/2012/04/28/query-optimizer-deep-dive-part-1.aspx
