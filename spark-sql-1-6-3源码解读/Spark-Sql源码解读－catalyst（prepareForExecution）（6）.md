首先在开头我先唠叨两句，这个模块是catalyst最难以理解的一个模块，但你又不得不理解，当然这对简单的算法，可能没有必要；但是对稍微复杂一点的操作，如join等，就必须要熟悉这部分的内容，而在工作中，你肯定会遇到join操作等稍微复杂一点的操作，所以这里我们就有必要来掌握这一块的内容。

```
 /**
   * Prepares a planned SparkPlan for execution by inserting shuffle operations and internal
   * row format conversions as needed.
   */
  @transient
  protected[sql] val prepareForExecution = new RuleExecutor[SparkPlan] {
    val batches = Seq(
      1. 这个主要是用来协调shuffle的方法
      Batch("Add exchange", Once, EnsureRequirements(self)),
      2. 这个就是要确保row 的formats，因为处理的row有可能是SafeRows和UnsafeRows，这里需要将处理的记录和相应的格式匹配起来
      Batch("Add row converters", Once, EnsureRowFormats)
    )
  }  
```
这里它是继承自RuleExecutor，所以它的执行，其实也是使用的RuleExecutor的execute来实现的。

###  EnsureRequirements
接下来，我们重点分析EnsureRequirements方法，下面先列出需要使用这个方法来增加Exchange操作的原因：

 1.child物理计划的输出数据分布不satisfies当前物理计划的数据分布要求。比如说child物理计划的数据输出分布是UnspecifiedDistribution，而当前物理计划的数据分布要求是ClusteredDistribution

2.对于包含2个Child物理计划的情况，2个Child物理计划的输出数据有可能不compatile。因为涉及到两个Child物理计划采用相同的Shuffle运算方法才能够保证在本物理计划执行的时候一个分区的数据在一个节点，所以2个Child物理计划的输出数据必须采用compatile的分区输出算法。如果不compatile需要创建Exchange替换掉Child物理计划。

接着就直接看EnsureRequirements对物理计划的操作，直接看它的apply方法：

```
   1.这里注意这个transformUp方法，其实它是和LogicalPlan中的resolveOperators方法是功能是一样的，只不过它没有对处理过的子节点的检查，都是来遍历这个treeNode，先操作其子节点，再对它自己进行操作。
   def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator: SparkPlan => ensureDistributionAndOrdering(operator)
  }
```
这里直接看这个ensureDistributionAndOrdering方法，这个方法大家不要觉得太难，其实它的逻辑很简单下面，我一步步的给大家分析，说白了，就是翻译它的注释，从它每一句话都有写注释，你也应该能猜到这个方法的重要性：

```
private def ensureDistributionAndOrdering(operator: SparkPlan): SparkPlan = {
    val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    var children: Seq[SparkPlan] = operator.children
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    // Ensure that the operator's children satisfy their output distribution requirements:
     1. 这就是上面提到的第一个原因，child物理计划的输出数据分布是否满足当前物理计划的数据分布要求。例如：当前的操作LeftSemiJoinHash，而它的Distribution为ClusteredDistribution，它的child的Distribution为UnspecifiedDistribution，则会执行下面的else，即给这个child添加一个Exchange操作。
    children = children.zip(requiredChildDistributions).map { case (child, distribution) =>
      if (child.outputPartitioning.satisfies(distribution)) {
        child
      } else {
        Exchange(createPartitioning(distribution, defaultNumPreShufflePartitions), child)
      }
    }

    // If the operator has multiple children and specifies child output distributions (e.g. join),
    // then the children's output partitionings must be compatible:
    2. 这里就是上面提到的第二个原因，需要添加Exchange操作：对于包含2个Child物理计划的情况，2个Child物理计划的输出数据有可能不compatile。因为涉及到两个Child物理计划采用相同的Shuffle运算方法才能够保证在本物理计划执行的时候一个分区的数据在一个节点，所以2个Child物理计划的输出数据必须采用compatile的分区输出算法。如果不compatile需要创建Exchange替换掉Child物理计划。
    if (children.length > 1
        && requiredChildDistributions.toSet != Set(UnspecifiedDistribution)
        && !Partitioning.allCompatible(children.map(_.outputPartitioning))) {

      // First check if the existing partitions of the children all match. This means they are
      // partitioned by the same partitioning into the same number of partitions. In that case,
      // don't try to make them match `defaultPartitions`, just use the existing partitioning.
      3. 获取Children下最大的partition的数量
      val maxChildrenNumPartitions = children.map(_.outputPartitioning.numPartitions).max
      4. 检查children的partition数量是否都是这个值，换句话说，也就是各个child的Partitioning是否是一样的
      val useExistingPartitioning = children.zip(requiredChildDistributions).forall {
        case (child, distribution) => {
          child.outputPartitioning.guarantees(
            createPartitioning(distribution, maxChildrenNumPartitions))
        }
      }
     5. 如果是一致的则不需要shuffle child的输出
      children = if (useExistingPartitioning) {
        // We do not need to shuffle any child's output.
        children
      } else {
        // We need to shuffle at least one child's output.
        // Now, we will determine the number of partitions that will be used by created
        // partitioning schemes.
        val numPartitions = {
          // Let's see if we need to shuffle all child's outputs when we use
          // maxChildrenNumPartitions.
          6. 检查一下是否需要shuffle 所有的child的输出
          val shufflesAllChildren = children.zip(requiredChildDistributions).forall {
            case (child, distribution) => {
              !child.outputPartitioning.guarantees(
                createPartitioning(distribution, maxChildrenNumPartitions))
            }
          }
          // If we need to shuffle all children, we use defaultNumPreShufflePartitions as the
          // number of partitions. Otherwise, we use maxChildrenNumPartitions.
          7. 根据上面的结果来决定，最终的分区数，如果需要shuffle 所有的child，则使用默认分区数，如果仅需要shuffle 部分child，则使用上面得到的最大分区数。
          if (shufflesAllChildren) defaultNumPreShufflePartitions else maxChildrenNumPartitions
        }

        children.zip(requiredChildDistributions).map {
          case (child, distribution) => {
            val targetPartitioning =
              createPartitioning(distribution, numPartitions)
            8. 这里需要主要的是如果child的outputPartitioning和最终的Partitioning一致的话，就不需要添加exchange（这个其实就是对应的部分shuffle的情况），否则则添加。
            if (child.outputPartitioning.guarantees(targetPartitioning)) {
              child
            } else {
              child match {
                // If child is an exchange, we replace it with
                // a new one having targetPartitioning.
                关于这个child如果本身就是一个Exchange的话，就直接更改它的Partitioning，如果不是则添加一个Exchange操作。
                case Exchange(_, c, _) => Exchange(targetPartitioning, c)
                case _ => Exchange(targetPartitioning, child)
              }
            }
          }
        }
      }
    }

    // Now, we need to add ExchangeCoordinator if necessary.
    // Actually, it is not a good idea to add ExchangeCoordinators while we are adding Exchanges.
    // However, with the way that we plan the query, we do not have a place where we have a
    // global picture of all shuffle dependencies of a post-shuffle stage. So, we add coordinator
    // at here for now.
    // Once we finish https://issues.apache.org/jira/browse/SPARK-10665,
    // we can first add Exchanges and then add coordinator once we have a DAG of query fragments.
    9. 这里根据前面添加的操作还需要添加一个协调器，稍后详细介绍
    children = withExchangeCoordinator(children, requiredChildDistributions)

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    10. 是否需要对shuffles进行排序，如果需要的话，则再添加一个Sort操作，下面很简单，理解起来应该没什么难度。
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      if (requiredOrdering.nonEmpty) {
        // If child.outputOrdering is [a, b] and requiredOrdering is [a], we do not need to sort.
        if (requiredOrdering != child.outputOrdering.take(requiredOrdering.length)) {
          Sort(requiredOrdering, global = false, child = child)
        } else {
          child
        }
      } else {
        child
      }
    }
    11. 然后就是新增的这些操作添加进来，这里就是TreeNode的操作，也就是树的操作，大家应该都明白。
    operator.withNewChildren(children)
  }
```
下面就看这个协调器的创建withExchangeCoordinator，这段代码就是注释过多，具体流程很简单，下面给大家分析：

```
private def withExchangeCoordinator(
      children: Seq[SparkPlan],
      requiredChildDistributions: Seq[Distribution]): Seq[SparkPlan] = {
    1. 先判断它能否支持ExchangeCoordinator
    val supportsCoordinator =
      if (children.exists(_.isInstanceOf[Exchange])) {
        // Right now, ExchangeCoordinator only support HashPartitionings.
       1.1 也就是children中必须含有Exchange，且必须保证它的输出partitioning都是HashPartitioning类型的。
        children.forall {
          case e @ Exchange(hash: HashPartitioning, _, _) => true
          case child =>
            child.outputPartitioning match {
              case hash: HashPartitioning => true
              case collection: PartitioningCollection =>
                collection.partitionings.forall(_.isInstanceOf[HashPartitioning])
              case _ => false
            }
        }
      } else {
        // In this case, although we do not have Exchange operators, we may still need to
        // shuffle data when we have more than one children because data generated by
        // these children may not be partitioned in the same way.
        // Please see the comment in withCoordinator for more details.
        val supportsDistribution =
       1.2 第二种情况就是，children 中shuffle的数据没有使用同一种方式来进行分区  requiredChildDistributions.forall(_.isInstanceOf[ClusteredDistribution])
        children.length > 1 && supportsDistribution
      }
    2. 这里注意如果使用这个Coordinator的话，还需要打开其开关，默认情况下是关闭的，即spark.sql.adaptive.enabled。
    val withCoordinator =
      if (adaptiveExecutionEnabled && supportsCoordinator) {
        3. 下面就是创建这个coordinator。
        val coordinator =
          new ExchangeCoordinator(
            children.length,
            targetPostShuffleInputSize,
            minNumPostShufflePartitions)
        children.zip(requiredChildDistributions).map {
          3.1 如果child是Exchange，则直接将这个coordinator赋值给它。
          case (e: Exchange, _) =>
            // This child is an Exchange, we need to add the coordinator.
            e.copy(coordinator = Some(coordinator))
          case (child, distribution) =>
            // If this child is not an Exchange, we need to add an Exchange for now.
            // Ideally, we can try to avoid this Exchange. However, when we reach here,
            // there are at least two children operators (because if there is a single child
            // and we can avoid Exchange, supportsCoordinator will be false and we
            // will not reach here.). Although we can make two children have the same number of
            // post-shuffle partitions. Their numbers of pre-shuffle partitions may be different.
            // For example, let's say we have the following plan
            //         Join
            //         /  \
            //       Agg  Exchange
            //       /      \
            //    Exchange  t2
            //      /
            //     t1
            // In this case, because a post-shuffle partition can include multiple pre-shuffle
            // partitions, a HashPartitioning will not be strictly partitioned by the hashcodes
            // after shuffle. So, even we can use the child Exchange operator of the Join to
            // have a number of post-shuffle partitions that matches the number of partitions of
            // Agg, we cannot say these two children are partitioned in the same way.
            // Here is another case
            //         Join
            //         /  \
            //       Agg1  Agg2
            //       /      \
            //   Exchange1  Exchange2
            //       /       \
            //      t1       t2
            // In this case, two Aggs shuffle data with the same column of the join condition.
            // After we use ExchangeCoordinator, these two Aggs may not be partitioned in the same
            // way. Let's say that Agg1 and Agg2 both have 5 pre-shuffle partitions and 2
            // post-shuffle partitions. It is possible that Agg1 fetches those pre-shuffle
            // partitions by using a partitionStartIndices [0, 3]. However, Agg2 may fetch its
            // pre-shuffle partitions by using another partitionStartIndices [0, 4].
            // So, Agg1 and Agg2 are actually not co-partitioned.
            //
            // It will be great to introduce a new Partitioning to represent the post-shuffle
            // partitions when one post-shuffle partition includes multiple pre-shuffle partitions.
            3. 关于上面的注释，理解起来其实很简单，就是这个child的获得的数据可能不是使用同一种partitioning来获得的，所以这里重新给它构造一个Exchange，使得在这个child进行操作的时候，要操作的数据是按照同样的partitioning来分区的数据。
           4. 所以这里的实现就是再给这个child创建一个新的Exchange操作。
            val targetPartitioning =
              createPartitioning(distribution, defaultNumPreShufflePartitions)
            assert(targetPartitioning.isInstanceOf[HashPartitioning])
            Exchange(targetPartitioning, child, Some(coordinator))
        }
      } else {
        // If we do not need ExchangeCoordinator, the original children are returned.
        children
      }

    withCoordinator
  }
    
```
到这里，整个sparkplan的协调工作就全部完成了。在这一步处理完之后还需要进行EnsureRowFormats处理，这个就是要确保row 的formats的合理性，因为处理的row有可能是SafeRows和UnsafeRows，这里需要将处理的记录和相应的格式匹配起来。关于具体的代码，我就不加以分析了，有问题，给我留言。

###  后话
到这里你肯定会有一个疑问，那就是这个Coordinator是怎么工作的，这就是下一篇博客，要讲的内容了。万里之行就差最好一步了，也就是这个任务的真正执行。

### 参考资料
1. Spark 1.6.3源码
2. https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf
3. http://infolab.stanford.edu/~hyunjung/cs346/qpnotes.html
4. http://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf
5. http://blog.hydronitrogen.com/2016/02/22/in-the-code-spark-sql-query-planning-and-execution/#Exchange
6. http://blog.csdn.net/oopsoom/article/details/38257749
7. http://sqlblog.com/blogs/paul_white/archive/2012/04/28/query-optimizer-deep-dive-part-1.aspx
