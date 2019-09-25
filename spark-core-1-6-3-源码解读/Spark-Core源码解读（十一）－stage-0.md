终于到了stage 0了，那这个要从何说起呢？还是从上篇博客中的提到的ShuffleMapTask中的runTask说起，我这里只指出重要的代码：

![Screen Shot 2016-12-09 at 6.10.36 PM.png](http://upload-images.jianshu.io/upload_images/3736220-ee52f353eff291dc.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

```
...... 
1. 这里完全根据上篇博客中的例子说明：这里得到的结果，rdd为上图中的stage 0 中的最后一个MapPartitionsRDD，
2. 然后这个dep为上面stage 1中的reduceByKey产生的ShuffledRDD的dependency。
val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
.......
3.上篇博客中已经详细的描绘了shuffle write方法，下面我们看它到底写的是什么数据，也就是rdd.iterator(partition, context)得到的结果。
 writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
```

因为上面的rdd为stage 0 中的最后一个MapPartitionsRDD，下面我们接直接看是怎么得到它的，先看一下我们的业务代码：

```
     val lines = sc.textFile("/Users/chenkevin/spark-1.6.0-bin-hadoop2.6/spark",1)
    val words = lines.flatMap(_.split(" "))
    1. 它是通过map方法构建了一个MapPartitionsRDD
    val pairs = words.map(word=>(word,1))
```
下面看这个map的实现：
```
  def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    1. 清理这个函数中的闭包，这个闭包的英文单词是closure
    2. 这里注意这个f就是word=>(word,1)。
    val cleanF = sc.clean(f)
    3. 生成一个MapPartitionsRDD
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
  }
```
stage 0 中其它的RDD也基本都是以这种方式生成的，接着回过头来看它的iterator方法，也就是上文中提到的rdd.iterator(partition, context）：
```
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    1. 如果有把rdd进行cache的话，则从cache中读取
    if (storageLevel != StorageLevel.NONE) {
      SparkEnv.get.cacheManager.getOrCompute(this, split, context, storageLevel)
    } else {
     2.对partition的数据进行计算 
      computeOrReadCheckpoint(split, context)
    }
  }

```
下面看这里的computeOrReadCheckpoint方法：

```
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    1.是否有对rdd进行过checkpoint，如果有，则从checkpoint中读取数据
    if (isCheckpointedAndMaterialized) {
      firstParent[T].iterator(split, context)
    } else {
    2. 继续对原有partition数据进行计算
      compute(split, context)
    }
  }
  
```
关于这个compute方法，一般都是在具体的RDD中实现的，这里就是在MapPartitionsRDD实现的：
```
  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))
      
```
关于这个f函数，这里的f函数是怎么得来的，还是得要说一下：
注意，下面不是具体的代码啊，我只是为了方便，把相关的代码放在一起了。
```
val pairs = words.map(word=>(word,1))
1.由上面可以得知f ＝  word=>(word,1)，然后经过闭包清理后得到cleanF

new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
2. 在这里会得到前面compute方法中调用的是f ＝ (context, pid, iter) => iter.map(cleanF)   
```
所以最终MapPartitionsRDD使用的是这个方法：f ＝ (context, pid, iter) => iter.map(cleanF) 。所以通过这个f处理，最终得到的是(key,value)的iterator。

到这里回过头来看    f(context, split.index, firstParent[T].iterator(split, context))中的firstParent：
```
  /** Returns the first parent RDD */
  protected[spark] def firstParent[U: ClassTag]: RDD[U] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
}
      
```
也就是说明firstParent指的是这个rdd的父RDD，即stage中通过flatMap产生的MapPartitionsRDD。到这里你会看到这是在一直回溯，直到回溯到，compute方法中操作的是具体的partition，才不在回溯，直接进行计算，然后后一个rdd中的compute方法在依次根据前一个rdd中的compute得到的结果进行计算。直到最后得到最终的数据。

这里关于HadoopRDD的方法，我就不分析了，大家自己下去看吧！也没什么分析的必要了，而之后stage 1中的操作大约也是这种调调，我也不加以分析了，当然如果大家有问题的可以直接给我留言。其实业务逻辑很简单的。






