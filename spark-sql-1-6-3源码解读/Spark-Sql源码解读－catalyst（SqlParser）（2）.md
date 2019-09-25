上篇博客的结尾已经给大家介绍了Spark Sql的核心流程，而这个核心流程也就是通过Spark Sql中最核心的catalyst模块来实现的。下面按步骤一步步分析每个处理的过程：

1. SQL 语句经过 SqlParser 解析成 Unresolved LogicalPlan;
2. 使用 analyzer 结合数据数据字典(catalog)进行绑定,生成 resolved LogicalPlan;
3. 使用 optimizer 对 resolved LogicalPlan 进行优化,生成 optimized LogicalPlan;
4. 使用 SparkPlan 将 LogicalPlan 转换成 PhysicalPlan;
5. 使用 prepareForExecution()将 PhysicalPlan 转换成可执行物理计划;
6. 使用 execute()执行可执行物理计划;
7. 生成 RDD。

### 1.1 SqlParser
在Spark Sql中，对于HQL和SQL语句都是支持的，这里我们只解读对于SQL语句的分析（原理一致，只是小的细节有所不同）。而spark sql中对于sql语句解析的类就是SqlParser。而在对SqlParser的解析前，这里我先自己写了一个简单数字表达式解析过程的demo：
```
package org.dt.examples.sql

import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

/**
  * @example 简单数字表达式解析
  * @author chenKevin
  */
object MyLexical extends StandardTokenParsers with PackratParsers {

  //定义分割符
  lexical.delimiters ++= List(".", ";", "+", "-", "*")
  //定义表达式,支持加,减,乘
  lazy val expr: PackratParser[Int] = plus | minus | multi
  //加法表示式的实现
  lazy val plus: PackratParser[Int] = num ~ "+" ~ num ^^ {
    case n1 ~ "+" ~ n2 => n1.toInt + n2.toInt
  }
  //减法表达式的实现
  lazy val minus: PackratParser[Int] = num ~ "-" ~ num ^^ {
    case n1 ~ "-" ~ n2 => n1.toInt - n2.toInt
  }

  //乘法表达式的实现
  lazy val multi: PackratParser[Int] = num ~ "*" ~ num ^^ {
    case n1 ~ "*" ~ n2 => n1.toInt * n2.toInt
  }

  lazy val num = numericLit

  def parse(input: String) = {
    //定义词法读入器 myread,并将扫描头放置在 input 的首位
    val myread = new PackratReader(new lexical.Scanner(input))
    print("处理表达式 " + input)
    phrase(expr)(myread) match {
      case Success(result, _) => println(" Success!"); println(result); Some(result)
      case n => println(n); println("Err!"); None
    }
  }

  def main(args: Array[String]) {
    val prg = "6 * 3 " :: "24-/*aaa*/4" :: "a+5" :: "21/3" :: Nil
    prg.map(parse)
  }
}
```
运行结果：

![Screen Shot 2016-12-17 at 12.38.18 AM.png](http://upload-images.jianshu.io/upload_images/3736220-273d651bf9a64749.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

在运行的时候,首先对表达式 6 ＊ 3 进行解析,词法读入器 myread 将扫描头置于 6的位置;当 phrase()函数使用定义好的数字表达式 expr 处理 6 ＊ 3 的时候,6 ＊ 3 每读入一个词法,就和 expr 进行匹配,如读入 6＊和 expr 进行匹配,先匹配表达式 plus,＊和＋匹配不上;就继续匹配表达式 minus,＊和－匹配不上;就继续匹配表达式 multi,这次匹配上了,等读入 3 的时候,因为 3 是 num 类型,就调用调用 n1.toInt ＊ n2.toInt 进行计算。

注意,这里的 expr、plus、minus、multi、num 都是表达式,|、~、^^是复合因子,表达式和复合因子可以组成一个新的表达式,如 plus(num ~ "+" ~ num ^^ { casen1 ~ "+" ~ n2 => n1.toInt + n2.toInt})就是一个由 num、+、num、函数构成的复合表达式;而 expr(plus | minus | multi)是由 plus、minus、multi 构成的复合表达式;复合因子的含义定义在类 scala.util.parsing.combinator.Parsers,下面是几个常用的复合因子:

* `p~q` ，p成功,才会q;放回p,q的结果
* `p~>q` ，p成功,才会q,返回q的结果
* `p<~q` ，p成功,才会q,返回p的结果
* `p | q` ，p 失败则 q,返回第一个成功的结果
* `p^^f` ，如果p成功,将函数f应用到p的结果上
* `p^?f` ，如果p成功,如果函数f可以应用到p的结果上的话,就将p的结果用f进行转换

但在阅读源码的时候，你还会用到下面两个复合因子：

* opt， `opt(p)` is a parser that returns `Some(x)` if `p` returns `x` and `None` if `p` fails.如果p算子成功则返回则返回Some（x） 如果p算子失败，返回fails
* ^^^ ，`p ^^^ v` succeeds if `p` succeeds; discards its result, and returns `v` instead.如果左边的算子成功，取消左边算子的结果，返回右边算子。


针对上面的 6 ＊ 3 使用的是 multi 表达式(num ~ "＊" ~ num ^^ { case n1 ~ "＊" ~
n2 => n1.toInt ＊ n2.toInt}),其含义就是:num 后跟*再跟 num,如果满足就将使用函数 n1.toInt  ＊ n2.toInt。到这里为止,大家应该明白整个解析过程了吧。SqlParser 的原理和这个表达式解析器使用了一样的原理,只不过是定义的 SQL 语法表达式 start 复杂一些,使用的词法读入器更丰富一些而已。下面分别介绍一下相关组件 SqlParser、SqlLexical、start。

### 1.2 SqlParser 的简单的 UML 图

![Screen Shot 2016-12-17 at 3.48.42 AM.png](http://upload-images.jianshu.io/upload_images/3736220-43b953e728d21cd1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
其中其次SqlParser 最终继承自类 StandardTokenParsers 和特质PackratParsers，还有DataTypeParser。

其中,PackratParsers:
* 扩展了 scala.util.parsing.combinator.Parsers 所提供的 parser,做了内存化处理;
* Packrat 解析器实现了回溯解析和递归下降解析,具有无限先行和线性分析时的优势。同时,也支持左递归词法解析。
* 从 Parsers 中继承出来的 class 或 trait 都可以使用 PackratParsers,如:object MyGrammar extends StandardTokenParsers with PackratParsers;

个人理解：PackratParsers可以通俗的理解为sql的解释器。

PackratParsers 将分析结果进行缓存,因此，PackratsParsers 需要 PackratReader(内存化处理的 Reader)作为输入更多的细节参见scala.util.parsing.combinator.PackratParsers.scala 


StandardTokenParsers 是最终继承自 Parsers

增加了词法的处理能力(Parsers 是字符处理),在 StdTokenParsers 中定义了四种基本词法:
* keyword tokens
* numeric literal tokens
* string literal tokens
* identifier tokens
* 定义了一个词法读入器 lexical,可以进行词法读入

而DataTypeParser就是一个数据类型转换器，如转化为arrayType ，mapType ，structType ，primitiveType。

### 1.3 Spark Sql解析Sql语句的流程
下面我们就一步步的看Sql是如何被解析的。
下面看一个调用sql语句的业务代码：

```
    val query = sql("SELECT * FROM src")
```
```
    def sql(sqlText: String): DataFrame = {
    DataFrame(this, parseSql(sqlText))
  }
```
```
    protected[sql] def parseSql(sql: String): LogicalPlan = ddlParser.parse(sql, false)
```
接着在调用的这个parse方法位于DDLParser
```
  def parse(input: String, exceptionOnError: Boolean): LogicalPlan = {
    try {
      parse(input)
    } catch {
      case ddlException: DDLException => throw ddlException
      case _ if !exceptionOnError => parseQuery(input)
      case x: Throwable => throw x
    }
  }

```
接着看下面调用的parse方法：
```
   def parse(input: String): LogicalPlan = synchronized {
    // Initialize the Keywords.
    1. 初始化关键字
    initLexical
    4. 根据获取的关键字，对sql语句解析，最终获得logical plan。
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError => sys.error(failureOrError.toString)
    }
  }

   2. 上面1中初始化是调用这里，而这里的参数是获取reservedWords的函数
  protected lazy val initLexical: Unit = lexical.initialize(reservedWords)
```
3. 因为最后调用这个方法的类是DDLParser，所以其实这里获取的关键字，也就是DDLParser中的关键字变量。

```
  protected lazy val reservedWords: Seq[String] =
    this
      .getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].normalize)

```
下面再详细分析一下4， phrase(start)(new lexical.Scanner(input)) 
其中在类中的start为ddl，而ddl却是：

```
    protected lazy val ddl: Parser[LogicalPlan] = createTable | describeTable | refreshTable
```
又由于我们的sql语句为SELECT * FROM src，在通过ddl构建的PackratParser，根本无法匹配到SELECT，因为DDLParser中获取的关键字根本不包含SELECT。所以这里还得看代码的执行：

```
   def parse(input: String): LogicalPlan = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError => sys.error(failureOrError.toString)
    }
  }
```
即这里会抛出异常，那么接着看它是怎么处理的?

```
class DDLParser(parseQuery: String => LogicalPlan)
  extends AbstractSparkSQLParser with DataTypeParser with Logging {

  def parse(input: String, exceptionOnError: Boolean): LogicalPlan = {
    try {
      1.上面的异常是通过，下面这句代码产生的
      parse(input)
    } catch {
      case ddlException: DDLException => throw ddlException
      2. 根据的抛出的异常是sys.error(failureOrError.toString)，它抛出的是RuntimeException，所以不会匹配到上面的ddlException，
因此会执行下面这句话，另外由于exceptionOnError刚开始指定的是false，所以直接执行parseQuery
      case _ if !exceptionOnError => parseQuery(input)
      case x: Throwable => throw x
    }
  }
  ......
}
  
```
那么这里这个parseQuery又是什么？ 其实它在这里
```
class DDLParser(parseQuery: String => LogicalPlan)  
```
也就是，DDLParser实例化的时候传入的，那么看一下它是什么？

```
  protected[sql] val ddlParser = new DDLParser(sqlParser.parse(_))

```
```
  @transient
  protected[sql] val sqlParser = new SparkSQLParser(getSQLDialect().parse(_))
```
所以上面那个parseQuery就是SparkSQLParser的parse方法，但是这个SparkSQLParser中还是没有SELECT的关键字。所以它仍然匹配不到，但是万幸的是它的start中有一个others：
```
 private lazy val others: Parser[LogicalPlan] =
    wholeInput ^^ {
      case input => fallback(input)
    }
```
在这里它又会去调用fallback方法了，那这个fallback又是什么玩意呢？

```
class SparkSQLParser(fallback: String => LogicalPlan) extends AbstractSparkSQLParser
```
他也是在new 一个SparkSQLParser的时候传入的，那现在一下前面new这个对象的那段代码：

```
 1. 这里就是new 这个SparkSQLParser的地方
 @transient
  protected[sql] val sqlParser = new SparkSQLParser(getSQLDialect().parse(_))
 2. 下面这个方法就是通过反射的方法来构建的传入的那个对象
  protected[sql] def getSQLDialect(): ParserDialect = {
    try {
      val clazz = Utils.classForName(dialectClassName)
      clazz.newInstance().asInstanceOf[ParserDialect]
    } catch {
      case NonFatal(e) =>
        // Since we didn't find the available SQL Dialect, it will fail even for SET command:
        // SET spark.sql.dialect=sql; Let's reset as default dialect automatically.
        val dialect = conf.dialect
        // reset the sql dialect
        conf.unsetConf(SQLConf.DIALECT)
        // throw out the exception, and the default sql dialect will take effect for next query.
        throw new DialectException(
          s"""Instantiating dialect '$dialect' failed.
             |Reverting to default dialect '${conf.dialect}'""".stripMargin, e)
    }
  }

```
下面我们看那个对象的类是什么？ 即dialectClassName：
```
   protected[sql] def dialectClassName = if (conf.dialect == "sql") {
   1. 这里它会执行这个if语句，因为conf.dialect默认就是"sql"
    classOf[DefaultParserDialect].getCanonicalName
  } else {
    conf.dialect
  }
```
所以我们最终执行的是DefaultParserDialect的parse方法。接着我们就看一下这个方法：

```
  private[spark] class DefaultParserDialect extends ParserDialect {
  @transient
  protected val sqlParser = SqlParser

  override def parse(sqlText: String): LogicalPlan = {
    sqlParser.parse(sqlText)
  }
}
```
也就是说其实最终的最终，fallback方法指的是SqlParser中的parse方法，而这个方法其实还是我们前面提到的AbstractSparkSQLParser的这个parse，但是如今这里它的父类变成了SqlParser，而SqlParser中是由关键字SELECT的，即
```
  protected val SELECT = Keyword("SELECT")
```
所以sql语句最终是在这里解析的。

### 1.4 Spark Sql解析Sql语句的流程（2）
下面就看他时怎么匹配的，其中在SqlParser中start为
```
    protected lazy val start: Parser[LogicalPlan] =
    start1 | insert | cte
```
而我们的sql语句为SELECT * FROM src，也就是这个start会首先去匹配SELECT关键字。

它是通过这个start1来匹配的：

```
  protected lazy val start1: Parser[LogicalPlan] =
    (select | ("(" ~> select <~ ")")) *
    ( UNION ~ ALL        ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Union(q1, q2) }
    | INTERSECT          ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Intersect(q1, q2) }
    | EXCEPT             ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Except(q1, q2)}
    | UNION ~ DISTINCT.? ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Distinct(Union(q1, q2)) }
    )
    
```
在上面还没有匹配到，接着看这个select
```
 protected lazy val select: Parser[LogicalPlan] =
    1. 到这里他才匹配到这个SELECT关键字
    SELECT ~> DISTINCT.? ~
      repsep(projection, ",") ~
      (FROM   ~> relations).? ~
      (WHERE  ~> expression).? ~
      (GROUP  ~  BY ~> rep1sep(expression, ",")).? ~
      (HAVING ~> expression).? ~
      sortType.? ~
      (LIMIT  ~> expression).? ^^ {
        case d ~ p ~ r ~ f ~ g ~ h ~ o ~ l =>
          val base = r.getOrElse(OneRowRelation)
          val withFilter = f.map(Filter(_, base)).getOrElse(base)
          val withProjection = g
            .map(Aggregate(_, p.map(UnresolvedAlias(_)), withFilter))
            .getOrElse(Project(p.map(UnresolvedAlias(_)), withFilter))
          val withDistinct = d.map(_ => Distinct(withProjection)).getOrElse(withProjection)
          val withHaving = h.map(Filter(_, withDistinct)).getOrElse(withDistinct)
          val withOrder = o.map(_(withHaving)).getOrElse(withHaving)
          val withLimit = l.map(Limit(_, withOrder)).getOrElse(withOrder)
          withLimit
      }    
```
接着匹配我们的projection，在这条语句中是一个“＊”：
```
 protected lazy val projection: Parser[Expression] =
   1. 先是匹配到这里
    expression ~ (AS.? ~> ident.?) ^^ {
      case e ~ a => a.fold(e)(Alias(e, _)())
    }
 
```
接下来我只把变量依次写出来，直到匹配到最终的结果：
expression－》andExpression－》notExpression－》comparisonExpression－》termExpression－》termExpression－》baseExpression
直到这里最后匹配到这个星号：

```
 protected lazy val baseExpression: Parser[Expression] =
    ( "*" ^^^ UnresolvedStar(None)
    | rep1(ident <~ ".") <~ "*" ^^ { case target => UnresolvedStar(Option(target))}
    | primary
   )
```
这里它匹配到的是 "*" ^^^ UnresolvedStar(None)
所以最终的处理结果为UnresolvedStar(None)。
同理匹配relations的时候最终得到的是UnresolvedRelation(tableIdent, alias)

所以说sql语句在转义完之后，首先生成的是Unresolved LogicalPlan。

而在select的下面经过模式匹配的处理就是在构建tree。即：

```
 case d ~ p ~ r ~ f ~ g ~ h ~ o ~ l =>
          val base = r.getOrElse(OneRowRelation)
          val withFilter = f.map(Filter(_, base)).getOrElse(base)
          val withProjection = g
            .map(Aggregate(_, p.map(UnresolvedAlias(_)), withFilter))
            .getOrElse(Project(p.map(UnresolvedAlias(_)), withFilter))
          val withDistinct = d.map(_ => Distinct(withProjection)).getOrElse(withProjection)
          val withHaving = h.map(Filter(_, withDistinct)).getOrElse(withDistinct)
          val withOrder = o.map(_(withHaving)).getOrElse(withHaving)
          val withLimit = l.map(Limit(_, withOrder)).getOrElse(withOrder)
          withLimit     
```
下面我在逐行的分析一下每一行运行产生的结果：
1. 
```
     val base = r.getOrElse(OneRowRelation)       
```
首先呢，这个r匹配的是 (FROM   ~> relations).?，这里？就是opt的操作。根据上面对~>和opt的说明，最终产生的结果为relations处理的结果，即为UnresolvedRelation(tableIdent, alias)
2. 
```
    val withFilter = f.map(Filter(_, base)).getOrElse(base)
```
f匹配的是(WHERE  ~> expression).?，而上文中提供的sql语句为SELECT * FROM src，所以它匹配不到WHERE，所以f为None。这里withFilter最终获取的就是base
的值，即UnresolvedRelation(tableIdent, alias)
3. 
```
val withProjection = g
            .map(Aggregate(_, p.map(UnresolvedAlias(_)), withFilter))
            .getOrElse(Project(p.map(UnresolvedAlias(_)), withFilter))      
                 
```
g匹配的是(GROUP  ~  BY ~> rep1sep(expression, ",")).?，所以g也是None，那么这段代码最终的结果withProjection就是Project(p.map(UnresolvedAlias(_)), withFilter)。

在这里形成的tree为:

![Screen Shot 2016-12-17 at 11.50.34 PM.png](http://upload-images.jianshu.io/upload_images/3736220-da164e5dd140de99.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

接下来看d，h，o，l的匹配结果，均为None，所以默认值均是取得withProjection的值，所以最终的tree的构建结果就是上面那个图。

ok，到这里整个catalyst的解析器部分就讲完了，后面几个步骤我会放在一篇博客里面讲完，因为最开始的解析过程是有点麻烦。周末愉快！！！

### 参考资料
1. Spark 1.6.3源码
2. https://people.csail.mit.edu/matei/papers/2015/sigmod_spark_sql.pdf
3. http://infolab.stanford.edu/~hyunjung/cs346/qpnotes.html
4. http://paperhub.s3.amazonaws.com/dace52a42c07f7f8348b08dc2b186061.pdf
5. http://blog.hydronitrogen.com/2016/02/22/in-the-code-spark-sql-query-planning-and-execution/#Exchange
6. http://blog.csdn.net/oopsoom/article/details/38257749
7. http://sqlblog.com/blogs/paul_white/archive/2012/04/28/query-optimizer-deep-dive-part-1.aspx
