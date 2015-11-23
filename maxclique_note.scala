//-----------------------------------处理之前---------------------------------------------
//val input_path = "datamine/outputFile/CLEANED_9/*"
//val source = sc.textFile(input_path)
//val res = source.map(x=>(x.split("\001")(0),x.split("\001")(1)))
//val ult = res.groupBy(x=>x._1).map(x=>x._2.map(x=>x._2))
//ult.saveAsTextFile("liziyao_spark/output2/")
//-----------------------------------处理------------------------------------------------
import collection.mutable.ArrayBuffer
//读取数据（每行作为一个String类型的元素的Array数组）
//val input_path = "/Users/liziyao/Desktop/testfile.txt"
val input_path = "liziyao_spark/output2/*"
val textFile = sc.textFile(input_path)
//读取资源信息，进行资源过滤的数据准备
val OBJECT_CLASS_FILE = "datamine/redisDatamine/AWJ_NETSOURCE.csv"
val obj = sc.textFile(OBJECT_CLASS_FILE)
//设置参数
val minconf = 0.5
val minsup = 0
//变换形式，输入(Array(a,b).c)，返回Array(a,b,c)
def arrOpt( a: (Array[String], String)):Array[String]={
            val arrbuff1 = ArrayBuffer[String]()
            arrbuff1 ++= a._1
            arrbuff1 += a._2
            arrbuff1.toArray
}
//变换形式，输入Array(a,b,c),返回(a?b,c)
def arr2tup(a:Array[String]):(String,String)={
            val arrbuff1 = a.toBuffer
            arrbuff1.trimEnd(1)
            (arrbuff1.toArray.mkString("\001"),a(a.length-1))
}
//变换形式，输入(a,Array(b,c)),返回(a,b,c)
def tup2arr(a:(String, Array[String])):Array[String]={
            val res1 = a._1+"\001"+a._2.mkString("\001")
            val res2 = res1.split("\001")
            res2
}
//变换形式，输入Array(a,b,c)，返回List(Array(a,b),Array(a,c))
def arr2arr2(a:Array[String]):List[Array[String]]={
            val tmp = a.toBuffer
            var ress = List[Array[String]]()
            tmp.remove(tmp.length-1)
            ress = tmp.toArray::ress
            val tmp1 = a.toBuffer
            tmp1.remove(tmp1.length-2)
            ress = tmp1.toArray::ress
            ress
}
//变换形式，输入(a,Array(b,c))，输出List((a,b),(a,c))
def com1( a: (String, Array[String])):List[(String,String)]={
          var tmp = List[(String,String)]()
          for( i <- 0 to a._2.length-1)
            tmp = (a._1,a._2(i))::tmp
          tmp
}
//遍历数组，枚举2-元集，返回以“a b”为元素的数组
def com2(a : Array[String]):Array[String]={
          var tmp = List[String]()
          for( i <- 0 to a.length-1)
             for(j <- i+1 to a.length-1)
                 tmp = a(i)+"\001"+a(j) :: tmp
          return tmp.toArray
}
//判断数组a的最后两项是否在映射b中，是返回true
def judge(a:Array[String],b:Map[String,Int]):Boolean={
            val arrbuff = ArrayBuffer(a(a.length-2),a(a.length-1)).toArray
            if(b.contains(arrbuff.mkString("\001")))
                 return true
            else return false
}
//判断数组b的元素是否都在映射a中，是返回true
def sons(b:Array[String],a: Map[String,Int]):Boolean={
            for( i <- 0 to b.length-1){
                  val arrbuff2 = ArrayBuffer[String]()
                  arrbuff2 ++= b
                  arrbuff2.remove(i)
                  val tmp = arrbuff2.toArray
                  if(!a.contains(tmp.mkString("\001"))){
                      println(tmp.mkString("\001"))
                      return false
                  }
            }
            return true
}
//取出子集，输入Array(a,b,c)，输出List(Array(b,c),Array(a,c),Array(a,b))
def son(a:Array[String]):List[Array[String]]={
            var ress = List[Array[String]]()
            for(i <- 0 to a.length-1){
                val tmp = a.toBuffer
                tmp.remove(i)
                ress = tmp.toArray::ress
            }
            ress
}
//1-项集统计
val source = textFile.map(x => x.replaceAll("[List()]",""))
val count = source.flatMap(line => line.split(",")).map(word => (word,1)).reduceByKey(_+_)
val colcount = textFile.count
val dic1 = count.toArray.toMap
//资源过滤对应的数据表
val obj_map = obj.map(x => (x.split(",")(0), x.split(",")(1))).toArray.toMap
val obj_map_reverse = obj.map(x => (x.split(",")(1), x.split(",")(0))).toArray.toMap
//资源过滤判断函数
def JudgeIn(a:Array[String]):Boolean={
        if(obj_map.contains(a(0))&&obj_map.getOrElse(a(0),"00")==a(1))
          return true
        else if(obj_map.contains(a(1))&&obj_map.getOrElse(a(1),"00")==a(0))
          return true
        else 
          return false
}
//计算2-项集的置信度
def calconf(a:(Array[String], Int)):Double={
        val up = a._2
        val down = dic1(a._1(0)).toDouble+dic1(a._1(1)).toDouble-up
        up/down
}
//计算2-项集相关的支持度
def calsup(a:(Array[String], Int)):Boolean={
        val result1 = dic1(a._1(0)).toDouble/colcount
        val result2 = dic1(a._1(1)).toDouble/colcount
        if ( result1 >= minsup && result2 >= minsup) return true
        else return false
}
//2-项集统计
val column2 = source.map(x => x.split(","))
val count2 = column2.flatMap( x => com2(x)).map(word => (word,1)).reduceByKey(_+_)
val com3 = count2.map( x => (x._1.split("\001"),x._2))
val conf = com3.filter( x => calsup(x)).map( x => (x,calconf(x)) )
//这一步加上了资源过滤的条件
val pfj_2 = conf.filter( x => x._2 >= minconf).map( x => (x._1._1)).filter(x => JudgeIn(x))

val table_2 = pfj_2.map(x=>(x.mkString("\001"),1)).toArray.toMap
//连接函数，由k-项可信集和k-1项规则生成k+1项可信集和k项规则
def connect( csk: org.apache.spark.rdd.RDD[Array[String]],result:List[Set[String]]):List[Set[String]]={
          var resu = List[Set[String]]()
          resu = result ::: resu
          if( csk.count > 0){
      //变换形式，生成k-项可信集的映射map
            val table = csk.map(x=>(x.mkString("\001"),1)).toArray.toMap
      //连接，将(a,b,c),(a,b,d)连接成(a?b,Array(c,d))
            val csk_tu = csk.map( x=>arr2tup(x)).reduceByKey(_+"\001"+_).map( x => (x._1,x._2.split("\001")))
      //将=2项相连的过滤出来，同时将(a?b,Array(c,d))变换为(a?b?c,d)
            val candidate_1 = csk_tu.filter ( x => x._2.length==2).map(x => (x._1+"\001"+x._2.sortWith(_<_)(0),x._2.sortWith(_<_)(1)))
      //将>2项相连的过滤出来，同时将(a?b,Array(c,d,e))变换为List((a?b,Array(c,d)),(a?b,Array(c,e)),(a?b,Array(d,e)))
            val pre_candidate = csk_tu.filter ( x => x._2.length>2).map( x => (x._1,com2(x._2.sortWith(_<_)))).flatMap( x => com1(x))
      //变换形式，将(a?b,Array(c,d))变换为(a?b?c,d)
            val candidate_2 = pre_candidate.map( x => (x._1+"\001"+x._2.split(" ")(0),x._2.split(" ")(1)))
      //将>=2项的处理结果连接起来，同时将(a?b?c,d)变换为(a,b,c,d),过滤掉不满足judge、sons函数的，剩下的即为k+1项集
            val candidate = candidate_1.union(candidate_2)
            val cnd_cskp1 = candidate.map ( x =>arrOpt( (x._1.split("\001"),x._2)))
            val cskp1 = cnd_cskp1.filter(x=>judge(x,table_2)).filter(x=>sons(x,table))
      //将<2项相连的过滤出来，同时将(a?b,Array(c))变换为(a,b,c)
            val rules1 = csk_tu.filter ( x => x._2.length==1).map(x=> tup2arr(x))
      //过滤出k+1项集中不满足judge、sons函数的，连接去重，作为rule的子集
            val rules2 = cnd_cskp1.filter( x => !judge(x,table_2))
            val rules3 = cnd_cskp1.filter( x => !sons(x,table))
            //val rules4 = rules2.union(rules3).flatMap(x=>arr2arr2(x)).map(x=>x.mkString("\001")).map(x=>x.split("\001"))
            val rules4 = rules2.union(rules3).flatMap(x=>arr2arr2(x))
      //连接rule，同时将(a,b,c,d)变换为(a?b?c?d)
            val rules = rules1.union(rules4) 
            val rul2set = rules.filter(x => x.size > 1).map( x=> x.mkString("\001")).distinct.toArray.toSet
            val cskp1son = cskp1.flatMap(x=>son(x)).map(x=>x.mkString("\001")).filter(x=>rul2set.contains(x)).distinct.toArray.toSet
            val trueRules = rul2set diff cskp1son
            resu =  trueRules::resu
            connect(cskp1,resu)
          }
          else
            return resu
}
val result = List[Set[String]]()
val ress = connect(pfj_2,result)
scala.tools.nsc.io.File("/home/boco/lzy/result3").writeAll(ress.toString)
