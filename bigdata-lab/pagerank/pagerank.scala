import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.File
 
object PageRank 
{

  //删除HDFS上指定的文件。
  def hdfsDel(sc: SparkContext, filePath: String): Unit = 
  {
  	    //第一步将文件路径转换为Path对象
    	val output = new Path(filePath)
    	//第二步获得与SparkContext的配置相关联的hadoopConfiguration
    	val conf = sc.hadoopConfiguration
    	//然后通过FileSystem.get (conf)获得hdfs文件系统
    	val hdfs = FileSystem.get(conf)
    	//最后，检查文件是否存在于文件系统中，如果存在则用hdfs对象对其进行删除
    	//hdfs.delete(output, true)中的true表明该目录不为空
    	if (hdfs.exists(output))
      		hdfs.delete(output, true)
  }
  
  def main(args: Array[String]): Unit = 
  {
  	    //setMaster:设置主节点,使用与类似于hdfs的conf设置
        val conf = new SparkConf().setAppName("PageRank_lalayouyi").setMaster("spark://10.102.0.198:7077")
        //通过SparkContext得到Spark的上下文，可以连接到文件系统，主要还是得到RDD算子进行操作。
    	val scontext = new SparkContext(conf)
    	
    	
    	//-----------------------------进行迭代前的一些准备-------------------------
    	val d = 0.85//阻尼系数
    	val iterCount = 10//迭代次数
    	//从HDFS读取图结构,并把图结构存入内存
    	val lines = scontext.textFile("hdfs://10.102.0.198:9000/ex3/input", 1)
    	//map,得到<page {page1,page2,page3...}>,即<当前网页,指向的网页列表>
    	//将每行数据按tab分隔，取分隔后的第一个元素作为key，取分隔后的第二个元素，按逗号分隔成多个元素作为value,返回一个新的RDD,RDD（弹性分布式数据集）
    	//这一系列操作会被多次使用，最后加个cache表示将此RDD存放内存，增加代码的效率
    	val links = lines.map(line => (line.split("\t")(0), line.split("\t")(1).split(","))).cache()
    	//初始化PR值
    	var ranks = links.mapValues(_ => 1.0)


        //------------------------------- 接下来进行迭代---------------------------
    	for (i <- 0 until iterCount) //进行10次迭代
    	{
    	    //得到<page,({page1,page2,page3……},PR)>,指向的网页信息+ranks值字符串
      		val mapInput = links.join(ranks)
      		//计算输出的pr
      		val answer = mapInput.flatMap 
      		{
        		//将每个网页的排名值(rank)平均分配给它所指向的所有链接页面(linkList)
        		//由一个page输出多个<page_i,pr'>,即标定好给谁多少pr
        		case (_, (linkList, rank)) => linkList.map(pageTo => (pageTo, rank / linkList.size))
      		}
      		
      		//reduce，先求和得到总的pr，再加权,这里的reduceByKey就是将同一个key中的value值累加
      		val pagePR = answer.reduceByKey((x, y) => x + y)
      		//mapValues只对value做出操作，key保留;
      		ranks = pagePR.mapValues(v => (1 - d) + d * v)
    	}
 
 	    //sortBy进行排序操作(mapValues只对value做出操作，key保留),输出格式是保留10位小数
    	val result = ranks.sortBy(x => x._2, false).mapValues(x => x.formatted("%.10f"))
    	//输出:(page,PR)
    	
    	//结果输出路径
    	val SavePath = "hdfs://10.102.0.198:9000/user/bigdata_学号/Experiment_3_Spark"
    	//如果目标目录已经存在，那么再写到该目录会出错，因此需要先将存在的目录删除
   	    hdfsDel(scontext, SavePath)
    	//保存到文件系统
    	result.saveAsTextFile(SavePath)
  }
}

