package pagerank;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class pagerank 
{
	//用于处理原始输入得到所需输入格式
	public static class GraphBuilder 
	{	
		public static class GraphBuilderMapper extends Mapper<Object, Text, Text, Text> 
		{
            @Override
		    //map：逐行分析原始数据
        	//输入：<默认为行偏移量,text类型的数据，按行处理>
            //key不是page，而是行偏移。value是整个<Page， {page_1,page_2,page_3,...}>
			protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
            {
                String initialpr = "1.0";  //初始化PR值
                String[] temp = value.toString().split("\t");//按tab分片,temp[0]是网页，temp[1]是链接的网页
            	//输出<网页, (初始化PR值, 链接列表)>
                //<Page,PR  {page1,page2,page3,...}>
                context.write(new Text(temp[0]), new Text(initialpr + "\t" + temp[1]));
			}
        }
		//Reduce什么也不需要干，因此可以不写Reduce，直接将Map的输出作为最后的输出即可
        public static void main(String[] args) throws Exception 
        {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            Job job = Job.getInstance(conf, "GraphBuilder");
            job.setJarByClass(GraphBuilder.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(GraphBuilderMapper.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        }
	}

	//迭代计算各个网页的PageRank值
    public static class PageRankIter 
    {
        private static final double d = 0.85;  //damping阻尼系数
        public static class PRIterMapper extends Mapper<Object, Text, Text, Text> 
        {  
        	//map：逐行分析数据
        	//输入：<Page,PR  {page1,page2,page3,...}>
        	@Override
        	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        	{
        		//按tab分片,temp[0]是网页，temp[1]是初始PR值,temp[2]是链接(指向)网页
        		String[] temp = value.toString().split("\t");
                String url = temp[0];
                double cur_rank = Double.parseDouble(temp[1]);//转换成double类型的数据
                if (temp.length > 2) 
                {//说明有链接的(指向的)网页
                    String[] link = temp[2].split(",");//按逗号分片
                    for (String i : link) 
                    {
                    	//输出:<链接的网页,当前排名/指向的网页的数量>
                    	//对于pageList里的每一个page都输出<page,pr>
                        context.write(new Text(i), new Text(String.valueOf(cur_rank / link.length)));
                    }
                }
                //输出:<当前网页,&+链接的网页>
                //将所有出边tuple[2]传递到Reduce，这里用一个‘&’作为分割，方便后续判断是哪一种输入类型，即value是pr还是pageList。
                context.write(new Text(url), new Text("&" + temp[2]));  // 做个标记"&"
        	}       	
        }
        
        public static class PRIterReducer extends Reducer<Text, Text, Text, Text> 
        {	
        	//reduce:按行处理数据
        	//输入:<链接的网页,当前排名/指向的网页的数量>;<当前网页,&+链接的网页>
        	@Override
        	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        	{
        		double new_rank = 0;//用于累加PR
                String outurl = "";
                for (Text i : values) 
                {
                	//对输入的value首字符进行判断，如果是'&'表示这是一个图结构，那么就保存下来便于待会Reduce输出；如果不是,那么说明是pr值，进行累加。
                    String temp = i.toString();
                    if (temp.startsWith("&")) 
                    {//有&标识符,标志着向的信息  
                        outurl = temp.substring(1);//取出向的网页列表
                    } 
                    else 
                    {//存放的是计算的PR中间值
                        new_rank += Double.parseDouble(temp);//原始PR+中间计算值
                    }
                }
                new_rank = d * new_rank + (1 - d);  //加上阻尼系数限制,计算最后的PR
                //输出:<网页,新的PR值 指向的网页信息>
                //<pagei,PR {page1,page2,page3,...}>
                context.write(key, new Text(String.valueOf(new_rank)+"\t"+outurl));
        	}
		}

        public static void main(String[] args) throws Exception 
        {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            Job job = Job.getInstance(conf, "PageRankIter");
            job.setJarByClass(PageRankIter.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(PRIterMapper.class);
            job.setReducerClass(PRIterReducer.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        }
    }
    
    //将PageRank值从大到小输出
    public static class RankViewer 
    {   	
    	public static class PRViewerMapper extends Mapper<Object, Text, DoubleWritable, Text> 
    	{ 	
    		//处理经过迭代处理后输出的Data中间文件
    		//输入:<默认为行偏移量,text类型的数据，按行处理>
    		@Override
    		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    		{
    			String[] temp = value.toString().split("\t");//按tab分片,temp[0]是网页,temp[1]是PR值,temp[2]是向的网页信息
    			// 输出:<PR值,网页>
    			//将PR提出来变为key，page作为value(图结构不需要用了，可以抛弃掉了)。
                context.write(new DoubleWritable(Double.parseDouble(temp[1])), new Text(temp[0]));
    		}
    	}

    	public static class DescDoubleComparator extends DoubleWritable.Comparator 
    	{  	
    		//给定的标准输出是按照降序排序，而Map默认是升序排序，因此需要自定义一个排序函数。基本就是继承原有的类将输出变成相反数即可
    		//重载key的比较函数，变为从大到小
    		public float compare(WritableComparator a, WritableComparable<DoubleWritable> b) 
    		{
    			return -super.compare(a, b);
    		}
   
    		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
    		{
    			return -super.compare(b1, s1, l1, b2, s2, l2);
    		}
    	}
    	
    	public static class PRViewerReducer extends Reducer<DoubleWritable, Text, Text, Text>
    	{
    		//输入:<PR值,网页list(值为PR值的那些网页)>
    		@Override
    		protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
    		{
    			for (Text i : values) 
    			{
    				//遍历网页列表，输出:(网页名,保留小数点后10位的PR值)
    				context.write(new Text("(" + i + "," + String.format("%.10f", key.get()) + ")"), null);
                }
    		}
    	}

        public static void main(String[] args) throws Exception
        {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            Job job = Job.getInstance(conf, "RankViewer");
            job.setJarByClass(RankViewer.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(PRViewerMapper.class);
            job.setSortComparatorClass(DescDoubleComparator.class);
            job.setReducerClass(PRViewerReducer.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
        }
    }
    
    
    //以PageRankfunction作为主类，调用前三个函数的main函数
    public static class PageRankFunction 
    {
    	//进行10次迭代
    	private static int times = 10;
    	public static void main(String[] args) throws Exception 
    	{
    		//建立网页间的连接信息并初始化PR值,结果存入Data0
			String[] functionPageRankBuilder = {"", args[1] + "/Data0"};  
			functionPageRankBuilder[0] = args[0];
			GraphBuilder.main(functionPageRankBuilder);
			
			String[] functionPageRankIter = {"", ""};  //迭代操作
			for (int i = 0; i < times; i++) 
			{
				functionPageRankIter[0] = args[1] + "/Data" + i;//Data i 是输入路径，Data i+1 是输出路径
				functionPageRankIter[1] = args[1] + "/Data" + (i + 1);
				PageRankIter.main(functionPageRankIter);
			}
			
			//最后一次输出的数据是输入信息，结果输出到FinalRank文件夹
			String[] functionPageRankViewer = {args[1] + "/Data" + times, args[1] + "/FinalRank"}; 
			RankViewer.main(functionPageRankViewer);
		}
    }
    
    // 主函数入口
    public static void main(String[] args) throws Exception 
    {
   		Configuration conf = new Configuration();
    	conf.set("fs.defaultFS", "hdfs:localhost:9000");
    	PageRankFunction.main(args);
    }
}