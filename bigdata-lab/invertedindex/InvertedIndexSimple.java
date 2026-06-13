package exp2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndexSimple {
    
    // Mapper类：输出<单词, 文档名:1>
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase());
            
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                token = token.replaceAll("[^a-zA-Z0-9']", "");
                if (token.length() > 0) {
                    outKey.set(token);
                    outValue.set(fileName + ":1");
                    context.write(outKey, outValue);
                }
            }
        }
    }
    
    // Reducer类：合并统计，去除停用词
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        private Set<String> stopWords = new HashSet<String>();
        private Text result = new Text();
        
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader("./stop_words_eng.txt"));
                String line;
                while ((line = reader.readLine()) != null) {
                    String word = line.trim().toLowerCase();
                    if (word.length() > 0) {
                        stopWords.add(word);
                    }
                }
                reader.close();
            }
        }
        
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String word = key.toString();
            
            // 过滤停用词
            if (stopWords.contains(word)) {
                return;
            }
            
            // 统计每个文档中的词频
            java.util.Map<String, Integer> docMap = new TreeMap<String, Integer>();
            
            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length == 2) {
                    String docName = parts[0];
                    int count = 1;
                    if (docMap.containsKey(docName)) {
                        count = docMap.get(docName) + 1;
                    }
                    docMap.put(docName, count);
                }
            }
            
            // 构建输出字符串
            StringBuilder sb = new StringBuilder();
            for (java.util.Map.Entry<String, Integer> entry : docMap.entrySet()) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(entry.getKey()).append(":").append(entry.getValue());
            }
            
            result.set(sb.toString());
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception {
        args = new String[]{
            "hdfs://localhost:9000/exp2/input",
            "hdfs://localhost:9000/exp2/output",
            "hdfs://localhost:9000/exp2/stop_words_eng.txt"
        };
        
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: InvertedIndexSimple <in> <out> <stopwords>");
            System.exit(2);
        }
        
        Path outputPath = new Path(otherArgs[1]);
        FileSystem fileSystem = outputPath.getFileSystem(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndexSimple.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.addCacheFile(new URI(otherArgs[2]));
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}