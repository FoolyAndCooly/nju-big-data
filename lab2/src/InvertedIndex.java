import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndex {

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            Text word = new Text();

            StringBuilder wordBuilder = new StringBuilder();
            String text = value.toString().toLowerCase();
            for (int i = 0; i < text.length(); i++) {
                char c = text.charAt(i);
                if (Character.isLetterOrDigit(c)) {
                    wordBuilder.append(c);
                } else {
                    if (wordBuilder.length() > 0) {
                        word.set(wordBuilder.toString());
                        context.write(new Text(word.toString() + "#" + fileName), new IntWritable(1));
                        wordBuilder.setLength(0);
                    }
                }
            }

            if (wordBuilder.length() > 0) {
                word.set(wordBuilder.toString());
                context.write(new Text(word.toString() + "#" + fileName), new IntWritable(1));
            }
        }
    }


    public static class Combine extends Reducer<Text, IntWritable, Text, IntWritable> {
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum ++;
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class Partition extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
        private String preDoc = null;
        private String preWord = null;
        private String str = "";
        private int wordPerDocCount = 0;
        private int totalWordCount = 0;
        private int docCount = 0;
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String word = key.toString().split("#")[0];
            String doc = key.toString().split("#")[1];
            if(preWord == null) {
                preWord = word;
            }
            if(preDoc == null) {
                docCount++;
                preDoc = doc;
            }
            if (!word.equals(preWord)) {
                str += preDoc + ":" + wordPerDocCount + ";";
                str = String.format("%.2f", (double)totalWordCount/docCount) + "," + str;
                context.write(new Text(preWord), new Text(str));
                preWord = word;
                preDoc = doc;
                wordPerDocCount = 0;
                totalWordCount = 0;
                docCount = 1;
                str = "";
            }

            if(!doc.equals(preDoc)) {
                docCount++;
                str += preDoc + ":" + wordPerDocCount + ";";
                preDoc = doc;
                wordPerDocCount = 0;
            }

            for (IntWritable val : values) {
                wordPerDocCount += val.get();
                totalWordCount += val.get();
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            str += preDoc + ":" + wordPerDocCount + ";";
            str = String.format("%.2f", (double)totalWordCount/docCount) + "," + str;
            context.write(new Text(preWord), new Text(str));

            super.cleanup(context);
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master001:9000");
        if(args.length != 2) {
            System.err.println("Usage: Relation <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "InvertedIndex");//设置环境参数
        job.setJarByClass(InvertedIndex.class);//设置整个程序的类名
        job.setMapperClass(Map.class);//设置Mapper类
        job.setCombinerClass(Combine.class);//设置combiner类
        job.setPartitionerClass(Partition.class);//设置Partitioner类
        job.setReducerClass(Reduce.class);//设置reducer类
        job.setOutputKeyClass(Text.class);//设置Mapper输出key类型
        job.setOutputValueClass(IntWritable.class);//设置Mapper输出value类型
        FileInputFormat.addInputPath(job, new Path(args[0]));//输入文件目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出文件目录
        System.exit(job.waitForCompletion(true) ? 0 : 1);//参数true表示检查并打印 Job 和 Task 的运行状况
    }
}