package src;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            Text fileName_lineOffset = new Text(fileName);
            String[] buffer = value.toString().toLowerCase().split("[\\p{Punct}\\s]+");
            for (String itr : buffer) {
                if (itr.length() != 0 && !itr.matches(".*\\d.*")) {
                    context.write(new Text(itr), fileName_lineOffset);
                }
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> timesMap = new HashMap<>();
            int times = 0;
            StringBuilder all = new StringBuilder();
            for (Text t: values) {
                String filename = t.toString();
                timesMap.put(filename, timesMap.getOrDefault(filename, 0) + 1);
                times += 1;
            }
            all.append(String.format("%.2f", ((double) times) / timesMap.size()));
            all.append(",");
            for (Map.Entry<String, Integer> entry: timesMap.entrySet()) {
                all.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
            }
            context.write(key, new Text(all.toString()));        
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}