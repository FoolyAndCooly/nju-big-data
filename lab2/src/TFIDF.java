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

public class TFIDF{

    public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
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

    public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> perWorkMap = new HashMap<>();
            int DocNum = 0;
            for (Text t: values) {
                DocNum++;
                String[] buffer = t.toString().split("-");
                String workName = buffer[1];
                perWorkMap.put(workName, perWorkMap.getOrDefault(workName, 0) + 1);
            }
            
            for (Map.Entry<String, Integer> entry: perWorkMap.entrySet()) {
                context.write(new Text(entry.getKey()), new Text(key.toString() + "," + entry.getValue() + "-" + String.valueOf(Math.log10(40.0 / 1 + DocNum))));
            }     
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(TFIDF.class);

        job.setMapperClass(TFIDFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(TFIDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}