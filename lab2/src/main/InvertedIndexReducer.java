package src.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

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
