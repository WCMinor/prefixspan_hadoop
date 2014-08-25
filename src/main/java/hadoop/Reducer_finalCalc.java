package hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import org.apache.hadoop.io.*;


public class Reducer_finalCalc extends Reducer<Text, Text, Text, Text>{

    private static Text itemset = new Text();
    private static Text finalsupport = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        long numofSeqs = Long.valueOf(conf.get("numofSeqs"));
        Double Support = Double.valueOf(conf.get("Support"));
        long value = 0L;
        for (Text val : values) {
            value = value + Long.valueOf(val.toString());
        }
            long support = (value * 100) / numofSeqs;

        if (Long.valueOf(value) >= numofSeqs*Support) {
            finalsupport.set(String.valueOf(support) + "%");
            itemset.set(key);
            context.write(itemset, finalsupport);
        }
    }
}