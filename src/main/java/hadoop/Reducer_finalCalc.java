package hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.*;


public class Reducer_finalCalc extends Reducer<Text, Text, Text, Text>{

    private static Text itemset = new Text();
    private static Text finalsupport = new Text();

    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        double Support = Double.valueOf(conf.get("Support"));
        long noOfSequences = Long.valueOf(conf.get("noOfSequences"));
        long support = 0;
        long totalSupport;
        itemset.set(key);
        for (Text val : value) {
            support = support+Long.valueOf(String.valueOf(val));
        }
        totalSupport = (support * 100 / noOfSequences);
            if (totalSupport >= Support * 100) {
                finalsupport.set(totalSupport + "%");
                context.write(itemset, finalsupport);
            }

    }
}
