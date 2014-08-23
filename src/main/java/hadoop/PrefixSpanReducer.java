package hadoop;

import org.apache.hadoop.mapred.Reducer;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class PrefixSpanReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

    private static long noOfSequences;
    private static Double Support;

    public void configure(JobConf job) {
        noOfSequences = Long.parseLong(job.get("noSeq"));
        Support = Double.valueOf(job.get("Support"));
    }

    private static Text results = new Text();

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        long support = 0;
        long totalSupport;
        while (values.hasNext()) {
            support = support + Long.valueOf(values.next().toString());
        }
        totalSupport = (support*100/noOfSequences);
        if (totalSupport >= Support*100) {
            results.set(String.valueOf(totalSupport) + "%");
            output.collect(key, results);
        }
    }
}
