package hadoop;

//import org.apache.commons.codec.binary.StringUtils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

public class PrefixSpanReducer_no_sequences extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

    private static Text results = new Text();


    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        long noOfSequences=0;
        while (values.hasNext()) {
            noOfSequences= noOfSequences+Long.parseLong(values.next().toString());
        }
        results.set(String.valueOf(noOfSequences));
        output.collect(key, results);
    }
}
