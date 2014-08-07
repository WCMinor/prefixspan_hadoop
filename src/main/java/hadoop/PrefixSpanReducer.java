package hadoop;

import org.apache.hadoop.mapred.Reducer;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PrefixSpanReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{

    private static Text sum = new Text();

    //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException
    {
//            /*iterates through all the values available with a key and add them together and give the
//            final result as the key and sum of its values*/
        StringBuffer results = new StringBuffer();
        while (values.hasNext())
        {
            results.append(values.next());
        }
        sum.set(results.toString());

        output.collect(key, sum);
    }


}
