package hadoop;

import org.apache.hadoop.mapred.Reducer;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

//public class PrefixSpanReducer extends MapReduceBase implements Reducer<Integer, IntWritable, Text, IntWritable>
public class PrefixSpanReducer extends MapReduceBase implements Reducer<Integer, String, Integer, StringBuffer>

{
    //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
    public void reduce(Integer key, Iterator<String> values, OutputCollector<Integer, StringBuffer> output, Reporter reporter) throws IOException
    {
        StringBuffer results = null;
            /*iterates through all the values available with a key and add them together and give the
            final result as the key and sum of its values*/
        while (values.hasNext())
        {
            results.append(values.toString());
        }
        output.collect(key, results);
    }


}
