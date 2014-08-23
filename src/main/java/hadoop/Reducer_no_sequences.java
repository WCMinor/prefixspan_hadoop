package hadoop;


/**
 * Created by WCMinor on 22/08/2014.
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Reducer_no_sequences extends Reducer<Text, Text, Text, Text> {

    private static Text result = new Text();
    private static Text keyOut = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        keyOut.set(key);

        for (Text val : values) {
            result.set(val);
            context.write(keyOut, result);
        }
    }
}