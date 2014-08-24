package hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Reducer_getnumOfSeqs extends Reducer<Text, Text, Text, Text>{

    private static Text newkey = new Text();
    private static Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long sum =0L;
        for (Text val : values) {
            sum = sum+Long.valueOf(val.toString());
        }
                newkey.set("size");
                result.set(String.valueOf(sum));
                context.write(newkey, result);
    }
}
