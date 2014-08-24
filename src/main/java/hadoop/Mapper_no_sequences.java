package hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Mapper_no_sequences extends Mapper<LongWritable, Text, Text, Text> {

    private static Text result = new Text();
    private static Text newkey = new Text();

    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        newkey.set(key.toString());
        long sum =0L;

        String[] size = values.toString().split("\t");
        for (int i = 0; i < size.length; i++) {
            if (size[i].contains("size")) {
                sum = sum+Long.valueOf(size[i+1].split("\n")[0]);
            }
        }

        result.set("size "+String.valueOf(sum));
            context.write(newkey, result);
    }
}
