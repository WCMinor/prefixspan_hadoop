package hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Mapper_getnumOfSeqs extends Mapper<LongWritable, Text, Text, Text> {

    private static Text result = new Text();
    private static Text newkey = new Text();

    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        String value;
        String[] splits = values.toString().split("\n");
        for (String val : splits){
            if (val.split("\t")[1].contentEquals("size")) {
                value = val.split("\t")[2];
                newkey.set("size");
                result.set(value);
                context.write(newkey, result);
            }
        }
    }
}
