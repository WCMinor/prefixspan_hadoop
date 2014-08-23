package hadoop;

//import org.apache.commons.codec.binary.StringUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper_finalCalc extends Mapper<LongWritable, Text, Text, Text> {

    private static Text result = new Text();
    //    private static Text unique = new Text("size");
    private static Text newkey = new Text();

    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        long support = 0;
        String item = null;
//        newkey.set(key.toString());

        String[] str = values.toString().split("\n");
        for (String i : str) {
            if (!i.contains("size")) {
                item = i.split("\t")[0];
                support = Long.valueOf(i.split("\t")[1]);
                result.set(String.valueOf(support));
                newkey.set(item);
                context.write(newkey, result);
            }
        }
    }
}

