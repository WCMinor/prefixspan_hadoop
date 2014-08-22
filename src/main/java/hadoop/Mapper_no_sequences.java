package hadoop;

//import org.apache.commons.codec.binary.StringUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Mapper_no_sequences extends Mapper<LongWritable, Text, Text, Text> {

    private static Text sum = new Text();
    private static Text newKey = new Text();


    public void map(LongWritable key, Text values, Context context) throws IOException {

        String[] size = values.toString().split("\t");
        if (size[0].equals("size")){
            sum.set(size[1]);
        }
        try {
            context.write(newKey, sum);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
