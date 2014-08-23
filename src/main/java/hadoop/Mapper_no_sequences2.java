package hadoop;

//import org.apache.commons.codec.binary.StringUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper_no_sequences2 extends Mapper<LongWritable, Text, Text, Text> {

    private static Text result = new Text();
//    private static Text unique = new Text("size");
    private static Text newkey = new Text();

    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        newkey.set(key.toString());
        int sum =0;

        String[] size = values.toString().split("\t");
        for (String i : size) {
            if (i.contains("size")) {
                try {
                    String str = i.split(" ")[1];
                    int index = str.indexOf("\n");
                    str = str.substring(0,index);
                    sum = sum+Integer.parseInt(str);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
                System.out.println(sum);
            }
        }
        result.set("size "+String.valueOf(sum));
            context.write(newkey, result);
    }
}
