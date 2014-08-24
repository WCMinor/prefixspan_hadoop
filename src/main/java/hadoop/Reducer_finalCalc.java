package hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.*;


public class Reducer_finalCalc extends Reducer<Text, Text, Text, Text>{

    private static Text itemset = new Text();
    private static Text finalsupport = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Double Support = Double.valueOf(conf.get("Support"));
        long support;
        String item = null;
        String value = null;
        Map<String, String> listresults = new HashMap<String, String>();

        for (Text val : values){
            item = val.toString().split("\t")[0];
            value = val.toString().split("\t")[1];
            if (listresults.get(item) != null){
                long oldvalue = Long.valueOf(listresults.get(item));
                String newvalue = String.valueOf(oldvalue + Long.valueOf(value));
                listresults.put(item,newvalue);
                listresults.put(item,newvalue);
            }
            else{
                listresults.put(item,value);
            }
        }

        for (String i : listresults.keySet()){
            if (!i.contentEquals("size")){
                itemset.set(i);
                support = (Long.valueOf(listresults.get(i)) * 100) / Long.valueOf(listresults.get("size"));
                if (support >= Support * 100) {
                    finalsupport.set(String.valueOf(support) + "%");
                    context.write(itemset, finalsupport);
                }
            }
        }
    }
}
