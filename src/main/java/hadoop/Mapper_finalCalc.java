package hadoop;

//import org.apache.commons.codec.binary.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Mapper_finalCalc extends Mapper<LongWritable, Text, Text, Text> {


    private static Text result = new Text();
    private static Text newkey = new Text();

    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        long numofSeqs = Long.valueOf(conf.get("numofSeqs"));
        Double Support = Double.valueOf(conf.get("Support"));
        //only one reducer as is the final wrap
//        newkey.set("one reducer");
        String item = null;
        String value = null;
        Map<String, String> listresults = new HashMap<String, String>();
        String[] splits = values.toString().split("\n");
        for (String val : splits){

            if (val.split("\t").length ==3) {
                item = val.split("\t")[1];
                value = val.split("\t")[2];
            }
            else if(val.split("\t").length ==2){
                item = val.split("\t")[0];
                value = val.split("\t")[1];
            }
            if (listresults.get(item) != null){
                long oldvalue = Long.valueOf(listresults.get(item));
                String newvalue = String.valueOf(oldvalue + Long.valueOf(value));
                listresults.put(item,newvalue);
            }
            else{
                listresults.put(item,value);
            }
        }
        for (String i : listresults.keySet()){
            if (Long.valueOf(listresults.get(i)) >= numofSeqs*Support) {
                if (!i.contentEquals("size")) {
                    result.set(listresults.get(i));
                    newkey.set(i);
                    context.write(newkey, result);
                }
            }
        }
    }
}

