package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.*;
import algorithms.PrefixSpan.*;
import input.sequence_database_list_integers.*;

public class PrefixSpanMapper extends Mapper<LongWritable, Text, Text, Text>{
    //hadoop supported data types
    private static Text supportvalue = new Text();
    private static Text patternkey = new Text();



    public void map(LongWritable key, Text value, Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        Double Support = Double.valueOf(conf.get("Support"));
        //        item.set(key.hashCode());
        // Create an instance of the algorithm
        // it is currently giving heap memory problems when doing some serious stuff, please take care of it
        AlgoPrefixSpan algo = new AlgoPrefixSpan();
        //create a sequence database and feed it with the mapper chewed data
        SequenceDatabase sequenceDatabase = new SequenceDatabase();
        /*iterates through all the values available with a key and add them together and give the
        final result as the key and sum of its values*/
        sequenceDatabase.addSequence(value.toString().split(" "));
        algo.runAlgorithm(sequenceDatabase, Support, null);
        String[] listOfPatterns = algo.getFrequentPatterns(sequenceDatabase.size()).split("next");
        for (String pattern: listOfPatterns){
            patternkey.set(pattern.split("support")[0]);
            supportvalue.set(pattern.split("support")[1]);
            try {
                context.write(patternkey, supportvalue);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        patternkey.set("size");
        supportvalue.set(String.valueOf(sequenceDatabase.size()));
        try {
            context.write(patternkey, supportvalue);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}