package hadoop;

import algorithms.PrefixSpan.AlgoPrefixSpan;
import input.sequence_database_list_integers.SequenceDatabase;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class PrefixSpanMapper_no_sequences extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
    //hadoop supported data types
    private static Text supportvalue = new Text();
    private static Text patternkey = new Text();


    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        patternkey.set("unique");
        // Create an instance of the algorithm
        // it is currently giving heap memory problems when doing some serious stuff, please take care of it
        AlgoPrefixSpan algo = new AlgoPrefixSpan();
        //create a sequence database and feed it with the mapper chewed data
        SequenceDatabase sequenceDatabase = new SequenceDatabase();
        /*iterates through all the values available with a key and add them together and give the
        final result as the key and sum of its values*/
        sequenceDatabase.addSequence(value.toString().split(" "));
        //set a default 0.5 support, fixme
        double support = 0.5;
        algo.runAlgorithm(sequenceDatabase, support, null);
            supportvalue.set(String.valueOf(sequenceDatabase.size()));
            output.collect(patternkey, supportvalue);
    }
}