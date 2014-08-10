package hadoop;

import org.apache.hadoop.mapred.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import algorithms.PrefixSpan.*;
import input.sequence_database_list_integers.*;

public class PrefixSpanMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
    //hadoop supported data types
    private static Text chewed = new Text();
    private static IntWritable item = new IntWritable();

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        item.set(key.hashCode());
//        item.set(1);
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
        for (Sequence sequence : sequenceDatabase.getSequences()) {
            chewed.set(String.valueOf(sequence)+"endOfSequence");//last append indicates the end of the sequence
            output.collect(item, chewed);
        }
    }
}