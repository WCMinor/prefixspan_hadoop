package hadoop;

import org.apache.hadoop.mapred.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import algorithms.PrefixSpan.*;
import input.sequence_database_list_integers.*;

public class PrefixSpanMapper extends MapReduceBase implements Mapper<LongWritable, Text, Integer, String>{
    //hadoop supported data types
    private static String stats = new String();
    private static Integer item;

    //map method that populates the sequence database for each mapper
    public void map(LongWritable key, Text value, OutputCollector<Integer, String> output, Reporter reporter) throws IOException
//    public void map(LongWritable key, Text value, OutputCollector<Integer, String> output, Reporter reporter) throws IOException
    {
        // Load a sequence database
        SequenceDatabase sequenceDatabase = new SequenceDatabase();
        sequenceDatabase.addSequence(value.toString().split(" "));

        // Create an instance of the algorithm
        AlgoPrefixSpan algo = new AlgoPrefixSpan();
        //set a default 0.5 support, fixme
        double support = 0.5;
        algo.runAlgorithm(sequenceDatabase, support, null);
        stats = algo.getStatistics(1);

        item = value.hashCode();

        output.collect(item, stats);
    }

}