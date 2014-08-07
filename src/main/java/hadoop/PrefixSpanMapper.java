package hadoop;

import org.apache.hadoop.mapred.Mapper;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import algorithms.PrefixSpan.*;
import input.sequence_database_list_integers.*;

public class PrefixSpanMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>{
    //hadoop supported data types
    private static Text stats = new Text();
    private static IntWritable item = new IntWritable();

    //map method that populates the sequence database for each mapper
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException
    {
        // Load a sequence database
        SequenceDatabase sequenceDatabase = new SequenceDatabase();
        sequenceDatabase.addSequence(value.toString().split(" "));

        // Create an instance of the algorithm
        // it is currently giving heap memory problems when doing some serious stuff, please take care of it
        AlgoPrefixSpan algo = new AlgoPrefixSpan();
        //set a default 0.5 support, fixme
        double support = 0.5;
        algo.runAlgorithm(sequenceDatabase, support, null);
        stats.set(algo.getPatterns());
        item.set((int) key.get());
        output.collect(item, stats);
    }

}