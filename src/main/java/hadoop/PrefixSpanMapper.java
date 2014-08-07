package hadoop;

import org.apache.hadoop.mapred.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import algorithms.PrefixSpan.AlgoPrefixSpan;
import input.sequence_database_list_integers.*;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PrefixSpanMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

    //hadoop supported data types
    private final static IntWritable one = new IntWritable(1);
    private Text item = new Text();

    //map method that populates the sequence database for each mapper
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
    {
        // Load a sequence database
        SequenceDatabase sequenceDatabase = new SequenceDatabase();
        sequenceDatabase.addSequence(value.toString().split(" "));

        // Create an instance of the algorithm
        AlgoPrefixSpan algo = new AlgoPrefixSpan();
        //set a default 0.5 support, fixme
        double support = 0.5;
        algo.runAlgorithm(sequenceDatabase, support, null);

        //taking one line at a time and tokenizing the same
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        //iterating through all the words available in that line and forming the key value pair
        while (tokenizer.hasMoreTokens())
        {
            item.set(tokenizer.nextToken());
            //sending to output collector which inturn passes the same to reducer
            output.collect(item, one);
        }
    }
}