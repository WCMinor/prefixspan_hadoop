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
    private static Text chewed = new Text();
    private static IntWritable item = new IntWritable();

    //map method that populates the sequence database for each mapper
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException
    {
        chewed.set(value);
        item.set(1);
        output.collect(item, chewed);
    }

}