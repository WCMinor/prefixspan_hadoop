package hadoop;

/**
 * Created by WCMinor on 05/08/2014.
 */


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.util.*;

import java.io.*;


public class PrefixSpanHadoop extends Configured implements Tool{
    public int run(String[] arg) throws IOException, ClassNotFoundException, InterruptedException {


        //creating a JobConf object and assigning a Hadoop job name for identification purposes
        Configuration prefixconf = new Configuration();
        String support = arg[1];
        prefixconf.set("Support", support);
        Job PrefixSpan = new Job(prefixconf);
        PrefixSpan.setJarByClass(PrefixSpanHadoop.class);
        FileSystem fs = FileSystem.get(new Configuration());
        Path temp_output = new Path("temp_output");
        if (fs.exists(temp_output)) {
            fs.delete(temp_output, true); //Delete existing Directory
        }
        PrefixSpan.setJobName("Mining the Data");

        //Setting configuration object with the Data Type of output Key and Value
        PrefixSpan.setOutputKeyClass(Text.class);
        PrefixSpan.setOutputValueClass(Text.class);

        //Providing the mapper and reducer class names
        PrefixSpan.setMapperClass(PrefixSpanMapper.class);
        PrefixSpan.setNumReduceTasks(0);

        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.addInputPath(PrefixSpan, new Path(arg[0]));
        PrefixSpan.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(PrefixSpan, temp_output);
        // Execute job
        PrefixSpan.waitForCompletion(true);

        long thistask = 0;
        long lastask;
        while (true) {
            lastask =thistask;
            //creating a JobConf object and assigning a Hadoop job name for identification purposes
            Configuration NoSequencesConf = new Configuration();
            Job NoSequences = new Job(NoSequencesConf);
            NoSequences.setJarByClass(PrefixSpanHadoop.class);
            Path noOfSequences = new Path("noOfSequences");
            if (fs.exists(noOfSequences)) {
                fs.delete(noOfSequences, true); //Delete existing Directory
            }
            NoSequences.setJobName("Counting number of sequences");
            //Setting configuration object with the Data Type of output Key and Value
            NoSequences.setOutputKeyClass(Text.class);
            NoSequences.setOutputValueClass(Text.class);
            //Providing the mapper and reducer class names
            NoSequences.setMapperClass(Mapper_no_sequences.class);
            NoSequences.setNumReduceTasks(0);
            //the hdfs input and output directory to be fetched from the command line
            FileInputFormat.addInputPath(NoSequences, temp_output);
            NoSequences.setInputFormatClass(NLinesInputFormat.class);
            FileOutputFormat.setOutputPath(NoSequences, noOfSequences);
            NoSequences.waitForCompletion(true);
            fs.delete(temp_output, true);
            fs.rename(noOfSequences, temp_output);
            //check if it has converged
            Counters counters = NoSequences.getCounters();
            thistask = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();
            if (thistask == lastask) {
                break;
            }

        }

        Configuration finalCalcConf = new Configuration();
        finalCalcConf.set("Support", support);
        Job finalCalc = new Job(finalCalcConf);
        finalCalc.setJobName("gathering all sequences and filtering by support");
        finalCalc.setJarByClass(PrefixSpanHadoop.class);
        //Providing the mapper and reducer class names
        finalCalc.setMapperClass(Mapper_finalCalc.class);
        finalCalc.setReducerClass(Reducer_finalCalc.class);
        Path output = new Path("output");
        if (fs.exists(output)) {
            fs.delete(output, true); //Delete existing Directory
        }
        FileOutputFormat.setOutputPath(finalCalc, output);
        FileInputFormat.addInputPath(finalCalc, temp_output);
        //Setting configuration object with the Data Type of output Key and Value
        finalCalc.setOutputKeyClass(Text.class);
        finalCalc.setOutputValueClass(Text.class);
        finalCalc.setInputFormatClass(NLinesInputFormat.class);
        finalCalc.waitForCompletion(true);

        return 0;

    }
    public static void main(String[] arg) throws Exception{
        //         		get the support from the second argument

        if (arg.length != 2){
            System.out.println("The number of arguments must be 2, \"input path\" and \"support\"");
            System.exit(2);
        }
        int res = ToolRunner.run(new Configuration(), new PrefixSpanHadoop(), arg);
        System.exit(res);
    }
    public static String fileToPath(String filename) throws UnsupportedEncodingException {
        return java.net.URLDecoder.decode(filename,"UTF-8");
    }
}
