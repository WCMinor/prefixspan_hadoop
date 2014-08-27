package hadoop;

/**
 * Created by WCMinor on 05/08/2014.
 */


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

import java.io.*;


public class PrefixSpanHadoop extends Configured implements Tool{
    public int run(String[] arg) throws IOException, ClassNotFoundException, InterruptedException {


//        //First Job, here the algorithm is ran
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

        //Second job, iterating mapper tasks until we get a list of sequences and the count of each
        long thistask = 0;
        long lastask;
        while (true) {
            lastask =thistask;
            Configuration NoSequencesConf = new Configuration();
            Job NoSequences = new Job(NoSequencesConf);
            NoSequences.setJarByClass(PrefixSpanHadoop.class);
            Path Sequences = new Path("Sequences");
            if (fs.exists(Sequences)) {
                fs.delete(Sequences, true); //Delete existing Directory
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
            FileOutputFormat.setOutputPath(NoSequences, Sequences);
            NoSequences.waitForCompletion(true);
            fs.delete(temp_output, true);
            fs.rename(Sequences, temp_output);
            //check if it has converged
            Counters counters = NoSequences.getCounters();
            thistask = counters.findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();
            if (thistask == lastask) {
                break;
            }
        }

        //Thirth job getting the total number of sequences found
         Configuration getnumOfSeqsconf = new Configuration();
        Job getnumOfSeqs = new Job(getnumOfSeqsconf);
        getnumOfSeqs.setJobName("get total number of sequences");
        getnumOfSeqs.setJarByClass(PrefixSpanHadoop.class);
        //Providing the mapper and reducer class names
        getnumOfSeqs.setMapperClass(Mapper_getnumOfSeqs.class);
        getnumOfSeqs.setReducerClass(Reducer_getnumOfSeqs.class);
        Path numOfSeqs = new Path("numOfSeqs");
        if (fs.exists(numOfSeqs)) {
            fs.delete(numOfSeqs, true); //Delete existing Directory
        }
        FileOutputFormat.setOutputPath(getnumOfSeqs, numOfSeqs);
        FileInputFormat.addInputPath(getnumOfSeqs, temp_output);
        //Setting configuration object with the Data Type of output Key and Value
        getnumOfSeqs.setOutputKeyClass(Text.class);
        getnumOfSeqs.setOutputValueClass(Text.class);
        getnumOfSeqs.setInputFormatClass(NLinesInputFormat.class);
        getnumOfSeqs.waitForCompletion(true);


        //Last job wrapping up everything
        Configuration finalCalcConf = new Configuration();
        //get the number of sequences from the previous job scanning the output folder
        String numofSeqs = "0";
        FileStatus[] fss = fs.listStatus(numOfSeqs);
        for (FileStatus status : fss) {
            Path path = status.getPath();
            BufferedReader bfr=new BufferedReader(new InputStreamReader(fs.open(path)));
            if (bfr.ready()){
                String[] line = bfr.readLine().split("\t");
                if (line[0].contains("size")) {
                    numofSeqs = line[1];
                }
            }
        }
        finalCalcConf.set("numofSeqs", numofSeqs);
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
