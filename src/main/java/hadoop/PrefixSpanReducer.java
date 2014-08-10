package hadoop;

import algorithms.PrefixSpan.AlgoPrefixSpan;
import input.sequence_database_list_integers.Sequence;
import input.sequence_database_list_integers.SequenceDatabase;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.mapred.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PrefixSpanReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>{

    private static Text results = new Text();

    //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
//    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException
//    {
//
//
//        // Create an instance of the algorithm
//        // it is currently giving heap memory problems when doing some serious stuff, please take care of it
//        AlgoPrefixSpan algo = new AlgoPrefixSpan();
//        //create a sequence database and feed it with the mapper chewed data
//        SequenceDatabase sequenceDatabase = new SequenceDatabase();
//        /*iterates through all the values available with a key and add them together and give the
//        final result as the key and sum of its values*/
//        while (values.hasNext())
//        {
//            sequenceDatabase.addSequence(values.next().toString().split(" "));
//        }
//        //set a default 0.5 support, fixme
//        double support = 0.5;
//        algo.runAlgorithm(sequenceDatabase, support, null);
//        stats.set(algo.getStatistics(sequenceDatabase.size()));
//
//
//        output.collect(key, stats);
//    }
    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException
    {


        // Create an instance of the algorithm
        // it is currently giving heap memory problems when doing some serious stuff, please take care of it
        AlgoPrefixSpan algo = new AlgoPrefixSpan();
        //create a sequence database from the mappers output
        SequenceDatabase sequenceDatabase = new SequenceDatabase();



        while (values.hasNext())
        {
            String[] tokens = values.next().toString().split("[)]");
            if (tokens[0].isEmpty()){
                continue;
            }
            Sequence sequence = new Sequence(sequenceDatabase.size());

            // for each token in this line
            for (String token : tokens) {

                token=token.replaceAll("[(]","");
                String[] subtoken=token.split(" ");
                if (token.replaceAll("[ ]","").equals("endOfSequence")){
                    sequenceDatabase.addMappedSequence(sequence);
                    break;
                }
                // we parse it as an integer and add it to
                // the current itemset.
                List<Integer> itemset = new ArrayList<Integer>();
                for (String i :subtoken){
                    if (i==null){
                        continue;
                    }
                    itemset.add(Integer.parseInt(i));
                }
                sequence.addItemset(itemset);
            }

        }
        double support = 0.5;
        algo.runAlgorithm(sequenceDatabase, support, null);
        results.set(algo.getStatistics(sequenceDatabase.size()));

        output.collect(key, results);
    }

}
