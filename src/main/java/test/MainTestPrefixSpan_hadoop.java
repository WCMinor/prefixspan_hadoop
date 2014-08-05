package test;

import algorithms.sequentialpatterns.BIDE_and_prefixspan.AlgoPrefixSpan;
import input.sequence_database_list_integers.SequenceDatabase;
import java.io.IOException;
import java.io.UnsupportedEncodingException;



/**
 * Example of how to use the PrefixSpan algorithm in source code.
 * @author Philippe Fournier-Viger
 */
public class MainTestPrefixSpan_hadoop {
    //    private static double support;
    public static void main(String [] arg) throws IOException{

//         		get the support from the second argument

        if (arg.length != 2){
            System.out.println("The number of arguments must be 2, \"input file\" and \"support\"");
            System.exit(2);
        }
        double support;
        try {
            support = Double.parseDouble(arg[1]);
        }catch ( NumberFormatException e ) {
            System.out.println( "The second argument \"support\" must be a number.");
            return;
        }
        // Load a sequence database
        SequenceDatabase sequenceDatabase = new SequenceDatabase();
        try {
            // get the sequence filename from the first argument
            sequenceDatabase.loadFile(fileToPath(arg[0]));
        }
        catch (IOException e) {
            System.exit(2);
        }
        // print the database to console
//      	sequenceDatabase.print();

        // Create an instance of the algorithm
        AlgoPrefixSpan algo = new AlgoPrefixSpan();
//		algo.setMaximumPatternLength(3);


        algo.runAlgorithm(sequenceDatabase, support, null);
        algo.printStatistics(sequenceDatabase.size());

    }

    public static String fileToPath(String filename) throws UnsupportedEncodingException{

        return java.net.URLDecoder.decode(filename,"UTF-8");

    }
}