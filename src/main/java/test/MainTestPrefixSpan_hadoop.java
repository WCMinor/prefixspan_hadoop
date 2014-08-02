package test;

import algorithms.sequentialpatterns.BIDE_and_prefixspan.AlgoPrefixSpan;
import algorithms.sequentialpatterns.BIDE_and_prefixspan.SequentialPattern;
import algorithms.sequentialpatterns.BIDE_and_prefixspan.SequentialPatterns;
import input.sequence_database_list_integers.SequenceDatabase;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.List;



/**
 * Example of how to use the PrefixSpan algorithm in source code.
 * @author Philippe Fournier-Viger
 */
public class MainTestPrefixSpan_hadoop {

	public static void main(String [] arg) throws IOException{    
            
         		// get the support from the second argument
                double support;
                    try {support = Double.parseDouble(arg[1]);}
                    catch ( NumberFormatException e ) {
                        System.out.println( "The second argument \"support\" must be a number." );
                        return;
         }
		// Load a sequence database
		SequenceDatabase sequenceDatabase = new SequenceDatabase();
                // get the sequence filename from the first argument
		sequenceDatabase.loadFile(fileToPath(arg[0]));
		// print the database to console
		sequenceDatabase.print();
		
		// Create an instance of the algorithm 
		AlgoPrefixSpan algo = new AlgoPrefixSpan(); 
//		algo.setMaximumPatternLength(3);
		

		SequentialPatterns patterns = algo.runAlgorithm(sequenceDatabase, support, null);    
		algo.printStatistics(sequenceDatabase.size());
//		System.out.println(" == PATTERNS ==");
//		for(List<SequentialPattern> level : patterns.levels) {
//			for(SequentialPattern pattern : level){
//				System.out.println(pattern + " support : " + pattern.getAbsoluteSupport());
//			}
//		}
	}
	
	public static String fileToPath(String filename) throws UnsupportedEncodingException{
//		URL url = MainTestPrefixSpan_saveToMemory.class.getResource(filename);
                String url = java.net.URLDecoder.decode(filename,"UTF-8");

                return url;
//                return java.net.URLDecoder.decode(url.getPath(),"UTF-8");

        }
}