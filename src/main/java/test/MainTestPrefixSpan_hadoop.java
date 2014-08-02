package test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.List;

import algorithms.sequentialpatterns.BIDE_and_prefixspan.AlgoPrefixSpan;
import algorithms.sequentialpatterns.BIDE_and_prefixspan.SequentialPattern;
import algorithms.sequentialpatterns.BIDE_and_prefixspan.SequentialPatterns;
import input.sequence_database_list_integers.SequenceDatabase;


/**
 * Example of how to use the PrefixSpan algorithm in source code.
 * @author Philippe Fournier-Viger
 */
public class MainTestPrefixSpan_hadoop {

	public static void main(String [] arg) throws IOException{    
		// Load a sequence database
		SequenceDatabase sequenceDatabase = new SequenceDatabase(); 
		sequenceDatabase.loadFile(fileToPath("~/Dropbox/master/project/code/contextPrefixSpan.txt"));
		// print the database to console
		sequenceDatabase.print();
		
		// Create an instance of the algorithm 
		AlgoPrefixSpan algo = new AlgoPrefixSpan(); 
//		algo.setMaximumPatternLength(3);
		
		// execute the algorithm with minsup = 50 %
		SequentialPatterns patterns = algo.runAlgorithm(sequenceDatabase, 0.2, null);    
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
//                String url = java.net.URLDecoder.decode("/Users/finito/Dropbox/master/project/code/contextPrefixSpan.txt","UTF-8");
                String url = java.net.URLDecoder.decode("/Users/finito/Dropbox/master/project/code/dario3.txt","UTF-8");

                return url;
//                return java.net.URLDecoder.decode(url.getPath(),"UTF-8");

        }
}