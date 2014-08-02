package test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;

import algorithms.associationrules.closedrules.AlgoClosedRules;
import algorithms.frequentpatterns.charm.AlgoCharm_Bitset;
import input.transaction_database_list_integers.TransactionDatabase;
import patterns.itemset_array_integers_with_tids_bitset.Itemsets;

/**
 * Example of how to mine closed association rules from the source code.
 * @author Philippe Fournier-Viger (Copyright 2008)
 */
public class MainTestClosedAssociationRules_saveToFile {

	public static void main(String [] arg) throws IOException{
		// input and output file paths
		String input = fileToPath("contextZart.txt");
		String output = ".//output.txt";
		
		// the threshold
		double minsupp = 0.60;
		double  minconf = 0.60;
		
		// Loading the transaction database
		TransactionDatabase database = new TransactionDatabase();
		try {
			database.loadFile(input);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// STEP 1: Applying the Charm algorithm to find frequent closed itemsets
		AlgoCharm_Bitset algo = new AlgoCharm_Bitset();
		Itemsets patterns = algo.runAlgorithm(null, database, minsupp, true, 10000);
		algo.printStats();
		
		
		// STEP 2: Generate all rules from the set of frequent itemsets (based on Agrawal & Srikant, 94)
		AlgoClosedRules algoClosedRules = new AlgoClosedRules();
		algoClosedRules.runAlgorithm(patterns, output, database.size(), minconf);
		algoClosedRules.printStats();

	}
	
	public static String fileToPath(String filename) throws UnsupportedEncodingException{
		URL url = MainTestClosedAssociationRules_saveToFile.class.getResource(filename);
		 return java.net.URLDecoder.decode(url.getPath(),"UTF-8");
	}
}
