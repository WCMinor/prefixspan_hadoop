package test;

import java.io.UnsupportedEncodingException;
import java.net.URL;

import algorithms.associationrules.TopKRules_and_TNR.AlgoTNR;
import algorithms.associationrules.TopKRules_and_TNR.Database;
import algorithms.associationrules.TopKRules_and_TNR.RuleG;
import datastructures.redblacktree.RedBlackTree;

/**
 * Example of how to use the TNR algorithm in source code.
 * @author Philippe Fournier-Viger (Copyright 2010)
 */
public class MainTestTNR {

	public static void main(String [] arg) throws Exception{
		// Load database into memory
		Database database = new Database(); 
		database.loadFile(fileToPath("contextIGB.txt"));

		int k = 10; 
		double minConf = 0.5; 
		int delta =  2;
		
		AlgoTNR algo = new AlgoTNR();
		RedBlackTree<RuleG> kRules = algo.runAlgorithm(k, minConf, database,  delta );
		algo.writeResultTofile(".//output.txt");   // to save results to file
		
		algo.printStats();
	}



	public static String fileToPath(String filename) throws UnsupportedEncodingException{
		URL url = MainTestTNR.class.getResource(filename);
		 return java.net.URLDecoder.decode(url.getPath(),"UTF-8");
	}
}
