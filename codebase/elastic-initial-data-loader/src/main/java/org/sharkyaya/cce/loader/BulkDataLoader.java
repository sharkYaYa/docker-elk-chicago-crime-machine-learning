package org.sharkyaya.cce.loader;

import java.net.UnknownHostException;

import org.sharkyaya.cce.loader.parser.CrimeFeedParser;

public class BulkDataLoader {

	public static void main(String[] args) throws UnknownHostException {
		//CrimeFeedParser.parseAndInsert("/Users/Subhasish/Documents/eShark/Work/GitWork/chicago-crime-elastic/chicago-crime-elastic/crime-data/HalfCrimeDat.csv");
		//CrimeFeedParser.parseAndInsert("/Users/Subhasish/Documents/eShark/Work/GitWork/chicago-crime-elastic/chicago-crime-elastic/crime-data/ChicagoCrimeRepo.csv");
		
	for(int i=1; i<=7;i++)
		{
			long start = System.currentTimeMillis();
			CrimeFeedParser.parseAndInsert("/Users/Subhasish/Documents/eShark/configured/elastic-stack/crime-data-files/ChicagoCrimeRepo-"+i);
			System.out.println("Time Taken: " + (System.currentTimeMillis() -start));
		}
		
	}

}
