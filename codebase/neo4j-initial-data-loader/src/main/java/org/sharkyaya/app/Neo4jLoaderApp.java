package org.sharkyaya.app;

import java.io.FileNotFoundException;

import org.sharkyaya.loader.CrimeFeedNeo4jLoader;

public class Neo4jLoaderApp extends CrimeFeedNeo4jLoader{

	public static void main(String[] args) throws FileNotFoundException {
		Neo4jLoaderApp app= new Neo4jLoaderApp();
		app.parseAndInsert("E:/WorkBase/hackathon/data/crime-data.csv");
	}

}
