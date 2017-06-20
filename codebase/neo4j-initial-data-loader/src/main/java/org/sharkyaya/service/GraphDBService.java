package org.sharkyaya.service;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;

public final class GraphDBService {

	private GraphDatabaseService graphDB;

	public GraphDatabaseService getDBInstance() {
		if (graphDB == null) {
			initDatabase();
		}
		return graphDB;
	}

	public void initDatabase() {
		//File pathToConfig = new File("E://WorkBase//hackathon//config//neo4j.conf");
		graphDB = new HighlyAvailableGraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder(new File("E:/WorkBase/hackathon/data/crime.db"))
				.loadPropertiesFromFile("E://WorkBase//hackathon//config//neo4j.conf").newGraphDatabase();
		registerShutdownHook(graphDB);
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

}
