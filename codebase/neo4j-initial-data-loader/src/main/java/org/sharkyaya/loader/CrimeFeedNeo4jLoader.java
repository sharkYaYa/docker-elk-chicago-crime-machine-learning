package org.sharkyaya.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.sharkyaya.enums.Labels;
import org.sharkyaya.enums.RelTypes;
import org.sharkyaya.model.CrimeHeader;
import org.sharkyaya.service.GraphDBService;

public abstract class CrimeFeedNeo4jLoader {

	
	public void parseAndInsert(String fileWithPath) throws FileNotFoundException {
		GraphDBService graphDBService = new GraphDBService();
		GraphDatabaseService graphDb = graphDBService.getDBInstance();
		BufferedReader fileReader = new BufferedReader(new FileReader(new File(fileWithPath)));
		int BATCHSIZE = 30000;
		int count = 0;
		// Create the CSVFormat object with the header mapping
		CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(CrimeHeader.class);
		
		try(Transaction tx = graphDb.beginTx()){
			buildNeo4jSchemaIndex(graphDb);
			tx.success();
		}

		try (CSVParser csvFileParser = new CSVParser(fileReader, csvFileFormat)) {

			// Get a list of CSV file records
			// Read the CSV file records starting from the second record to skip
			// the header
			Transaction tx = graphDb.beginTx();

			try {
				
				for (CSVRecord csvRecord : csvFileParser) {
					/* To avoid loading the header as first record */

					if (csvRecord.getRecordNumber() == 1)
						continue;

					Map<String, String> map = csvRecord.toMap();
					buildChicagoCrimeData(map, graphDb);
					if (++count % BATCHSIZE == 0) {
						System.out.println("Neo4j Operartion: Commited "+BATCHSIZE+" Total records created "+count);
						tx.success();
						tx.close();
						tx = graphDb.beginTx();
					}
				}
				tx.success();
				tx.close();
			} catch (Exception e) {
				System.out.println("Error in Neo4j Operations !!!"+e);

			} finally {
				try {
					if (csvFileParser != null)
						csvFileParser.close();
					System.out.println("Parser is now closed...");
				} catch (IOException e) {
					System.out.println("Error while closing File Reader !!!"+e);
					e.printStackTrace();
				}
			}

		} catch (Exception e) {
			System.out.println("Error in CsvFileReader !!!"+e);
			e.printStackTrace();
		} finally {

			try {
				if (fileReader != null)
					fileReader.close();
				System.out.println("File Reader is now closed...");
			} catch (IOException e) {
				System.out.println("Error while closing CSV Parser !!!");
				e.printStackTrace();
			}

		}

	}

	private void buildChicagoCrimeData(Map<String, String> map, GraphDatabaseService graphDb) {
		Node crimeNode = buildCrimeData(map, graphDb);
		Node crimeTypeNode = buildCrimeTypeData(map, graphDb);
		Node beatNode = buildBeatsData(map, graphDb);
		Node locNode = buildLocationData(map, graphDb);
		buildCrimeLocationRel(graphDb, crimeNode, locNode);
		buildCrimeBeatsRel(graphDb, crimeNode, beatNode);
		buildCrimeToTypeRel(graphDb, crimeNode, crimeTypeNode);
	}

	private void buildCrimeToTypeRel(GraphDatabaseService graphDb, Node crimeNode, Node crimeTypeNode) {

		boolean isRelExist = false;
		for (Relationship rel : crimeNode.getRelationships(RelTypes.TYPE)) {
			Node node = rel.getEndNode();
			if (node.equals(crimeTypeNode)) {
				isRelExist = true;
				break;
			}
		}
		if (!isRelExist) {
			crimeNode.createRelationshipTo(crimeTypeNode, RelTypes.TYPE);
		}

	}

	private void buildCrimeBeatsRel(GraphDatabaseService graphDb, Node crimeNode, Node beatNode) {

		boolean isRelExist = false;
		for (Relationship rel : crimeNode.getRelationships(RelTypes.ON_BEAT)) {
			Node node = rel.getEndNode();
			if (node.equals(beatNode)) {
				isRelExist = true;
				break;
			}
		}
		if (!isRelExist) {
			crimeNode.createRelationshipTo(beatNode, RelTypes.ON_BEAT);
		}

	}

	private void buildNeo4jSchemaIndex(GraphDatabaseService graphDb) {
		graphDb.schema().indexFor(Label.label(Labels.Crime.name())).on("id").create();
		graphDb.schema().indexFor(Label.label(Labels.Beat.name())).on("id").create();
		graphDb.schema().indexFor(Label.label(Labels.Location.name())).on("id").create();
		graphDb.schema().indexFor(Label.label(Labels.CrimeType.name())).on("name").create();
		graphDb.schema().indexFor(Label.label(Labels.CrimeType.name())).on("crimeType").create();
		graphDb.schema().indexFor(Label.label(Labels.Location.name())).on("name").create();
		graphDb.schema().indexFor(Label.label(Labels.SubCategory.name())).on("code").create();
	}

	private void buildCrimeLocationRel(GraphDatabaseService graphDb, Node crimeNode, Node locNode) {

		boolean isRelExist = false;
		for (Relationship rel : crimeNode.getRelationships(RelTypes.COMMITTED_IN)) {
			Node node = rel.getEndNode();
			if (node.equals(locNode)) {
				isRelExist = true;
				break;
			}
		}
		if (!isRelExist) {
			crimeNode.createRelationshipTo(locNode, RelTypes.COMMITTED_IN);
		}

	}

	private Node buildLocationData(Map<String, String> map, GraphDatabaseService graphDb) {
		Node locNode = findOrCreate(Label.label(Labels.Location.name()), "id",
				map.get(CrimeHeader.LOCATION_DESCRIPTION.name()), graphDb);
		locNode.setProperty("id", map.get(CrimeHeader.LOCATION_DESCRIPTION.name()));
		return locNode;

	}

	private Node buildBeatsData(Map<String, String> map, GraphDatabaseService graphDb) {
		Node beatNode = findOrCreate(Label.label(Labels.Beat.name()), "id", map.get(CrimeHeader.BEAT.name()), graphDb);
		beatNode.setProperty("id", map.get(CrimeHeader.BEAT.name()));
		return beatNode;

	}

	private Node buildCrimeTypeData(Map<String, String> map, GraphDatabaseService graphDb) {
		Node crimeTypeNode = findOrCreate(Label.label(Labels.CrimeType.name()), "crimeType",
				map.get(CrimeHeader.PRIMARY_TYPE.name()), graphDb);
		crimeTypeNode.setProperty("crimeType", map.get(CrimeHeader.PRIMARY_TYPE.name()));
		return crimeTypeNode;

	}

	private Node buildCrimeData(Map<String, String> map, GraphDatabaseService graphDb) {
		
		Node crimeNode = findOrCreate(Label.label(Labels.Crime.name()), "id", map.get(CrimeHeader.ID.name()), graphDb);
		crimeNode.setProperty("id", Long.parseLong(map.get(CrimeHeader.ID.name())));
		crimeNode.setProperty("caseNumber", map.get(CrimeHeader.CASE_NUMBER.name()));
		crimeNode.setProperty("caseDate", map.get(CrimeHeader.DATE.name()));		
		crimeNode.setProperty("block", map.get(CrimeHeader.BLOCK.name()));
		crimeNode.setProperty("iucr", map.get(CrimeHeader.IUCR.name()));
		crimeNode.setProperty("primaryType", map.get(CrimeHeader.PRIMARY_TYPE.name()));
		crimeNode.setProperty("description", map.get(CrimeHeader.DESCRIPTION.name()));
		crimeNode.setProperty("locDescription", map.get(CrimeHeader.LOCATION_DESCRIPTION.name()));
		crimeNode.setProperty("arrest", map.get(CrimeHeader.ARREST.name()));
		crimeNode.setProperty("domestic", map.get(CrimeHeader.DOMESTIC.name()));
		crimeNode.setProperty("beat", map.get(CrimeHeader.BEAT.name()));
		crimeNode.setProperty("district", map.get(CrimeHeader.DISTRICT.name()));
		
		crimeNode.setProperty("ward", map.get(CrimeHeader.WARD.name()));
		crimeNode.setProperty("communityArea", map.get(CrimeHeader.COMMUNITY_AREA.name()));
		crimeNode.setProperty("fbiCode", map.get(CrimeHeader.FBI_CODE.name()));
		crimeNode.setProperty("xCordinate", map.get(CrimeHeader.X_COORDINATE.name()));
		crimeNode.setProperty("yCordinate", map.get(CrimeHeader.Y_COORDINATE.name()));
		crimeNode.setProperty("updatedOn", map.get(CrimeHeader.UPDATED_ON.name()));
		crimeNode.setProperty("latitude", map.get(CrimeHeader.LATITUDE.name()));
		
		crimeNode.setProperty("longitude", map.get(CrimeHeader.LONGITUDE.name()));
		crimeNode.setProperty("location", map.get(CrimeHeader.LOCATION.name()));
		
		
		return crimeNode;
	}

	private Node findOrCreate(Label label, String prop, String value, GraphDatabaseService graphDb) {
		Node node = graphDb.findNode(label, prop, value);
		if (node == null) {
			node = graphDb.createNode(label);
		}
		return node;
	}

}