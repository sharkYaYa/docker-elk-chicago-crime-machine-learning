package org.sharkyaya.cce.loader.parser;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.sharkyaya.cce.loader.model.Address;
import org.sharkyaya.cce.loader.model.CrimeHeader;
import org.sharkyaya.cce.loader.model.CrimeSummary;
import org.sharkyaya.cce.loader.model.Location;

/*
		NOT A PRODUCTION READY CODE
 */
public abstract class CrimeFeedParser {

	public static void parseAndInsert(String fileWithPath) throws UnknownHostException {
		FileReader fileReader = null;
		CSVParser csvFileParser = null;

		// Create the CSVFormat object with the header mapping
		CSVFormat csvFileFormat = CSVFormat.DEFAULT.withHeader(CrimeHeader.class);

		ObjectMapper mapper = new ObjectMapper();
		Settings settings = Settings.builder()
						.put("client.transport.nodes_sampler_interval", "5s")
						.put("client.transport.sniff", false)
						//.put("transport.tcp.compress", true)
						.put("cluster.name", "ChicagoInfo")
						//.put("xpack.security.transport.ssl.enabled", true)
						//.put("request.headers.X-Found-Cluster", "${cluster.name}")
						.put("xpack.security.user", "elastic:changeme")
				.build();

		
		@SuppressWarnings({ "resource", "unchecked" })
		TransportClient client = new PreBuiltXPackTransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300))
				.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9200));
		
		try {
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			BulkResponse bulkResponse = null;
			// initialize FileReader object
			fileReader = new FileReader(fileWithPath);
			// initialize CSVParser object
			csvFileParser = new CSVParser(fileReader, csvFileFormat);
			// Get a list of CSV file records
			// Read the CSV file records starting from the second record to skip
			// the header
			long i=1;
			for (CSVRecord csvRecord : csvFileParser.getRecords()) {
				/* To avoid loading the header as first record */
				if(i++ ==1) continue;
				/*
				 Enable only when you need to see the data for the testing
				 System.out.println(csvRecord);
				*/
				CrimeSummary cSummary = new CrimeSummary();
				Address address = new Address();
				

				/* Prepare Location Object */
				/* 
					Assumtions
					1. When the location LATITUDE or LONGITUDE or both is not present set it to ZERO '0.0F'
					2. When the location Y_COORDINATE or X_COORDINATE or both is not present set it to ZERO '0'
				
					Stupid works --- duh  yes ... :)
				*/
				
				Location location = new Location();
				location.setLocation(csvRecord.get(CrimeHeader.LOCATION));
				try {
					float latitude = Float.valueOf(csvRecord.get(CrimeHeader.LATITUDE));
					location.setLatitude(latitude);
				} catch (Exception E) {
					System.out.println("Error Converting LATITUDE: [" + csvRecord.get(CrimeHeader.LATITUDE)+"]");
					location.setLatitude(0.00F);
				}
				try {
					float longitude = Float.valueOf(csvRecord.get(CrimeHeader.LONGITUDE));
					location.setLongitude(longitude);
				} catch (Exception E) {
					System.out.println("Error Converting LONGITUDE: [" + csvRecord.get(CrimeHeader.LONGITUDE)+"]");
					location.setLongitude(0.00F);
				}
				try {
					long xCoordinate = Long.valueOf(csvRecord.get(CrimeHeader.ç));
					location.setxCoordinate(xCoordinate);
				} catch (Exception E) {
					System.out.println("Error Converting X_COORDINATE: [" + csvRecord.get(CrimeHeader.X_COORDINATE)+"]");
					location.setxCoordinate(0);
				}

				try {
					long yCoordinate = Long.valueOf(csvRecord.get(CrimeHeader.Y_COORDINATE));
					location.setyCoordinate(yCoordinate);
				} catch (Exception E) {
					System.out.println("Error Converting Y_COORDINATE: [" + csvRecord.get(CrimeHeader.Y_COORDINATE)+"]");
					location.setyCoordinate(0);
				}

				// prepare Address
				address.setBlock(csvRecord.get(CrimeHeader.BLOCK));
				address.setCommunityArea(csvRecord.get(CrimeHeader.COMMUNITY_AREA));
				address.setDistrict(csvRecord.get(CrimeHeader.DISTRICT));
				address.setWard(csvRecord.get(CrimeHeader.WARD));
				address.setLocation(location);

				// prepare ClientSummary
				cSummary.setArrest(csvRecord.get(CrimeHeader.ARREST));
				cSummary.setBeat(csvRecord.get(CrimeHeader.BEAT));
				cSummary.setCaseNo(csvRecord.get(CrimeHeader.CASE_NUMBER));
				
				
				try {
					long id = Long.valueOf(csvRecord.get(CrimeHeader.ID));
					cSummary.setId(id);
				} catch (Exception E) {
					System.out.println("Error Converting ID: " + csvRecord.get(CrimeHeader.ID));
				}
				
				try 
				{
					Date caseDate = new SimpleDateFormat(CrimeSummary.DATE_FORMAT).parse(csvRecord.get(CrimeHeader.DATE));
					cSummary.setDate(caseDate.getTime());
				
				} catch (Exception E) {
					System.out.println("Error Converting Date: " + csvRecord.get(CrimeHeader.DATE));
					cSummary.setDate(new Date().getTime());
				}
				cSummary.setDateAsString(csvRecord.get(CrimeHeader.DATE));
				try 
				{
					Date updatedOn = new SimpleDateFormat(CrimeSummary.DATE_FORMAT).parse(csvRecord.get(CrimeHeader.UPDATED_ON));
					cSummary.setUpdatedOn(updatedOn.getTime());
				
				} catch (Exception E) {
					System.out.println("Error Converting Date: " + csvRecord.get(CrimeHeader.UPDATED_ON));
					cSummary.setUpdatedOn(new Date().getTime());
				}
				cSummary.setUpdatedOnAsString(csvRecord.get(CrimeHeader.UPDATED_ON));
				
				try {
					int year = Integer.valueOf(csvRecord.get(CrimeHeader.YEAR));
					cSummary.setYear(year);
				} catch (Exception E) {
					System.out.println("Error Converting YEAR: " + csvRecord.get(CrimeHeader.YEAR));
				}
				
				
				cSummary.setDescription(csvRecord.get(CrimeHeader.DESCRIPTION));
				cSummary.setDomestic(csvRecord.get(CrimeHeader.DOMESTIC));
				cSummary.setFbiCode(csvRecord.get(CrimeHeader.FBI_CODE));
				cSummary.setIcur(csvRecord.get(CrimeHeader.IUCR));
				
				
				
				
				cSummary.setLocationDescription(csvRecord.get(CrimeHeader.LOCATION_DESCRIPTION));
				cSummary.setPrimaryType(csvRecord.get(CrimeHeader.PRIMARY_TYPE));
				
				
				
				
				
				cSummary.setAddress(address);

				// System.out.println(mapper.writeValueAsString(cSummary));

				bulkRequest.add(client.prepareIndex("chicago", "crime").setSource(mapper.writeValueAsString(cSummary)));
				if (bulkRequest.numberOfActions() > 2999) {
					try {
						bulkResponse = bulkRequest.execute().actionGet();
						System.out.println("Data Loaded: " + bulkRequest.numberOfActions() + " hasFailures: "
								+ bulkResponse.hasFailures());
						bulkRequest = client.prepareBulk();
					} catch (Exception e) {
						System.out.println("1.] Error while closing the Elastic client !!!");
						e.printStackTrace();
					}
				}

			}
			if (bulkRequest.numberOfActions() > 0) {
				try {
					bulkResponse = bulkRequest.execute().actionGet();
					System.out.println("Data Loaded: " + bulkRequest.numberOfActions() + " hasFailures: "
							+ bulkResponse.hasFailures());
					System.out.println();
					if (client != null)
						client.close();
					System.out.println("Client is now closed...");
				} catch (Exception e) {
					System.out.println("2.] Error while closing the Elastic client !!!");
					e.printStackTrace();
				}
			}

		} catch (Exception e) {
			System.out.println("Error in CsvFileReader !!!");
			e.printStackTrace();
		} finally {
			try {
				if (csvFileParser != null)
					csvFileParser.close();
				System.out.println("Parser is now closed...");
			} catch (IOException e) {
				System.out.println("Error while closing File Reader !!!");
				e.printStackTrace();
			}
			try {
				if (fileReader != null)
					fileReader.close();
				System.out.println("File Reader is now closed...");
			} catch (IOException e) {
				System.out.println("Error while closing CSV Parser !!!");
				e.printStackTrace();
			}

			try {
				if (client != null)
					client.close();
				System.out.println("Elaxtic Client is now closed...");
			} catch (Exception e) {
				System.out.println("Error while closing the Elastic client !!!");
				e.printStackTrace();
			}
		}

	}

}