package org.sharkyaya.app;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.sharkyaya.loader.CrimeFeedNeo4jLoader;

@Path("/v1/loader")
public class Neo4jLoaderApp {

	private GraphDatabaseService graphDb;
    private final ObjectMapper objectMapper;
    public Neo4jLoaderApp( @Context GraphDatabaseService graphDb )
    {
        this.graphDb = graphDb;
        this.objectMapper = new ObjectMapper();
    }
	
	@POST
    @Produces( MediaType.APPLICATION_JSON )
    @Path( "/crime" )
    public Response loadCrimeData(@FormParam(value="fileName") String fileName )
    {
		Response response = null;
		if(StringUtils.isBlank(fileName)){
			response = Response.status( Status.OK ).entity("fileName is blank").build();
			return response;
		}
		
		CrimeFeedNeo4jLoader loader = new CrimeFeedNeo4jLoader();
		Map<String,Integer> resultMap = new HashMap<String, Integer>();
		try{
			 resultMap = loader.parseAndInsert(fileName, this.graphDb);
			 response = Response.status( Status.OK ).entity(objectMapper.writeValueAsString(resultMap)).build();
		}catch(Exception ex){
			System.out.println("Error in loader !!!"+ex);
			response = Response.status( Status.INTERNAL_SERVER_ERROR).entity("Exception occured").build();
		}
		
        // Do stuff with the database
        return response;
    }

}
