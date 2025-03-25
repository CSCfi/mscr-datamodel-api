package fi.vm.yti.datamodel.api.v2.endpoint;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.core.search.Hit;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.Sets;

import fi.vm.yti.datamodel.api.v2.messaging.IntegrationContainerRequest;
import fi.vm.yti.datamodel.api.v2.messaging.IntegrationResourceDTO;
import fi.vm.yti.datamodel.api.v2.messaging.UpdatedResourcesResponseDTO;
import fi.vm.yti.datamodel.api.v2.messaging.Meta;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.opensearch.index.UpdatedResourceDTO;
import fi.vm.yti.datamodel.api.v2.opensearch.queries.ModelQueryFactory;
import fi.vm.yti.datamodel.api.v2.service.IntegrationService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import io.swagger.v3.oas.annotations.tags.Tag;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

@RestController
@RequestMapping("v2/updates")
@Tag(name = "Updates" )
public class UpdatesController {

	private final IntegrationService integrationService;
	private final OpenSearchClient client;
	private final JenaService jenaService;
	
	public UpdatesController(IntegrationService service, OpenSearchClient client, JenaService jenaService) {
		this.integrationService = service;
		this.client = client;
		this.jenaService = jenaService;
	}
	
	static IntegrationResourceDTO mapToIntegrationResult(UpdatedResourceDTO hit) {
		return null;
	}
	
    @PostMapping(path = "/resources", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    UpdatedResourcesResponseDTO resources(@RequestBody IntegrationContainerRequest containersRequest) throws OpenSearchException, IOException { 	
        var q = ModelQueryFactory.createUpdatedResourcesQuery(containersRequest);        
        var response = client.search(q, UpdatedResourceDTO.class);
        var searchResponse = new UpdatedResourcesResponseDTO();
		searchResponse.setResults(response.hits().hits().stream().map(Hit::source).toList());				
		Meta meta = new Meta();
		meta.setTotalResults(response.hits().total().value());
        meta.setFrom(containersRequest.getPageFrom());
        meta.setPageSize(containersRequest.getPageSize());
        searchResponse.setMeta(meta);
        return searchResponse;    	    	
    }	
    
    @PostMapping(path = "/latestresources", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    UpdatedResourcesResponseDTO latestResources(@RequestBody IntegrationContainerRequest c) throws OpenSearchException, IOException {
    	if(c.getUri() == null || c.getUri().isEmpty()) {
    		throw new RuntimeException("This endpoint must be called with integrationcontainer request that has one or more uris");
    	}
    	
    	String queryString = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX mscr: <http://uri.suomi.fi/datamodel/ns/mscr#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX iow: <http://uri.suomi.fi/datamodel/ns/iow/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX ms: <http://purl.org/obo/owl/MS#>
PREFIX io: <https://iaco.me/>

select distinct ?uri ?state ?label ?type (group_concat(?reasonCode;separator=',') as ?codes)
where {

  {
	?uri a mscr:Schema .
    ?uri mscr:state ?state .
    ?uri rdfs:label ?label .
    ?uri iow:contentModified ?contentModified .  
    BIND('1' as ?reasonCode) .
    BIND('schema' as ?type) . 
    filter(?uri in (%3$s))        
	FILTER (
      ?contentModified > "%1$s"^^xsd:dateTime 
      &&
      ?contentModified < "%2$s"^^xsd:dateTime     
    )    
  }
  UNION
  {
	?uri a mscr:Schema .
    ?uri mscr:state ?state .
    ?uri rdfs:label ?label .
    ?uri iow:stateModified ?stateModified .
    BIND('2' as ?reasonCode) .
    BIND('schema' as ?type) . 
    filter(?uri in (%3$s))  
	FILTER (
      ?stateModified > "%1$s"^^xsd:dateTime 
      &&
      ?stateModified < "%2$s"^^xsd:dateTime     
    )    
  }  
  UNION 
  {
	?uri a mscr:Crosswalk .
    ?uri mscr:state ?state .
    ?uri rdfs:label ?label .
    ?uri iow:contentModified ?contentModified .
    BIND('1' as ?reasonCode) .
    BIND('crosswalk' as ?type) .     
    filter(?uri in (%3$s))          
	FILTER (
      ?contentModified > "%1$s"^^xsd:dateTime 
      &&
      ?contentModified < "%2$s"^^xsd:dateTime     
    )     
  }
  UNION 
  {
	?uri a mscr:Crosswalk .
    ?uri mscr:state ?state .
    ?uri rdfs:label ?label .
    ?uri iow:stateModified ?stateModified .
    BIND('2' as ?reasonCode) .
    BIND('crosswalk' as ?type) .     
    filter(?uri in (%3$s))          
	FILTER (
      ?stateModified > "%1$s"^^xsd:dateTime 
      &&
      ?stateModified < "%2$s"^^xsd:dateTime     
    )     
  }  
    UNION 
  {
	?uri a mscr:Crosswalk .
    ?uri mscr:state ?state .
    ?uri rdfs:label ?label .
    ?uri mscr:sourceSchema ?sourceSchema .
    ?sourceSchema iow:contentModified ?sourceSchemaModified .
    BIND('3' as ?reasonCode) .
    BIND('crosswalk' as ?type) .     
    filter(?uri in (%3$s))        
	FILTER (
      (
        ?sourceSchemaModified > "%1$s"^^xsd:dateTime 
        &&
        ?sourceSchemaModified < "%2$s"^^xsd:dateTime
      )         
    )     
  }  
    UNION 
  {
	?uri a mscr:Crosswalk .
    ?uri mscr:state ?state .
    ?uri rdfs:label ?label .
    ?uri mscr:targetSchema ?targetSchema .
    ?targetSchema iow:contentModified ?targetSchemaModified .
    BIND('4' as ?reasonCode) .
    BIND('crosswalk' as ?type) .
    filter(?uri in (%3$s))        
	FILTER (
      (
        ?targetSchemaModified > "%1$s"^^xsd:dateTime 
        &&
        ?targetSchemaModified < "%2$s"^^xsd:dateTime
      )         
    )     
  }   
    UNION 
  {
	?uri a mscr:Crosswalk .
    ?uri mscr:state ?state .
    ?uri rdfs:label ?label .
    ?uri mscr:sourceSchema ?sourceSchema .
    ?sourceSchema mscr:aggregationKey ?sourceSchemaAggKey .
    ?sourceSchemaAggKey iow:stateModified ?sourceSchemaModified .
    BIND('5' as ?reasonCode) .
    BIND('crosswalk' as ?type) .     
    filter(?uri in (%3$s))      
	FILTER (
      (
      	?sourceSchemaModified > "%1$s"^^xsd:dateTime 
        &&
        ?sourceSchemaModified < "%2$s"^^xsd:dateTime
      )    
    )     
  }  
    UNION 
  {
	?uri a mscr:Crosswalk .
    ?uri mscr:state ?state .
    ?uri rdfs:label ?label .
    ?uri mscr:targetSchema ?targetSchema .
    ?targetSchema mscr:aggregationKey ?targetSchemaAggKey .
    ?targetSchemaAggKey iow:stateModified ?targetSchemaModified .
    BIND('6' as ?reasonCode) .
    BIND('crosswalk' as ?type) .     
    filter(?uri in (%3$s))   
	FILTER (
      (
      	?targetSchemaModified > "%1$s"^^xsd:dateTime 
        &&
        ?targetSchemaModified < "%2$s"^^xsd:dateTime
      )    
    )     
  }    

} group by ?uri ?state ?label ?type   			    			    			
    			""".formatted(c.getAfter(), c.getBefore(), String.join(",", c.getUri().stream().map(s -> '<' + s + '>').toList()));
        
    	ResultSet rs = jenaService.doCoreSelectQuery(queryString);
    	List<UpdatedResourceDTO> resources = new ArrayList<UpdatedResourceDTO>();
    	while(rs.hasNext()) {
    		QuerySolution q = rs.next();
    		UpdatedResourceDTO r = new UpdatedResourceDTO();
    		r.setId(q.get("uri").asResource().getURI());
    		r.setState(q.get("state").asLiteral().getString());
    		r.setLabel(Map.of("en", q.get("label").asLiteral().getString()));
    		r.setType(q.get("type").asLiteral().getString());
    		r.setReasonCodes(q.get("codes").asLiteral().getString().split(","));
    		resources.add(r);
    	}
    	
    	var searchResponse = new UpdatedResourcesResponseDTO();
    	searchResponse.setResults(resources);
		Meta meta = new Meta();
		meta.setTotalResults(new Long(resources.size()));
        meta.setFrom(0);
        meta.setPageSize(10000);
        searchResponse.setMeta(meta);    	
        return searchResponse;    	    	
    }    
}
