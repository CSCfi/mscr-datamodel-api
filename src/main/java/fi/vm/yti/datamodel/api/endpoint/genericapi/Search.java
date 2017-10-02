/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fi.vm.yti.datamodel.api.endpoint.genericapi;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import fi.vm.yti.datamodel.api.utils.JerseyJsonLDClient;
import fi.vm.yti.datamodel.api.config.EndpointServices;
import fi.vm.yti.datamodel.api.utils.LDHelper;
import org.apache.jena.query.ParameterizedSparqlString;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 * REST Web Service
 *
 * @author malonen
 */
@Path("search")
@Api(tags = {"Resource"}, description = "Search resources")
public class Search {

    @Context ServletContext context;
    EndpointServices services = new EndpointServices();
            
  @GET
  @Consumes("application/sparql-query")
  @Produces("application/ld+json")
  @ApiOperation(value = "Sparql query to given service", notes = "More notes about this method")
  @ApiResponses(value = {
      @ApiResponse(code = 400, message = "Query parse error"),
      @ApiResponse(code = 500, message = "Query exception"),
      @ApiResponse(code = 200, message = "OK")
  })
  public Response search(
          @ApiParam(value = "Search in graph") @QueryParam("graph") String graph,
          @ApiParam(value = "Searchstring", required = true) @QueryParam("search") String search,
          @ApiParam(value = "Language") @QueryParam("lang") String lang) {      

      /* TODO: ADD TEXTDATASET ONCE NAMESPACE BUG IS RESOLVED */
      // if(!search.endsWith("~")||!search.endsWith("*")) search = search+"*";
      
            String queryString = 
                    "CONSTRUCT {"
                  + "?resource rdf:type ?type ."
                  + "?resource rdfs:label ?label ."
                  + "?resource rdfs:comment ?comment ."
                  + "?resource rdfs:isDefinedBy ?super . "
                  + "?resource dcap:preferredXMLNamespaceName ?resnamespace . "
                  + "?resource dcap:preferredXMLNamespacePrefix ?resprefix . "
                  + "?super rdfs:label ?superLabel . "
                  + "?super dcap:preferredXMLNamespaceName ?namespace . "
                  + "?super dcap:preferredXMLNamespacePrefix ?prefix . "
                  + "} WHERE { "
                  + (graph==null||graph.equals("undefined")||graph.equals("default")?"":"GRAPH <"+graph+"#HasPartGraph> { <"+graph+"> dcterms:hasPart ?graph . } ")
                  + "GRAPH ?graph {"
                  + "?resource ?p ?literal . "
                  + "FILTER contains(lcase(?literal),lcase(?search)) " 
                  + "?resource rdf:type ?type . "
                  + "OPTIONAL {"
                  + "?resource dcap:preferredXMLNamespaceName ?resnamespace . "
                  + "?resource dcap:preferredXMLNamespacePrefix ?resprefix . "
                  + "}"  
                  + "OPTIONAL {"
                  + "?resource rdfs:isDefinedBy ?super . "
                  + "GRAPH ?super { ?super rdfs:label ?superLabel . "
                  + "?super dcap:preferredXMLNamespaceName ?namespace . "
                  + "?super dcap:preferredXMLNamespacePrefix ?prefix . "
                  + "}}"
                  //+ "UNION"
                 // + "{?resource sh:predicate ?predicate . ?super sh:property ?resource . ?super rdfs:label ?superLabel . BIND(sh:Constraint as ?type)}"
                  + "?resource rdfs:label ?label . " 
                  //+ "?resource text:query '"+search+"' . "
                  + "OPTIONAL{?resource rdfs:comment ?comment .}"
                  + (lang==null||lang.equals("undefined")?"":"FILTER langMatches(lang(?label),'"+lang+"')")
                  + "}}"; 

            ParameterizedSparqlString pss = new ParameterizedSparqlString();
            pss.setNsPrefixes(LDHelper.PREFIX_MAP);
            pss.setLiteral("search", search);
            pss.setCommandText(queryString);
            
            return JerseyJsonLDClient.constructGraphFromService(pss.toString(), services.getCoreReadAddress());

  }
  
}
