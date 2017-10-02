/*
 * Licensed under the European Union Public Licence (EUPL) V.1.1 
 */
package fi.vm.yti.datamodel.api.endpoint.model;


import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import fi.vm.yti.datamodel.api.config.EndpointServices;
import fi.vm.yti.datamodel.api.utils.JerseyJsonLDClient;
import fi.vm.yti.datamodel.api.utils.LDHelper;
import fi.vm.yti.datamodel.api.utils.NamespaceManager;
import org.apache.jena.query.ParameterizedSparqlString;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Map;

 
/**
 * Root resource (exposed at "myresource" path)
 */
@Path("history")
@Api(tags = {"History"}, description = "Get list of revisions of the resource from change history")
public class History {

    private static final Logger logger = Logger.getLogger(History.class.getName());

    @Context ServletContext context;
    EndpointServices services = new EndpointServices();
 
    
  @GET
  @Produces("application/ld+json")
  @ApiOperation(value = "Get activity history for the resource", notes = "More notes about this method")
  @ApiResponses(value = {
      @ApiResponse(code = 400, message = "Invalid model supplied"),
      @ApiResponse(code = 404, message = "Service not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Response json(
      @ApiParam(value = "resource id")
      @QueryParam("id") String id,
      @ApiParam(value = "Peek", defaultValue="false")
      @QueryParam("peek") boolean peek) {
      
      if(id==null || id.equals("undefined") || id.equals("default") || (peek && id!=null) ) {
 
        ParameterizedSparqlString pss = new ParameterizedSparqlString();
        
        Map<String, String> namespacemap = NamespaceManager.getCoreNamespaceMap();
        namespacemap.putAll(LDHelper.PREFIX_MAP);
        
        pss.setNsPrefixes(namespacemap);
        
        String queryString = "CONSTRUCT { "
                + "?activity a prov:Activity . "
                + "?activity prov:wasAttributedTo ?user . "
                + "?activity dcterms:modified ?modified . "
                + "?activity dcterms:identifier ?entity . " 
                + " } "
                + "WHERE {"
                + "?activity a prov:Activity . "
                + "?activity prov:used ?entity . "
                + "?entity a prov:Entity . "
                + "?entity prov:wasAttributedTo ?user . "
                + "?entity prov:generatedAtTime ?modified . "
                + "} ORDER BY DESC(?modified)"; 

        pss.setCommandText(queryString);
        
        if(peek) {
            pss.setIri("activity", id);
        }

        return JerseyJsonLDClient.constructGraphFromService(pss.toString(), services.getProvReadSparqlAddress());
      
      } else {
        return JerseyJsonLDClient.getGraphResponseFromService(id, services.getProvReadWriteAddress());
      }
      
    }
  
  }