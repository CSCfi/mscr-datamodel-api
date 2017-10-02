/*
 * Licensed under the European Union Public Licence (EUPL) V.1.1 
 */
package fi.vm.yti.datamodel.api.endpoint.model;

import java.util.logging.Logger;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import fi.vm.yti.datamodel.api.config.EndpointServices;
import fi.vm.yti.datamodel.api.config.LoginSession;
import fi.vm.yti.datamodel.api.utils.GraphManager;
import fi.vm.yti.datamodel.api.utils.IDManager;
import fi.vm.yti.datamodel.api.utils.JerseyJsonLDClient;
import fi.vm.yti.datamodel.api.utils.JerseyResponseManager;
import fi.vm.yti.datamodel.api.utils.LDHelper;
import fi.vm.yti.datamodel.api.utils.NamespaceManager;
import fi.vm.yti.datamodel.api.utils.ProvenanceManager;
import org.apache.jena.query.ParameterizedSparqlString;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.util.Map;
import java.util.UUID;
 
/**
 * Root resource (exposed at "myresource" path)
 */
@Path("version")
@Api(tags = {"Deprecated"}, description = "Get list of model versions from version history")
public class Version {

    private static final Logger logger = Logger.getLogger(Version.class.getName());

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
  
/* TODO: Should be removed and used from model api? */
    @PUT
    @ApiOperation(value = "Create new model version", notes = "Create new model version")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "New property is created"),
                    @ApiResponse(code = 400, message = "Invalid ID supplied"),
                    @ApiResponse(code = 403, message = "Invalid IRI in parameter"),
                    @ApiResponse(code = 401, message = "User is not logged in"),
                    @ApiResponse(code = 404, message = "Service not found") })
    public Response newModelVersion(
            @ApiParam(value = "model ID", required = true) @QueryParam("modelID") String modelID,
            @Context HttpServletRequest request) {

        HttpSession session = request.getSession();
        
        if(session==null) {
            return JerseyResponseManager.unauthorized();
        }
       
        LoginSession login = new LoginSession(session);
        
        if(!login.isLoggedIn() || !login.hasRightToEditModel(modelID)) {
            return JerseyResponseManager.unauthorized();
        }

        if(IDManager.isInvalid(modelID)) {
            return JerseyResponseManager.invalidIRI();
        }
    
        UUID versionUUID = UUID.randomUUID();
        
        try {
            GraphManager.addGraphFromServiceToService(modelID, "urn:uuid:"+versionUUID, services.getCoreReadAddress(), services.getProvReadWriteAddress());  
            GraphManager.addGraphFromServiceToService(modelID+"#HasPartGraph", modelID+"#HasPartGraph", services.getCoreReadAddress(), services.getProvReadWriteAddress());  
        } catch(NullPointerException ex) {
            logger.warning("Could not create PROV graphs!");
        }
        
        ProvenanceManager.createNewVersionModel(modelID, login.getEmail(), versionUUID);
        
        return JerseyResponseManager.okUUID(versionUUID);
         
    }

  
  }