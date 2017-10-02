/*
 * Licensed under the European Union Public Licence (EUPL) V.1.1 
 */
package fi.vm.yti.datamodel.api.endpoint.concepts;

import java.util.logging.Logger;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import fi.vm.yti.datamodel.api.config.LoginSession;
import fi.vm.yti.datamodel.api.utils.ConceptMapper;
import fi.vm.yti.datamodel.api.utils.JerseyJsonLDClient;
import fi.vm.yti.datamodel.api.utils.JerseyResponseManager;
import fi.vm.yti.datamodel.api.utils.LDHelper;
import fi.vm.yti.datamodel.api.config.EndpointServices;
import fi.vm.yti.datamodel.api.utils.IDManager;
import org.apache.jena.query.ParameterizedSparqlString;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
 
/**
 * Root resource (exposed at "class" path)
 */
@Path("modelConcepts")
@Api(tags = {"Concept"}, description = "Local concept operations")
public class ModelConcepts {

    @Context ServletContext context;
    EndpointServices services = new EndpointServices();
    private static final Logger logger = Logger.getLogger(ModelConcepts.class.getName());
    
  @GET
  @Produces("application/ld+json")
  @ApiOperation(value = "Get used concepts from model", notes = "Get used concepts in JSON-LD")
  @ApiResponses(value = {
      @ApiResponse(code = 404, message = "No such resource"), 
     @ApiResponse(code = 400, message = "Invalid model supplied"),
      @ApiResponse(code = 404, message = "Service not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Response json(
      @ApiParam(value = "Concept id")
      @QueryParam("id") String id,
      @ApiParam(value = "Model id", required = true)
      @QueryParam("model") String model) {
        
        if(id!=null && !id.equals("undefined") && IDManager.isInvalid(id)) {
            return JerseyResponseManager.invalidIRI();
        }
      
        if(IDManager.isInvalid(model)) {
            return JerseyResponseManager.invalidIRI();
        }
       
        ParameterizedSparqlString pss = new ParameterizedSparqlString();
        pss.setNsPrefixes(LDHelper.PREFIX_MAP);

        String queryString = "CONSTRUCT { "
                + "?skosCollection skos:member ?concept . "
                + "?concept skos:inScheme ?scheme . "
                + "?concept a skos:Concept . "
                + "?concept termed:id ?conceptId . "
                + "?concept termed:graph ?graph . "
                + "?scheme dcterms:title ?schemeTitle . "
                + "?scheme a skos:ConceptScheme . "
                + "?scheme termed:id ?schemeId . "
                + "?scheme termed:graph ?graph . "
                + "?graph termed:id ?graphId . "
                + "?concept skos:prefLabel ?label . "
                + "?concept skos:definition ?definition . "
                + "?concept prov:generatedAtTime ?time . "
                + "?concept prov:wasAssociatedWith ?user . "
                + "} WHERE {"
                + "GRAPH ?modelParts { "
                + "?model dcterms:hasPart ?resource . "
                + "}"
                + "GRAPH ?resource { "
                + "?resource dcterms:subject ?concept ."
                + "?concept skos:prefLabel ?label . "
                + "?concept skos:definition ?definition . "
                + "?concept skos:inScheme ?scheme . "
                + "OPTIONAL { ?concept prov:generatedAtTime ?time . }"
                + "OPTIONAL { ?concept prov:wasAssociatedWith ?user . }"
                + "OPTIONAL { ?scheme dcterms:title ?schemeTitle . }"
                + "OPTIONAL { ?concept termed:id ?conceptId . }"
                + "OPTIONAL { ?scheme termed:id ?schemeId . ?scheme termed:graph ?graph . ?graph termed:id ?graphId . }"
                + "}"
                + "}";
        
        pss.setIri("model",model);
        pss.setIri("modelParts",model+"#HasPartGraph");
        
         if(id!=null && !id.equals("undefined")) {
             pss.setIri("concept",id);
         }

        
        pss.setCommandText(queryString);

        return JerseyJsonLDClient.constructGraphFromService(pss.toString(), services.getCoreSparqlAddress());
     
  }
 
  @PUT
  @ApiOperation(value = "PUT existing concept reference to model", notes = "Adds concept reference to model concetps")
  @ApiResponses(value = {
      @ApiResponse(code = 404, message = "No such resource"),
      @ApiResponse(code = 400, message = "Invalid model supplied"),
      @ApiResponse(code = 404, message = "Service not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Response putConceptToModel(
      @ApiParam(value = "Concept id", required = true)
      @QueryParam("id") String id,
      @ApiParam(value = "Model id", required = true)
      @QueryParam("model") String model) {
  
      if(IDManager.isInvalid(id) || IDManager.isInvalid(model)) {
          return JerseyResponseManager.invalidIRI();
      }
      
      ConceptMapper.addConceptToLocalSKOSCollection(model,id);
  
      return JerseyResponseManager.okEmptyContent();
  }
  
  @DELETE
  @ApiOperation(value = "Delete concept reference from model", notes = "Delete concept reference from model")
  @ApiResponses(value = {
      @ApiResponse(code = 404, message = "No such resource"),
      @ApiResponse(code = 400, message = "Cannot be removed"),
      @ApiResponse(code = 404, message = "Service not found"),
      @ApiResponse(code = 500, message = "Internal server error")
  })
  public Response deleteConceptFromModel(
      @ApiParam(value = "Concept id", required = true)
      @QueryParam("id") String id,
      @ApiParam(value = "Model id", required = true)
      @QueryParam("model") String model,
      @Context HttpServletRequest request) {
  
        /* Check that URIs are valid */
        if(IDManager.isInvalid(model) || IDManager.isInvalid(id)) {
            return JerseyResponseManager.invalidIRI();
        }
        
       HttpSession session = request.getSession();

       if(session==null) return JerseyResponseManager.unauthorized();

       LoginSession login = new LoginSession(session);

       if(!login.isLoggedIn() || !login.hasRightToEditModel(model))
          return JerseyResponseManager.unauthorized();
      
      if(ConceptMapper.deleteModelReference(model,id))
         return JerseyResponseManager.okEmptyContent();
      else
         return JerseyResponseManager.depedencies();
  }
  
}
