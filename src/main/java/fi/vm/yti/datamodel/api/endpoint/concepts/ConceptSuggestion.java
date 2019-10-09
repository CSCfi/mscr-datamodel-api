package fi.vm.yti.datamodel.api.endpoint.concepts;

import com.jayway.jsonpath.JsonPath;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.service.*;
import fi.vm.yti.security.AuthenticatedUserProvider;
import io.swagger.annotations.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import java.util.UUID;

@Component
@Path("v1/conceptSuggestion")
@Api(tags = { "Concept" }, description = "Create concept suggestions")
public class ConceptSuggestion {

    private final AuthorizationManager authorizationManager;
    private final AuthenticatedUserProvider userProvider;
    private final JerseyResponseManager jerseyResponseManager;
    private final ModelManager modelManager;
    private final IDManager idManager;
    private final JerseyClient jerseyClient;
    private final TerminologyManager terminologyManager;

    private static final Logger logger = LoggerFactory.getLogger(ConceptSuggestion.class.getName());

    @Autowired
    ConceptSuggestion(AuthorizationManager authorizationManager,
                      AuthenticatedUserProvider userProvider,
                      JerseyResponseManager jerseyResponseManager,
                      TerminologyManager terminologyManager,
                      ModelManager modelManager,
                      IDManager idManager,
                      JerseyClient jerseyClient) {

        this.authorizationManager = authorizationManager;
        this.userProvider = userProvider;
        this.jerseyResponseManager = jerseyResponseManager;
        this.terminologyManager = terminologyManager;
        this.modelManager = modelManager;
        this.idManager = idManager;
        this.jerseyClient = jerseyClient;
    }

    @PUT
    @ApiOperation(value = "Create concept suggestion", notes = "Create new concept suggestion")
    @ApiResponses(value = { @ApiResponse(code = 200, message = "New concept is created"),
        @ApiResponse(code = 400, message = "Invalid ID supplied"),
        @ApiResponse(code = 403, message = "Invalid IRI in parameter"),
        @ApiResponse(code = 401, message = "User is not logged in"),
        @ApiResponse(code = 404, message = "Service not found") })
    public Response newConceptSuggestion(
        @ApiParam(value = "Terminology uri", required = true) @QueryParam("terminologyUri") String terminologyUri,
        @ApiParam(value = "Label", required = true) @QueryParam("label") String label,
        @ApiParam(value = "Comment", required = true) @QueryParam("comment") String comment,
        @ApiParam(value = "Initial language", required = true, allowableValues = "fi,en") @QueryParam("lang") String lang) {

        if (!authorizationManager.hasRightToSuggestConcept()) {
            return jerseyResponseManager.unauthorized();
        }

        logger.info("Creating concept suggestion: " + label);

        String jsonString = terminologyManager.createConceptSuggestionJson(lang, label, comment, userProvider.getUser().getId().toString());
        Response conceptResp = jerseyClient.saveConceptSuggestionUsingTerminologyAPI(jsonString, terminologyUri);

        if (conceptResp.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
            logger.info("Concept suggestion could not be saved! Invalid parameter or missing terminology?");
            return jerseyResponseManager.error();
        }

        String respString = conceptResp.readEntity(String.class);

        logger.info("Concept suggestion saved: ");
        logger.info(respString);

        String newConceptUUID = JsonPath.parse(respString).read("$.identifier");
        return jerseyResponseManager.successUrnUuid(UUID.fromString(newConceptUUID));

    }

}
