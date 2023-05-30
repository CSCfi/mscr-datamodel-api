package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.ResourceDTO;
import fi.vm.yti.datamodel.api.v2.dto.ResourceInfoDTO;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.service.GroupManagementService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.TerminologyService;
import fi.vm.yti.datamodel.api.v2.validator.ValidResource;
import fi.vm.yti.security.AuthenticatedUserProvider;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import org.springframework.beans.factory.annotation.Value;

@RestController
@RequestMapping("v2/resource")
@Tag(name = "Resource" )
@Validated
public class ResourceController {

    private final JenaService jenaService;
    private final AuthorizationManager authorizationManager;
    private final OpenSearchIndexer openSearchIndexer;
    private final AuthenticatedUserProvider userProvider;
    private final GroupManagementService groupManagementService;
    private final TerminologyService terminologyService;
    private final String defaultNamespace;

    public ResourceController(JenaService jenaService,
                              AuthorizationManager authorizationManager,
                              OpenSearchIndexer openSearchIndexer,
                              AuthenticatedUserProvider userProvider,
                              GroupManagementService groupManagementService,
                              TerminologyService terminologyService,
                              @Value("${defaultNamespace}") String defaultNamespace) {
        this.jenaService = jenaService;
        this.authorizationManager = authorizationManager;
        this.openSearchIndexer = openSearchIndexer;
        this.userProvider = userProvider;
        this.groupManagementService = groupManagementService;
        this.terminologyService = terminologyService;
        this.defaultNamespace = defaultNamespace;
    }

    @Operation(summary = "Add a class to a model")
    @ApiResponse(responseCode = "200", description = "Class added to model successfully")
    @PutMapping(value = "/{prefix}", consumes = APPLICATION_JSON_VALUE)
    public void createResource(@PathVariable String prefix, @RequestBody @ValidResource ResourceDTO dto){
        var graphUri = defaultNamespace + prefix;
        if(jenaService.doesResourceExistInGraph(graphUri, graphUri + prefix + "#" + dto.getIdentifier())){
            throw new MappingError("Already exists");
        }

        var model = jenaService.getDataModel(graphUri);
        if(model == null){
            throw new ResourceNotFoundException(prefix);
        }
        check(authorizationManager.hasRightToModel(prefix, model));

        terminologyService.resolveConcept(dto.getSubject());
        var resourceUri = ResourceMapper.mapToResource(graphUri, model, dto, userProvider.getUser());
        jenaService.putDataModelToCore(graphUri, model);
        var indexClass = ResourceMapper.mapToIndexResource(model, resourceUri);
        openSearchIndexer.createResourceToIndex(indexClass);
    }

    @Operation(summary = "Update a resource in a model")
    @ApiResponse(responseCode = "200", description = "Resource updated to model successfully")
    @PutMapping(value = "/{prefix}/{resourceIdentifier}", consumes = APPLICATION_JSON_VALUE)
    public void updateResource(@PathVariable String prefix, @PathVariable String resourceIdentifier, @RequestBody @ValidResource(updateProperty = true) ResourceDTO dto){
        var graphUri = defaultNamespace + prefix;

        if(!jenaService.doesResourceExistInGraph(graphUri, graphUri + "#" + resourceIdentifier)){
            throw new ResourceNotFoundException("Resource does not exist");
        }

        var model = jenaService.getDataModel(graphUri);
        if(model == null){
            //This should probably never happen,
            //but we will catch it just in case
            //something happens with the database
            throw new ResourceNotFoundException(prefix);
        }
        check(authorizationManager.hasRightToModel(prefix, model));

        terminologyService.resolveConcept(dto.getSubject());
        ResourceMapper.mapToUpdateResource(graphUri, model, resourceIdentifier, dto, userProvider.getUser());
        jenaService.putDataModelToCore(graphUri, model);
        var indexResource = ResourceMapper.mapToIndexResource(model, graphUri + "#" + resourceIdentifier);
        openSearchIndexer.updateResourceToIndex(indexResource);
    }

    @Operation(summary = "Find a class from a model")
    @ApiResponse(responseCode = "200", description = "Class found")
    @GetMapping(value = "/{prefix}/{resourceIdentifier}", produces = APPLICATION_JSON_VALUE)
    public ResourceInfoDTO getResource(@PathVariable String prefix, @PathVariable String resourceIdentifier){
        var graphUri = defaultNamespace + prefix;
        if(!jenaService.doesResourceExistInGraph(graphUri,graphUri + "#" + resourceIdentifier)){
            throw new ResourceNotFoundException("Resource does not exist");
        }

        var model = jenaService.getDataModel(graphUri);
        if(model == null){
            throw new ResourceNotFoundException(prefix);
        }

        var orgModel = jenaService.getOrganizations();
        var hasRightToModel = authorizationManager.hasRightToModel(prefix, model);

        var resourceInfoDTO = ResourceMapper.mapToResourceInfoDTO(model, graphUri, resourceIdentifier, orgModel, hasRightToModel, groupManagementService.mapUser());
        terminologyService.mapConceptToResource().accept(resourceInfoDTO);
        return resourceInfoDTO;
    }

    @Operation(summary = "Delete a resource from a data model")
    @ApiResponse(responseCode = "200", description = "Resource deleted successfully")
    @DeleteMapping(value = "/{prefix}/{resourceIdentifier}")
    public void deleteResource(@PathVariable String prefix, @PathVariable String resourceIdentifier){
        var modelURI = defaultNamespace + prefix;
        var resourceUri  = modelURI + "#" + resourceIdentifier;
        if(!jenaService.doesResourceExistInGraph(modelURI , resourceUri)){
            throw new ResourceNotFoundException(resourceUri);
        }
        var model = jenaService.getDataModel(modelURI);
        if(model == null){
            throw new ResourceNotFoundException(modelURI);
        }
        check(authorizationManager.hasRightToModel(prefix, model));

        jenaService.deleteResource(resourceUri);
        openSearchIndexer.deleteResourceFromIndex(resourceUri);
    }

}
