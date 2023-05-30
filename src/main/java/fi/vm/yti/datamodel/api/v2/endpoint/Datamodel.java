package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.DataModelDTO;
import fi.vm.yti.datamodel.api.v2.dto.DataModelInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.ModelMapper;
import fi.vm.yti.datamodel.api.v2.service.GroupManagementService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.TerminologyService;
import fi.vm.yti.datamodel.api.v2.validator.ValidDatamodel;
import fi.vm.yti.datamodel.api.v2.validator.ValidationConstants;
import fi.vm.yti.security.AuthenticatedUserProvider;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("v2/model")
@Tag(name = "Model" )
@Validated
public class Datamodel {

    private static final Logger logger = LoggerFactory.getLogger(Datamodel.class);

    private final AuthorizationManager authorizationManager;

    private final OpenSearchIndexer openSearchIndexer;

    private final JenaService jenaService;

    private final ModelMapper mapper;

    private final TerminologyService terminologyService;

    private final AuthenticatedUserProvider userProvider;

    private final GroupManagementService groupManagementService;
    
    private String defaultNamespace;

    public Datamodel(JenaService jenaService,
                     AuthorizationManager authorizationManager,
                     OpenSearchIndexer openSearchIndexer,
                     ModelMapper modelMapper,
                     TerminologyService terminologyService,
                     AuthenticatedUserProvider userProvider,
                     GroupManagementService groupManagementService,
                     @Value("${defaultNamespace}") String defaultNamespace) {
        this.authorizationManager = authorizationManager;
        this.mapper = modelMapper;
        this.openSearchIndexer = openSearchIndexer;
        this.jenaService = jenaService;
        this.terminologyService = terminologyService;
        this.userProvider = userProvider;
        this.groupManagementService = groupManagementService;
        this.defaultNamespace = defaultNamespace;
    }

    @Operation(summary = "Create a new model")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new model node")
    @ApiResponse(responseCode = "200", description = "The ID for the newly created model")
    @PutMapping(produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public void createModel(@ValidDatamodel @RequestBody DataModelDTO modelDTO) {
        logger.info("Create model {}", modelDTO);
        check(authorizationManager.hasRightToAnyOrganization(modelDTO.getOrganizations()));

        terminologyService.resolveTerminology(modelDTO.getTerminologies());
        var jenaModel = mapper.mapToJenaModel(modelDTO, userProvider.getUser());

        jenaService.putDataModelToCore(this.defaultNamespace + modelDTO.getPrefix(), jenaModel);

        var indexModel = mapper.mapToIndexModel(modelDTO.getPrefix(), jenaModel);
        openSearchIndexer.createModelToIndex(indexModel);
    }

    @Operation(summary = "Modify model")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new model node")
    @ApiResponse(responseCode = "200", description = "The ID for the newly created model")
    @PostMapping(path = "/{prefix}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public void updateModel(@ValidDatamodel(updateModel = true) @RequestBody DataModelDTO modelDTO,
                            @PathVariable String prefix) {
        logger.info("Updating model {}", modelDTO);

        var oldModel = jenaService.getDataModel(this.defaultNamespace + prefix);
        if(oldModel == null){
            throw new ResourceNotFoundException(prefix);
        }

        check(authorizationManager.hasRightToModel(prefix, oldModel));

        terminologyService.resolveTerminology(modelDTO.getTerminologies());

        var jenaModel = mapper.mapToUpdateJenaModel(prefix, modelDTO, oldModel, userProvider.getUser());

        jenaService.putDataModelToCore(this.defaultNamespace + prefix, jenaModel);


        var indexModel = mapper.mapToIndexModel(prefix, jenaModel);
        openSearchIndexer.updateModelToIndex(indexModel);
    }

    @Operation(summary = "Get a model from fuseki")
    @ApiResponse(responseCode = "200", description = "Datamodel object for the found model")
    @GetMapping(value = "/{prefix}", produces = APPLICATION_JSON_VALUE)
    public DataModelInfoDTO getModel(@PathVariable String prefix){
        var model = jenaService.getDataModel(this.defaultNamespace + prefix);
        var hasRightsToModel = authorizationManager.hasRightToModel(prefix, model);

        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;
        return mapper.mapToDataModelDTO(prefix, model, userMapper);
    }

    @Operation(summary = "Check if prefix already exists")
    @ApiResponse(responseCode = "200", description = "Boolean value indicating whether prefix")
    @GetMapping(value = "/freePrefix/{prefix}", produces = APPLICATION_JSON_VALUE)
    public Boolean freePrefix(@PathVariable String prefix) {
        if (ValidationConstants.RESERVED_WORDS.contains(prefix)) {
            return false;
        }
        return !jenaService.doesDataModelExist(this.defaultNamespace + prefix);
    }

    @Operation(summary = "Delete a model from fuseki")
    @ApiResponse(responseCode = "200", description = "Model deleted successfully")
    @DeleteMapping(value = "/{prefix}")
    public void deleteModel(@PathVariable String prefix) {
        var modelUri = this.defaultNamespace + prefix;
        if(!jenaService.doesDataModelExist(modelUri)){
            throw new ResourceNotFoundException(modelUri);
        }
        var model = jenaService.getDataModel(modelUri);
        if(model == null){
            throw new ResourceNotFoundException(modelUri);
        }
        check(authorizationManager.hasRightToModel(prefix, model));

        jenaService.deleteDataModel(modelUri);
        openSearchIndexer.deleteModelFromIndex(modelUri);
    }
}
