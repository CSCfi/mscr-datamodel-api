package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.dto.DataModelDTO;
import fi.vm.yti.datamodel.api.v2.dto.DataModelInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.ModelType;
import fi.vm.yti.datamodel.api.v2.service.DataModelService;
import fi.vm.yti.datamodel.api.v2.validator.ValidDatamodel;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.net.URISyntaxException;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("v2/model")
@Tag(name = "Model" )
@Validated
public class DataModelController {

    private final DataModelService dataModelService;

    public DataModelController(DataModelService dataModelService) {
        this.dataModelService = dataModelService;
    }

    @Operation(summary = "Create a new library")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new core model")
    @ApiResponse(responseCode = "201", description = "The ID for the newly created model")
    @PostMapping(path = "/library", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createLibrary(@ValidDatamodel(modelType = ModelType.LIBRARY) @RequestBody DataModelDTO modelDTO) throws URISyntaxException {
        var uri = dataModelService.create(modelDTO, ModelType.LIBRARY);
        return ResponseEntity.created(uri).build();
    }

    @Operation(summary = "Create a new application profile")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new application profile")
    @ApiResponse(responseCode = "201", description = "The ID for the newly created model")
    @PostMapping(path = "/profile", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createProfile(@ValidDatamodel(modelType = ModelType.PROFILE) @RequestBody DataModelDTO modelDTO) throws URISyntaxException {
        var uri = dataModelService.create(modelDTO, ModelType.PROFILE);
        return ResponseEntity.created(uri).build();
    }

    @Operation(summary = "Modify library")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new model node")
    @ApiResponse(responseCode = "204", description = "The ID for the newly created model")
    @PutMapping(path = "/library/{prefix}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> updateLibrary(@ValidDatamodel(modelType = ModelType.LIBRARY, updateModel = true) @RequestBody DataModelDTO modelDTO,
                                           @PathVariable String prefix) {
        dataModelService.update(prefix, modelDTO);
        return ResponseEntity.noContent().build();
    }

    @Operation(summary = "Modify application profile")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new model node")
    @ApiResponse(responseCode = "204", description = "The ID for the newly created model")
    @PutMapping(path = "/profile/{prefix}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> updateProfile(@ValidDatamodel(modelType = ModelType.PROFILE, updateModel = true) @RequestBody DataModelDTO modelDTO,
                              @PathVariable String prefix) {
        dataModelService.update(prefix, modelDTO);
        return ResponseEntity.noContent().build();
    }

    @Operation(summary = "Get a model from fuseki")
    @ApiResponse(responseCode = "200", description = "Datamodel object for the found model")
    @GetMapping(value = "/{prefix}", produces = APPLICATION_JSON_VALUE)
    public DataModelInfoDTO getModel(@PathVariable String prefix){
        return dataModelService.get(prefix);
    }

    @Operation(summary = "Check if prefix already exists")
    @ApiResponse(responseCode = "200", description = "Boolean value indicating whether prefix")
    @GetMapping(value = "/{prefix}/exists", produces = APPLICATION_JSON_VALUE)
    public Boolean exists(@PathVariable String prefix) {
        return dataModelService.exists(prefix);
    }

    @Operation(summary = "Delete a model from fuseki")
    @ApiResponse(responseCode = "200", description = "Model deleted successfully")
    @DeleteMapping(value = "/{prefix}")
    public void deleteModel(@PathVariable String prefix) {
        dataModelService.delete(prefix);
    }
}
