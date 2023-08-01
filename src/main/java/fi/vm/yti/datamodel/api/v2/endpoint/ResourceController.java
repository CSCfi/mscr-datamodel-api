package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.service.ResourceService;
import fi.vm.yti.datamodel.api.v2.validator.ValidPropertyShape;
import fi.vm.yti.datamodel.api.v2.validator.ValidResource;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.net.URISyntaxException;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("v2/resource")
@Tag(name = "Resource" )
@Validated
public class ResourceController {

    private final ResourceService resourceService;

    public ResourceController(ResourceService resourceService) {
        this.resourceService = resourceService;
    }

    @Operation(summary = "Add a attribute to a model")
    @ApiResponse(responseCode = "201", description = "Attribute added to model successfully")
    @PostMapping(value = "/library/{prefix}/attribute", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createAttribute(@PathVariable String prefix, @RequestBody @ValidResource(resourceType = ResourceType.ATTRIBUTE) ResourceDTO dto) throws URISyntaxException {
        var uri = resourceService.create(prefix, dto, ResourceType.ATTRIBUTE, false);
        return ResponseEntity.created(uri).build();
    }

    @Operation(summary = "Add a association to a model")
    @ApiResponse(responseCode = "201", description = "Association added to model successfully")
    @PostMapping(value = "/library/{prefix}/association", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createAssociation(@PathVariable String prefix, @RequestBody @ValidResource(resourceType = ResourceType.ASSOCIATION) ResourceDTO dto) throws URISyntaxException {
        var uri = resourceService.create(prefix, dto, ResourceType.ASSOCIATION, false);
        return ResponseEntity.created(uri).build();
    }

    @Operation(summary = "Add a property shape to a profile")
    @ApiResponse(responseCode = "201", description = "Property shape added to profile successfully")
    @PostMapping(value = "/profile/{prefix}", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createPropertyShape(@PathVariable String prefix, @ValidPropertyShape @RequestBody PropertyShapeDTO dto) throws URISyntaxException {
        var uri = resourceService.create(prefix, dto, null, true);
        return ResponseEntity.created(uri).build();
    }

    @Operation(summary = "Update a resource in a model")
    @ApiResponse(responseCode = "204", description = "Resource updated to model successfully")
    @PutMapping(value = "/library/{prefix}/attribute/{resourceIdentifier}", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> updateAttribute(@PathVariable String prefix, @PathVariable String resourceIdentifier,
                               @RequestBody @ValidResource(resourceType = ResourceType.ATTRIBUTE, updateProperty = true) ResourceDTO dto){
        resourceService.update(prefix, resourceIdentifier, dto);
        return ResponseEntity.noContent().build();
    }

    @Operation(summary = "Update a resource in a model")
    @ApiResponse(responseCode = "204", description = "Resource updated to model successfully")
    @PutMapping(value = "/library/{prefix}/association/{resourceIdentifier}", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> updateAssociation(@PathVariable String prefix, @PathVariable String resourceIdentifier,
                               @RequestBody @ValidResource(resourceType = ResourceType.ASSOCIATION, updateProperty = true) ResourceDTO dto){
        resourceService.update(prefix, resourceIdentifier, dto);
        return ResponseEntity.noContent().build();
    }

    @Operation(summary = "Update a property shape in a model")
    @ApiResponse(responseCode = "204", description = "Resource updated to model successfully")
    @PutMapping(value = "/profile/{prefix}/{propertyShapeIdentifier}", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> updatePropertyShape(@PathVariable String prefix, @PathVariable String propertyShapeIdentifier,
                                    @RequestBody @ValidPropertyShape(updateProperty = true) PropertyShapeDTO dto){
        resourceService.update(prefix, propertyShapeIdentifier, dto);
        return ResponseEntity.noContent().build();
    }

    @Operation(summary = "Create a local copy of a property shape")
    @ApiResponse(responseCode = "204", description = "Property shape copied successfully")
    @PostMapping(value ="/profile/{prefix}/{propertyShapeIdentifier}")
    public ResponseEntity<String> copyPropertyShape(@PathVariable String prefix, @PathVariable String propertyShapeIdentifier, @RequestParam String targetPrefix, @RequestParam String newIdentifier) throws URISyntaxException {
        var uri = resourceService.copyPropertyShape(prefix, propertyShapeIdentifier, targetPrefix, newIdentifier);
        return ResponseEntity.created(uri).build();
    }

    @Operation(summary = "Find an attribute or association from a model")
    @ApiResponse(responseCode = "200", description = "Attribute or association found")
    @GetMapping(value = "/library/{prefix}/{resourceIdentifier}", produces = APPLICATION_JSON_VALUE)
    public ResourceInfoDTO getResource(@PathVariable String prefix, @PathVariable String resourceIdentifier){
        //TODO check if this can be generic instead of casting
        return (ResourceInfoDTO) resourceService.get(prefix, resourceIdentifier);
    }

    @Operation(summary = "Find a property shape from a profile")
    @ApiResponse(responseCode = "200", description = "Property shape found")
    @GetMapping(value = "/profile/{prefix}/{propertyShapeIdentifier}", produces = APPLICATION_JSON_VALUE)
    public PropertyShapeInfoDTO getPropertyShape(@PathVariable String prefix, @PathVariable String propertyShapeIdentifier){
        return (PropertyShapeInfoDTO) resourceService.get(prefix, propertyShapeIdentifier);
    }

    @Operation(summary = "Get an external resource from imports")
    @ApiResponse(responseCode = "200", description = "External class found successfully")
    @GetMapping(value = "/external", produces = APPLICATION_JSON_VALUE)
    public ExternalResourceDTO getExternalClass(@RequestParam String uri) {
        return resourceService.getExternal(uri);
    }

    @Operation(summary = "Check if identifier for resource already exists")
    @ApiResponse(responseCode = "200", description = "Boolean value indicating whether prefix")
    @GetMapping(value = "/{prefix}/{identifier}/exists", produces = APPLICATION_JSON_VALUE)
    public Boolean freeIdentifier(@PathVariable String prefix, @PathVariable String identifier) {
        return resourceService.exists(prefix, identifier);
    }

    @Operation(summary = "Delete a resource from a data model")
    @ApiResponse(responseCode = "200", description = "Resource deleted successfully")
    @DeleteMapping(value = "/library/{prefix}/{resourceIdentifier}")
    public void deleteResource(@PathVariable String prefix, @PathVariable String resourceIdentifier){
        resourceService.delete(prefix, resourceIdentifier);
    }

    @Operation(summary = "Delete a resource from a data model")
    @ApiResponse(responseCode = "200", description = "Resource deleted successfully")
    @DeleteMapping(value = "/profile/{prefix}/{propertyShapeIdentifier}")
    public void deletePropertyShape(@PathVariable String prefix, @PathVariable String propertyShapeIdentifier) {
        // TODO: need to check node shapes' sh:property if resource is added there
        resourceService.delete(prefix, propertyShapeIdentifier);
    }
}
