package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexResource;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexResourceInfo;
import fi.vm.yti.datamodel.api.v2.service.ClassService;
import fi.vm.yti.datamodel.api.v2.validator.ValidClass;
import fi.vm.yti.datamodel.api.v2.validator.ValidNodeShape;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("v2/class")
@Tag(name = "Class" )
@Validated
public class ClassController {

    private final ClassService classService;

    public ClassController(ClassService classService){
        this.classService = classService;
    }

    @Operation(summary = "Add a class to a model")
    @ApiResponse(responseCode = "201", description = "Class added to model successfully")
    @PostMapping(value = "/library/{prefix}", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createClass(@PathVariable String prefix, @RequestBody @ValidClass ClassDTO classDTO) throws URISyntaxException {
        var classUri = classService.create(prefix, classDTO, false);
        return ResponseEntity.created(classUri).build();
    }

    @Operation(summary = "Add a node shape to a model")
    @ApiResponse(responseCode = "201", description = "Class added to model successfully")
    @PostMapping(value = "/profile/{prefix}", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> createNodeShape(@PathVariable String prefix, @RequestBody @ValidNodeShape NodeShapeDTO nodeShapeDTO) throws URISyntaxException {
        var classUri = classService.create(prefix, nodeShapeDTO, true);
        return ResponseEntity.created(classUri).build();
    }

    @Operation(summary = "Update a class in a model")
    @ApiResponse(responseCode =  "204", description = "Class updated in model successfully")
    @PutMapping(value = "/library/{prefix}/{classIdentifier}", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> updateClass(@PathVariable String prefix, @PathVariable String classIdentifier, @RequestBody @ValidClass(updateClass = true) ClassDTO classDTO){
        classService.update(prefix, classIdentifier, classDTO);
        return ResponseEntity.noContent().build();
    }

    @Operation(summary = "Update a node shape in a model")
    @ApiResponse(responseCode =  "204", description = "Class updated in model successfully")
    @PutMapping(value = "/profile/{prefix}/{nodeShapeIdentifier}", consumes = APPLICATION_JSON_VALUE)
    public ResponseEntity<Void> updateNodeShape(@PathVariable String prefix, @PathVariable String nodeShapeIdentifier,
                                @RequestBody @ValidNodeShape(updateNodeShape = true) NodeShapeDTO nodeShapeDTO){
        classService.update(prefix, nodeShapeIdentifier, nodeShapeDTO);
        return ResponseEntity.noContent().build();
    }

    @Operation(summary = "Get a class from a data model")
    @ApiResponse(responseCode = "200", description = "Class found successfully")
    @GetMapping(value = "/library/{prefix}/{classIdentifier}", produces = APPLICATION_JSON_VALUE)
    public ClassInfoDTO getClass(@PathVariable String prefix, @PathVariable String classIdentifier){
        return (ClassInfoDTO) classService.get(prefix, classIdentifier);
    }

    @Operation(summary = "Add a class to a model")
    @ApiResponse(responseCode = "200", description = "Node shape found successfully")
    @GetMapping(value = "/profile/{prefix}/{nodeShapeIdentifier}", produces = APPLICATION_JSON_VALUE)
    public NodeShapeInfoDTO getNodeShape(@PathVariable String prefix, @PathVariable String nodeShapeIdentifier){
        return (NodeShapeInfoDTO) classService.get(prefix, nodeShapeIdentifier);
    }


    @Operation(summary = "Check if identifier for resource already exists")
    @ApiResponse(responseCode = "200", description = "Boolean value indicating whether prefix")
    @GetMapping(value = "/{prefix}/{identifier}/exists", produces = APPLICATION_JSON_VALUE)
    public Boolean freeIdentifier(@PathVariable String prefix, @PathVariable String identifier) {
        return classService.exists(prefix, identifier);
    }

    @Operation(summary = "Delete a class from a data model")
    @ApiResponse(responseCode = "200", description = "Class deleted successfully")
    @DeleteMapping(value = "/library/{prefix}/{classIdentifier}")
    public void deleteClass(@PathVariable String prefix, @PathVariable String classIdentifier){
        classService.delete(prefix, classIdentifier);
    }

    @Operation(summary = "Delete a class from a data model")
    @ApiResponse(responseCode = "200", description = "Class deleted successfully")
    @DeleteMapping(value = "/profile/{prefix}/{classIdentifier}")
    public void deleteNodeShape(@PathVariable String prefix, @PathVariable String classIdentifier){
        classService.delete(prefix, classIdentifier);
    }

    @Operation(summary = "Add property reference to node shape")
    @ApiResponse(responseCode = "200", description = "Property reference deleted successfully")
    @PutMapping(value = "/profile/{prefix}/{nodeShapeIdentifier}/properties")
    public void addNodeShapePropertyReference(@PathVariable String prefix, @PathVariable String nodeShapeIdentifier,
                                                 @RequestParam String uri) {
        classService.handlePropertyShapeReference(prefix, nodeShapeIdentifier, uri, false);
    }

    @Operation(summary = "Delete property reference from node shape")
    @ApiResponse(responseCode = "200", description = "Property reference deleted successfully")
    @DeleteMapping(value = "/profile/{prefix}/{nodeShapeIdentifier}/properties")
    public void deleteNodeShapePropertyReference(@PathVariable String prefix, @PathVariable String nodeShapeIdentifier,
                                                 @RequestParam String uri) {
        classService.handlePropertyShapeReference(prefix, nodeShapeIdentifier, uri, true);
    }

    @Operation(summary = "Get an external class from imports")
    @ApiResponse(responseCode = "200", description = "External class found successfully")
    @GetMapping(value = "/external", produces = APPLICATION_JSON_VALUE)
    public ExternalClassDTO getExternalClass(@RequestParam String uri) {
        return classService.getExternal(uri);
    }

    @Operation(summary = "Get all node shapes based on given targetClass")
    @ApiResponse(responseCode = "200", description = "List of node shapes fetched successfully")
    @GetMapping(value = "/nodeshapes", produces = APPLICATION_JSON_VALUE)
    public Collection<IndexResourceInfo> getNodeShapes(@RequestParam String targetClass) throws IOException {
        return classService.getNodeShapes(targetClass);
    }

    @Operation(summary = "Get all node shapes properties based on sh:node reference")
    @ApiResponse(responseCode = "200", description = "List of node shape's properties fetched successfully")
    @GetMapping(value = "/nodeshape/properties", produces = APPLICATION_JSON_VALUE)
    public Collection<IndexResource> getNodeShapeProperties(@RequestParam String nodeURI) throws IOException {
        return classService.getNodeShapeProperties(nodeURI);
    }

    @Operation(summary = "Toggles deactivation of a single property shape")
    @ApiResponse(responseCode = "200", description = "Deactivation has changes successfully")
    @PutMapping(value = "/toggle-deactivate/{prefix}", produces = APPLICATION_JSON_VALUE)
    public void deactivatePropertyShape(@PathVariable String prefix, @RequestParam String propertyUri) {
        classService.togglePropertyShape(prefix, propertyUri);
    }

}
