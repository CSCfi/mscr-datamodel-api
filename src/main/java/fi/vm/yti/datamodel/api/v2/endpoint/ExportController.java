package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.service.DataModelService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("v2/export")
@Tag(name = "Export" )
@Validated
public class ExportController {

    private final DataModelService dataModelService;

    public ExportController(DataModelService dataModelService) {
        this.dataModelService = dataModelService;
    }

    @Operation(summary = "Get a datamodel or a single resource serialized")
    @ApiResponse(responseCode = "200", description = "Get and serialize resource successfully")
    @ApiResponse(responseCode = "404", description = "Resource not found")
    @GetMapping(value = {"{prefix}", "/{prefix}/{resource}"},
            produces = {"application/ld+json;charset=utf-8", "text/turtle;charset=utf-8", "application/rdf+xml;charset=utf-8"})
    public ResponseEntity<String> export(@PathVariable String prefix,
                                         @PathVariable(required = false) String resource,
                                         @RequestHeader(value = HttpHeaders.ACCEPT) String accept){
        return dataModelService.export(prefix, resource, accept);
    }
}
