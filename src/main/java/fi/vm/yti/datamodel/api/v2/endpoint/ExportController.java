package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.SKOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.io.StringWriter;

@RestController
@RequestMapping("v2/export")
@Tag(name = "Export" )
@Validated
public class ExportController {

    private static final Logger LOG = LoggerFactory.getLogger(ExportController.class);
    private final JenaService jenaService;
    private final AuthorizationManager authorizationManager;
    private String defaultNamespace;

    public ExportController(JenaService jenaService,
                            AuthorizationManager authorizationManager,
                            @Value("${defaultNamespace}") String defaultNamespace) {
        this.jenaService = jenaService;
        this.authorizationManager = authorizationManager;
        this.defaultNamespace = defaultNamespace;
    }

    @Operation(summary = "Get a datamodel or a single resource serialized")
    @ApiResponse(responseCode = "200", description = "Get and serialize resource successfully")
    @ApiResponse(responseCode = "404", description = "Resource not found")
    @GetMapping(value = {"{prefix}", "/{prefix}/{resource}"},
            produces = {"application/ld+json;charset=utf-8", "text/turtle;charset=utf-8", "application/rdf+xml;charset=utf-8"})
    public ResponseEntity<String> export(@PathVariable String prefix,
                                         @PathVariable(required = false) String resource,
                                         @RequestHeader(value = HttpHeaders.ACCEPT) String accept){
        var modelURI = this.defaultNamespace + prefix;
        LOG.info("Exporting datamodel {}, {}", modelURI, accept);

        Model exportedModel;
        Model model;

        try {
            model = jenaService.getDataModel(modelURI);
        } catch (ResourceNotFoundException e) {
            // cannot throw ResourceNotFoundException because accept header is not application/json
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            LOG.error("Error exporting datamodel", e);
            return ResponseEntity.internalServerError().build();
        }

        if (resource != null) {
            var res = model.getResource(modelURI + "#" + resource);
            var properties = res.listProperties();
            if (!properties.hasNext()) {
                return ResponseEntity.notFound().build();
            }
            exportedModel = properties.toModel();
        } else {
            exportedModel = model;
        }

        exportedModel.setNsPrefixes(ModelConstants.PREFIXES);

        // remove editorial notes from resources
        if (!authorizationManager.hasRightToModel(prefix, model)) {
            var hiddenValues = model.listStatements(
                    new SimpleSelector(null, SKOS.editorialNote, (String) null)).toList();
            exportedModel.remove(hiddenValues);
        }

        var stringWriter = new StringWriter();
        switch (accept) {
            case "text/turtle":
                RDFDataMgr.write(stringWriter, exportedModel, Lang.TURTLE);
                break;
            case "application/rdf+xml":
                RDFDataMgr.write(stringWriter, exportedModel, Lang.RDFXML);
                break;
            case "application/ld+json":
            default:
                RDFDataMgr.write(stringWriter, exportedModel, Lang.JSONLD);
        }
        return ResponseEntity.ok().body(stringWriter.toString());
    }
}
