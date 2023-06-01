package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.dto.VisualizationClassDTO;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.VisualizationMapper;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.jena.rdf.model.ModelFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("v2/visualization")
@Tag(name = "Visualization" )
public class VisualizationController {

	private final VisualizationMapper visualizationMapper;
    private final JenaService jenaService;
    
    @Value("${defaultNamespace}")
    private String defaultNamespace;

    public VisualizationController(JenaService jenaService, VisualizationMapper visualizationMapper) {
        this.jenaService = jenaService;
        this.visualizationMapper = visualizationMapper;
    }

    @Operation(summary = "Get data for model visualization")
    @ApiResponse(responseCode = "200", description = "Visualization data found for model")
    @GetMapping(value = "/{prefix}", produces = APPLICATION_JSON_VALUE)
    public List<VisualizationClassDTO> getVisualizationData(@PathVariable String prefix) {
        var graph = defaultNamespace + prefix;
        var model = jenaService.getDataModel(graph);
        var positions = ModelFactory.createDefaultModel();

        if(model == null) {
            throw new ResourceNotFoundException(graph);
        }

        return visualizationMapper.mapVisualizationData(prefix, model, positions);
    }
}
