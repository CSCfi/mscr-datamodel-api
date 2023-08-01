package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.dto.PositionDataDTO;
import fi.vm.yti.datamodel.api.v2.dto.VisualizationResultDTO;
import fi.vm.yti.datamodel.api.v2.service.VisualizationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("v2/visualization")
@Tag(name = "Visualization" )
public class VisualizationController {

    private final VisualizationService visualizationService;

    public VisualizationController(VisualizationService visualizationService) {
        this.visualizationService = visualizationService;
    }

    @Operation(summary = "Get data for model visualization")
    @ApiResponse(responseCode = "200", description = "Visualization data found for model")
    @GetMapping(value = "/{prefix}", produces = APPLICATION_JSON_VALUE)
    public VisualizationResultDTO getVisualizationData(@PathVariable String prefix) {
        return visualizationService.getVisualizationData(prefix);
    }

    @Operation(summary = "Saves position data for visualization components")
    @ApiResponse(responseCode = "204", description = "Visualization data saved or updated for the model")
    @PutMapping(value = "/{prefix}/positions")
    public ResponseEntity<Void> savePositions(@PathVariable String prefix, @RequestBody List<PositionDataDTO> positions) {
        visualizationService.savePositionData(prefix, positions);
        return ResponseEntity.noContent().build();
    }
}
