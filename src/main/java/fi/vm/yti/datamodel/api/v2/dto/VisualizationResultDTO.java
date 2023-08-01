package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Set;

public class VisualizationResultDTO {
    Set<VisualizationClassDTO> nodes;
    Set<VisualizationHiddenNodeDTO> hiddenNodes;

    public Set<VisualizationClassDTO> getNodes() {
        return nodes;
    }

    public void setNodes(Set<VisualizationClassDTO> nodes) {
        this.nodes = nodes;
    }

    public Set<VisualizationHiddenNodeDTO> getHiddenNodes() {
        return hiddenNodes;
    }

    public void setHiddenNodes(Set<VisualizationHiddenNodeDTO> hiddenNodes) {
        this.hiddenNodes = hiddenNodes;
    }
}
