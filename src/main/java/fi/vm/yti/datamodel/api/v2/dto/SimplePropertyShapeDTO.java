package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Map;

public class SimplePropertyShapeDTO {
    private String identifier;
    private String uri;
    private String modelId;
    private Map<String, String> label;
    private boolean deactivated;
    private boolean fromShNode;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public Map<String, String> getLabel() {
        return label;
    }

    public void setLabel(Map<String, String> label) {
        this.label = label;
    }

    public boolean isDeactivated() {
        return deactivated;
    }

    public void setDeactivated(boolean deactivated) {
        this.deactivated = deactivated;
    }

    public boolean isFromShNode() {
        return fromShNode;
    }

    public void setFromShNode(boolean fromShNode) {
        this.fromShNode = fromShNode;
    }
}
