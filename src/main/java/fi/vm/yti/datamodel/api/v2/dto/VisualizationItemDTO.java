package fi.vm.yti.datamodel.api.v2.dto;

import com.google.common.base.Objects;

import java.util.Map;

public class VisualizationItemDTO {
    private String identifier;
    private Map<String, String> label;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public Map<String, String> getLabel() {
        return label;
    }

    public void setLabel(Map<String, String> label) {
        this.label = label;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VisualizationItemDTO that = (VisualizationItemDTO) o;
        return Objects.equal(identifier, that.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(identifier);
    }
}
