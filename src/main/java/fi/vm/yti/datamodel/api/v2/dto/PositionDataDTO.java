package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Set;

public class PositionDataDTO {
    private String identifier;
    private Double x;
    private Double y;
    private Set<String> referenceTargets;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public Double getX() {
        return x;
    }

    public void setX(Double x) {
        this.x = x;
    }

    public Double getY() {
        return y;
    }

    public void setY(Double y) {
        this.y = y;
    }

    public Set<String> getReferenceTargets() {
        return referenceTargets;
    }

    public void setReferenceTargets(Set<String> referenceTargets) {
        this.referenceTargets = referenceTargets;
    }
}
