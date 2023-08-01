package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Set;

public class NodeShapeDTO extends BaseDTO {
    private String targetClass;
    private String targetNode;
    private Set<String> properties;

    public String getTargetNode() {
        return targetNode;
    }

    public void setTargetNode(String targetNode) {
        this.targetNode = targetNode;
    }

    public Set<String> getProperties() {
        return properties;
    }

    public void setProperties(Set<String> properties) {
        this.properties = properties;
    }

    public String getTargetClass() {
        return targetClass;
    }

    public void setTargetClass(String targetClass) {
        this.targetClass = targetClass;
    }
}
