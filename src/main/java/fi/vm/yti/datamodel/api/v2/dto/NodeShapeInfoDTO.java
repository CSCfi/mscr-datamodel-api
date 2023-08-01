package fi.vm.yti.datamodel.api.v2.dto;

import java.util.ArrayList;
import java.util.List;

public class NodeShapeInfoDTO extends ResourceInfoBaseDTO {
    private UriDTO targetClass;
    private UriDTO targetNode;
    private List<SimplePropertyShapeDTO> attribute = new ArrayList<>();
    private List<SimplePropertyShapeDTO> association = new ArrayList<>();

    public UriDTO getTargetClass() {
        return targetClass;
    }

    public void setTargetClass(UriDTO targetClass) {
        this.targetClass = targetClass;
    }

    public UriDTO getTargetNode() {
        return targetNode;
    }

    public void setTargetNode(UriDTO targetNode) {
        this.targetNode = targetNode;
    }

    public List<SimplePropertyShapeDTO> getAttribute() {
        return attribute;
    }

    public void setAttribute(List<SimplePropertyShapeDTO> attribute) {
        this.attribute = attribute;
    }

    public List<SimplePropertyShapeDTO> getAssociation() {
        return association;
    }

    public void setAssociation(List<SimplePropertyShapeDTO> association) {
        this.association = association;
    }
}
