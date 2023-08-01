package fi.vm.yti.datamodel.api.v2.dto;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ClassInfoDTO extends ResourceInfoBaseDTO {
    private Set<UriDTO> equivalentClass;
    private Set<UriDTO> subClassOf;
    private List<SimpleResourceDTO> attribute = new ArrayList<>();
    private List<SimpleResourceDTO> association = new ArrayList<>();

    public Set<UriDTO> getEquivalentClass() {
        return equivalentClass;
    }

    public void setEquivalentClass(Set<UriDTO> equivalentClass) {
        this.equivalentClass = equivalentClass;
    }

    public Set<UriDTO> getSubClassOf() {
        return subClassOf;
    }

    public void setSubClassOf(Set<UriDTO> subClassOf) {
        this.subClassOf = subClassOf;
    }

    public List<SimpleResourceDTO> getAttribute() {
        return attribute;
    }

    public void setAttribute(List<SimpleResourceDTO> attribute) {
        this.attribute = attribute;
    }

    public List<SimpleResourceDTO> getAssociation() {
        return association;
    }

    public void setAssociation(List<SimpleResourceDTO> association) {
        this.association = association;
    }
}
