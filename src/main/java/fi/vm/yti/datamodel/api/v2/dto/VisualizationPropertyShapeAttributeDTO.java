package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Set;

public class VisualizationPropertyShapeAttributeDTO extends VisualizationAttributeDTO {
    private String path;
    private Integer minCount;
    private Integer maxCount;
    private String dataType;
    private Set<String> codeLists;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Integer getMinCount() {
        return minCount;
    }

    public void setMinCount(Integer minCount) {
        this.minCount = minCount;
    }

    public Integer getMaxCount() {
        return maxCount;
    }

    public void setMaxCount(Integer maxCount) {
        this.maxCount = maxCount;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Set<String> getCodeLists() {
        return codeLists;
    }

    public void setCodeLists(Set<String> codeLists) {
        this.codeLists = codeLists;
    }
}
