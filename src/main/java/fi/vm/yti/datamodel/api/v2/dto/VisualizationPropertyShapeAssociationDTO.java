package fi.vm.yti.datamodel.api.v2.dto;

public class VisualizationPropertyShapeAssociationDTO extends VisualizationAssociationDTO {
    private String path;
    private Integer minCount;
    private Integer maxCount;

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
}
