package fi.vm.yti.datamodel.api.v2.dto;

import java.util.ArrayList;
import java.util.List;

public class PropertyShapeDTO extends BaseDTO {
    private String path;
    private String classType;
    private ResourceType type;
    private String dataType;
    private List<String> allowedValues = new ArrayList<>();
    private String defaultValue;
    private String hasValue;
    private Integer maxLength;
    private Integer minLength;
    private Integer maxCount;
    private Integer minCount;
    private Integer minInclusive;
    private Integer maxInclusive;
    private Integer minExclusive;
    private Integer maxExclusive;

    private String codeList;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getClassType() {
        return classType;
    }

    public void setClassType(String classType) {
        this.classType = classType;
    }

    public ResourceType getType() {
        return type;
    }

    public void setType(ResourceType type) {
        this.type = type;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public List<String> getAllowedValues() {
        return allowedValues;
    }

    public void setAllowedValues(List<String> allowedValues) {
        this.allowedValues = allowedValues;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public String getHasValue() {
        return hasValue;
    }

    public void setHasValue(String hasValue) {
        this.hasValue = hasValue;
    }

    public Integer getMaxLength() {
        return maxLength;
    }

    public void setMaxLength(Integer maxLength) {
        this.maxLength = maxLength;
    }

    public Integer getMinLength() {
        return minLength;
    }

    public void setMinLength(Integer minLength) {
        this.minLength = minLength;
    }

    public Integer getMaxCount() {
        return maxCount;
    }

    public void setMaxCount(Integer maxCount) {
        this.maxCount = maxCount;
    }

    public Integer getMinCount() {
        return minCount;
    }

    public void setMinCount(Integer minCount) {
        this.minCount = minCount;
    }

    public Integer getMinInclusive() {
        return minInclusive;
    }

    public void setMinInclusive(Integer minInclusive) {
        this.minInclusive = minInclusive;
    }

    public Integer getMaxInclusive() {
        return maxInclusive;
    }

    public void setMaxInclusive(Integer maxInclusive) {
        this.maxInclusive = maxInclusive;
    }

    public Integer getMinExclusive() {
        return minExclusive;
    }

    public void setMinExclusive(Integer minExclusive) {
        this.minExclusive = minExclusive;
    }

    public Integer getMaxExclusive() {
        return maxExclusive;
    }

    public void setMaxExclusive(Integer maxExclusive) {
        this.maxExclusive = maxExclusive;
    }

    public String getCodeList() {
        return codeList;
    }

    public void setCodeList(String codeList) {
        this.codeList = codeList;
    }
}
