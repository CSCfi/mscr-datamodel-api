package fi.vm.yti.datamodel.api.v2.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GroupManagementOrganizationDTO {
    private String uuid;
    private Map<String,String> prefLabel;
    private Map<String,String> description;
    private String url;

    private String parentId;

    
    public GroupManagementOrganizationDTO() {
    	uuid = "7d3a3c00-5a6b-489b-a3ed-63bb58c26a63";
    	
    	prefLabel = new HashMap<String, String>();
		description = new HashMap<String, String>();
		prefLabel.put("en", "test");
		prefLabel.put("fi", "test");
		prefLabel.put("sv", "test");
		description.put("en", "test");
		description.put("fi", "test");
		description.put("sv", "test");
		
    }
    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    
    public Map<String, String> getPrefLabel() {
        return prefLabel;
    }

    public void setPrefLabel(Map<String, String> prefLabel) {
        this.prefLabel = prefLabel;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public Map<String, String> getDescription() {
        return description;
    }

    public void setDescription(Map<String, String> description) {
        this.description = description;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
