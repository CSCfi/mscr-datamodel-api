package fi.vm.yti.datamodel.api.v2.opensearch.index;

import java.util.Date;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UpdatedResourceDTO {
    private String id;
    private String handle;
    private String state;
    private Map<String, String> label;
    private Date contentModified;
    private Date stateModified;
    private Date modified;
    private Date created;
    private String sourceSchemaAggregationKey;
    private String targetSchemaAggregationKey;
    private String type;
    private String versionLabel;
    private String[] reasonCodes;
    
    
    
	public String[] getReasonCodes() {
		return reasonCodes;
	}
	public void setReasonCodes(String[] reasonCodes) {
		this.reasonCodes = reasonCodes;
	}
	public Date getCreated() {
		return created;
	}
	public void setCreated(Date created) {
		this.created = created;
	}
	public String getVersionLabel() {
		return versionLabel;
	}
	public void setVersionLabel(String versionLabel) {
		this.versionLabel = versionLabel;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public Date getModified() {
		return modified;
	}
	public void setModified(Date modified) {
		this.modified = modified;
	}
	public Date getStateModified() {
		return stateModified;
	}
	public void setStateModified(Date stateModified) {
		this.stateModified = stateModified;
	}
	public String getSourceSchemaAggregationKey() {
		return sourceSchemaAggregationKey;
	}
	public void setSourceSchemaAggregationKey(String sourceSchemaAggregationKey) {
		this.sourceSchemaAggregationKey = sourceSchemaAggregationKey;
	}
	public String getTargetSchemaAggregationKey() {
		return targetSchemaAggregationKey;
	}
	public void setTargetSchemaAggregationKey(String targetSchemaAggregationKey) {
		this.targetSchemaAggregationKey = targetSchemaAggregationKey;
	}
	@JsonProperty("uri")
	public String getId() {
		return id;
	}
	public String getHandle() {
		return handle;
	}
	public void setHandle(String handle) {
		this.handle = handle;
	}
	@JsonProperty("status")
	public String getState() {
		return state;
	}
	@JsonProperty("state")
	public void setState(String state) {
		this.state = state;
	}
	@JsonProperty("id")
	public void setId(String id) {
		this.id = id;
	}
	@JsonProperty("prefLabel")
	public Map<String, String> getLabel() {
		return label;
	}
	@JsonProperty("label")
	public void setLabel(Map<String, String> label) {
		this.label = label;
	}
	public Date getContentModified() {
		return contentModified;
	}
	public void setContentModified(Date contentModified) {
		this.contentModified = contentModified;
	}

}
