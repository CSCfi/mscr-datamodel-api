package fi.vm.yti.datamodel.api.v2.dto;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class CrosswalkDTO extends DataModelDTO implements MSCRCommonMetadata {

	private MSCRState state;
	private MSCRVisibility visibility = MSCRVisibility.PUBLIC;

	private CrosswalkFormat format;
	private String sourceSchema;
	private String targetSchema;
	private String versionLabel;
	private String sourceURL;
	
	
	public String getSourceURL() {
		return sourceURL;
	}

	public void setSourceURL(String sourceURL) {
		this.sourceURL = sourceURL;
	}	
		
	
	public MSCRState getState() {
		return state;
	}
	public void setState(MSCRState state) {
		this.state = state;
	}
	
	public MSCRVisibility getVisibility() {
		return visibility;
	}
	public void setVisibility(MSCRVisibility visibility) {
		this.visibility = visibility;
	}
	public String getSourceSchema() {
		return sourceSchema;
	}
	public void setSourceSchema(String sourceSchema) {
		this.sourceSchema = sourceSchema;
	}
	public String getTargetSchema() {
		return targetSchema;
	}
	public void setTargetSchema(String targetSchema) {
		this.targetSchema = targetSchema;
	}
	
	
	public CrosswalkFormat getFormat() {
		return format;
	}

	public void setFormat(CrosswalkFormat type) {
		this.format = type;
	}
	
	public String getVersionLabel() {
		return versionLabel;
	}

	public void setVersionLabel(String versionLabel) {
		this.versionLabel = versionLabel;
	}


	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
