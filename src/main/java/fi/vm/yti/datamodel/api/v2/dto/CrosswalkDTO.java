package fi.vm.yti.datamodel.api.v2.dto;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class CrosswalkDTO extends DataModelDTO {

	private CrosswalkFormat format;
	private String sourceSchema;
	private String targetSchema;
	private String versionLabel;
	
	
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
