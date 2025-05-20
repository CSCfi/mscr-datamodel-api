package fi.vm.yti.datamodel.api.v2.dto;

public class PublicCrosswalkMetadataDTO extends PublicMSCRMetadataDTO {

	private String sourceSchema;
	private String targetSchema;
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
	
	
}
