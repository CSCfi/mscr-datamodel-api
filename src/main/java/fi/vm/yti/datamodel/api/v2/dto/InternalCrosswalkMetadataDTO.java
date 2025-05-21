package fi.vm.yti.datamodel.api.v2.dto;

public interface InternalCrosswalkMetadataDTO extends InternalMSCRMetadataDTO {

	public String getSourceSchema();
	public void setSourceSchema(String value);
	public String getTargetSchema();
	public void setTargetSchema(String value);
}
