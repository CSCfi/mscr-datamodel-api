package fi.vm.yti.datamodel.api.v2.dto;

public interface InternalSchemaMetadataDTO extends InternalMSCRMetadataDTO {
	public String getNamespace();
	public void setNamespace(String namespace);

}
