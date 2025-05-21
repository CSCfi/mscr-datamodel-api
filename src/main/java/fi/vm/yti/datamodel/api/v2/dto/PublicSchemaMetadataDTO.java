package fi.vm.yti.datamodel.api.v2.dto;

public class PublicSchemaMetadataDTO extends PublicMSCRMetadataDTO {
	private String namespace;
	public String getNamespace() {
		return namespace;
	}
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
}
