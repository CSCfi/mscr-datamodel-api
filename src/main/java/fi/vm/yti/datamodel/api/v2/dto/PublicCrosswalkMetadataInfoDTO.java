package fi.vm.yti.datamodel.api.v2.dto;

public class PublicCrosswalkMetadataInfoDTO extends PublicCrosswalkMetadataDTO {
	private String internalID;
	private String handle;
	public String getInternalID() {
		return internalID;
	}
	public void setInternalID(String internalID) {
		this.internalID = internalID;
	}
	public String getHandle() {
		return handle;
	}
	public void setHandle(String handle) {
		this.handle = handle;
	}
}
