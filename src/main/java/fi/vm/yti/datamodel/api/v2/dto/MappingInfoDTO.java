package fi.vm.yti.datamodel.api.v2.dto;

public class MappingInfoDTO extends MappingDTO {

	protected String PID;
	protected String isPartOf;

	
	public MappingInfoDTO() {}

	public String getPID() {
		return PID;
	}

	public void setPID(String pID) {
		PID = pID;
	}
	
	
	public String getIsPartOf() {
		return isPartOf;
	}

	public void setIsPartOf(String isPartOf) {
		this.isPartOf = isPartOf;
	}

	
}
