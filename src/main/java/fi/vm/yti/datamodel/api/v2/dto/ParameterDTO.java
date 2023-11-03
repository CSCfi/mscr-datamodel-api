package fi.vm.yti.datamodel.api.v2.dto;


public class ParameterDTO {


	private String name;
	private String datatype;
	private boolean isRequired;
	
	public ParameterDTO() {}
	public ParameterDTO(String name, String datatype, boolean isRequired) {
		super();
		this.name = name;
		this.datatype = datatype;
		this.isRequired = isRequired;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getDatatype() {
		return datatype;
	}
	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}
	public boolean isRequired() {
		return isRequired;
	}
	public void setRequired(boolean isRequired) {
		this.isRequired = isRequired;
	}
}
