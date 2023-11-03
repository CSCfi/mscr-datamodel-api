package fi.vm.yti.datamodel.api.v2.dto;

import java.util.List;

public class FunctionDTO {

	private String name;
	private String uri;
	private String description;
	private List<ParameterDTO> parameters;
	private List<OutputDTO> outputs;
	
	public FunctionDTO() {
		
	}
	public FunctionDTO(String name, String uri, String description, List<ParameterDTO> parameters,
			List<OutputDTO> outputs) {
		super();
		this.name = name;
		this.uri = uri;
		this.description = description;
		this.parameters = parameters;
		this.outputs = outputs;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public List<ParameterDTO> getParameters() {
		return parameters;
	}
	public void setParameters(List<ParameterDTO> parameters) {
		this.parameters = parameters;
	}
	public List<OutputDTO> getOutputs() {
		return outputs;
	}
	public void setOutputs(List<OutputDTO> outputs) {
		this.outputs = outputs;
	}
	
	
}
