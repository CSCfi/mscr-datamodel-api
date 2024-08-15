package fi.vm.yti.datamodel.api.v2.dto;

public class UpdateResponseDTO {
	private String message;
	private String id;
	
	public UpdateResponseDTO(String message, String id) {
		super();
		this.message = message;
		this.id = id;
	}

	public String getMessage() {
		return message;
	}

	public String getId() {
		return id;
	}
	
}
