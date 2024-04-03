package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Map;

public class OwnerDTO {

	private String id;
	private String name;
	
	public OwnerDTO(String id, String name) {
		super();
		this.id = id;
		this.name = name;
	}	
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	public void setName(Map<String, String> names) {
		if(names.containsKey("en")) {
			this.name = names.get("en");
		}
		else {
			this.name = names.get(names.keySet().iterator().next());
		}
	}
	
	
}
