package fi.vm.yti.datamodel.api.v2.dto;

import java.util.List;

public class OneOfDTO {

	private MappingFilterDTO filter;
	private List<MappingDTO> mappings;
	
	public OneOfDTO() {
		
	}

	public MappingFilterDTO getFilter() {
		return filter;
	}

	public void setFilter(MappingFilterDTO filter) {
		this.filter = filter;
	}

	public List<MappingDTO> getMappings() {
		return mappings;
	}

	public void setMappings(List<MappingDTO> mappings) {
		this.mappings = mappings;
	}
	
	
}
