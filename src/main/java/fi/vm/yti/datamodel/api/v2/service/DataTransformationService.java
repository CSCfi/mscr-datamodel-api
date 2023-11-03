package fi.vm.yti.datamodel.api.v2.service;

import java.util.List;
import java.util.Map;

import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;

public interface DataTransformationService {

	//public String transform(String sourceDocument, String sourceFormat, String targetFormat, String crosswalkID) throws Exception;
	public String transform(String sourceDocument, String sourceFormat, List<MappingDTO> mappings, String targetFormat) throws Exception;
	public String transform(String sourceDocument, String sourceFormat, List<MappingDTO> mappings, String targetFormat, Map<String, String> namespaces, String rootElement) throws Exception;

}
