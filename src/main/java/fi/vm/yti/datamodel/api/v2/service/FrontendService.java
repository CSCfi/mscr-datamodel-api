package fi.vm.yti.datamodel.api.v2.service;

import static fi.vm.yti.datamodel.api.v2.dto.ModelConstants.DEFAULT_LANGUAGE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jena.rdf.model.Model;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.datamodel.api.v2.dto.CrosswalkEditorSchemaDTO;
import fi.vm.yti.datamodel.api.v2.dto.FunctionDTO;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.OrganizationDTO;
import fi.vm.yti.datamodel.api.v2.dto.OutputDTO;
import fi.vm.yti.datamodel.api.v2.dto.ParameterDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.ServiceCategoryDTO;
import fi.vm.yti.datamodel.api.v2.mapper.FunctionMapper;
import fi.vm.yti.datamodel.api.v2.mapper.OrganizationMapper;
import fi.vm.yti.datamodel.api.v2.mapper.ServiceCategoryMapper;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;

import io.zenwave360.jsonrefparser.$RefParser;
import io.zenwave360.jsonrefparser.$Refs;

@Service
public class FrontendService {

    private final CoreRepository coreRepository;

    
    
    public FrontendService(CoreRepository coreRepository) {
        this.coreRepository = coreRepository;
        
    }

    public List<OrganizationDTO> getOrganizations(@NotNull String sortLanguage, boolean includeChildOrganizations) {
        var organizations = coreRepository.getOrganizations();
        var dtos = OrganizationMapper.mapToListOrganizationDTO(organizations);

        dtos.sort((a, b) -> {
            var labelA = a.getLabel().getOrDefault(sortLanguage, a.getLabel().get(DEFAULT_LANGUAGE));
            var labelB = b.getLabel().getOrDefault(sortLanguage, b.getLabel().get(DEFAULT_LANGUAGE));
            return labelA.compareTo(labelB);
        });

        return includeChildOrganizations ? dtos : dtos.stream()
                .filter(dto -> dto.getParentOrganization() == null)
                .toList();
    }

    public List<ServiceCategoryDTO> getServiceCategories(@NotNull String sortLanguage) {
        var serviceCategories = coreRepository.getServiceCategories();
        var dtos = ServiceCategoryMapper.mapToListServiceCategoryDTO(serviceCategories);

        dtos.sort((a, b) -> {
            var labelA = a.getLabel().getOrDefault(sortLanguage, a.getLabel().get(DEFAULT_LANGUAGE));
            var labelB = b.getLabel().getOrDefault(sortLanguage, b.getLabel().get(DEFAULT_LANGUAGE));
            return labelA.compareTo(labelB);
        });

        return dtos;
    }
    
    public List<FunctionDTO> getFunctions() {
    	Model model = coreRepository.fetch(MSCR.FUNCTIONS_GRAPH);
    	return FunctionMapper.mapFunctionsToDTO(model);
    }

	public CrosswalkEditorSchemaDTO getSchema(String contentString, SchemaInfoDTO metadata) throws Exception {
		ObjectMapper mapper = new ObjectMapper(); // turn into a bean
		$RefParser parser = new $RefParser(contentString);
			
		$Refs refs = parser.parse().dereference().mergeAllOf().getRefs();
		Object resultMapOrList = refs.schema();
		if(resultMapOrList instanceof Map) {
			Map<String, Object> map = (Map<String, Object>)resultMapOrList;
			map.remove("definitions");
		}
		
		CrosswalkEditorSchemaDTO dto = new CrosswalkEditorSchemaDTO();
		dto.setMetadata(metadata);
		dto.setContent(mapper.valueToTree(resultMapOrList));
		
		
		return dto;
	}
	
	private FunctionDTO createSimpleFilterFunction(String name, String operator) {
		return new FunctionDTO(
				name, operator, "", 
				List.of(
						new ParameterDTO("input", "object", true) 
						), 
				List.of(
						new OutputDTO("output", "object", true)
						)
				);
		
	}

	public List<FunctionDTO> getFilters() {
		List<FunctionDTO> list = new ArrayList<FunctionDTO>();
		list.add(createSimpleFilterFunction("equals", "="));
		list.add(createSimpleFilterFunction("not equals", "!="));
		list.add(createSimpleFilterFunction("in", "in"));
		list.add(createSimpleFilterFunction("startsWith", "startsWith"));
		list.add(createSimpleFilterFunction("isURI", "isURI"));
		list.add(createSimpleFilterFunction("contains", "contains"));
		list.add(createSimpleFilterFunction("Does not contain", "!contains"));
		
		
		return list;
	}
}
