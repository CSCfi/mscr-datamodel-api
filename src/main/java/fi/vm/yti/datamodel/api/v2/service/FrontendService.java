package fi.vm.yti.datamodel.api.v2.service;

import static fi.vm.yti.datamodel.api.v2.dto.ModelConstants.DEFAULT_LANGUAGE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import org.apache.jena.rdf.model.Model;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Service;

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
    
    public List<OrganizationDTO> getOrganizationsByID(Set<UUID> ids) {
    	var model = coreRepository.getOrganizationsByIds(ids);
    	var dtos = OrganizationMapper.mapToListOrganizationDTO(model);
    	return dtos;
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

    
    private void handleNode(Map<String, Object> node, Map subTree, Queue<Map> subTrees, Queue<Map<String, Object>> queue ) {		
		Map<String, Object> props = (Map<String, Object>)node.get("properties");
		if(props != null) {
			Iterator<String> i = props.keySet().iterator();
			while(i.hasNext()) {				
				String prop = i.next();
				Map map = new HashMap();
				subTrees.add(map);
				subTree.put(prop, map);			
				Map<String, Object> obj = (Map<String, Object>) props.get(prop);
				queue.add(obj);				
			}			
		}		    
    }
    
	private Map<String, Object> createTree(Map<String, Object> resultMapOrList) {
		Map<String, Object> tree = new HashMap<String, Object>();	
		Queue<Map<String, Object>> queue = new LinkedList<Map<String, Object>>();
		Queue<Map> subTrees = new LinkedList<Map>();
		subTrees.add(tree);
		queue.add(resultMapOrList);
		while(!queue.isEmpty()) {			
			Map<String, Object> node = queue.poll();
			Map subTree = subTrees.poll();
			if(node != null) {
				String nodeType = node.containsKey("type") ? node.get("type").toString() : "string";
				if(nodeType.equals("object")) {
					handleNode(node, subTree, subTrees, queue);
				}
				
				else if(nodeType.equals("array")) {
					Map<String, Object> node2 = ((Map<String, Object>)node.get("items"));
					String nodeType2 = node2.containsKey("type") ? node2.get("type").toString() : "string";
					if(nodeType2.equals("object")) {
						handleNode(node2, subTree, subTrees, queue);
					}					
				}
			}
		}
		return tree;
	}
	
	public CrosswalkEditorSchemaDTO getSchema(String contentString, SchemaInfoDTO metadata) throws Exception {
		ObjectMapper mapper = new ObjectMapper(); // turn into a bean
		$RefParser parser = new $RefParser(contentString);
			
		$Refs refs = parser.parse().dereference().mergeAllOf().getRefs();
		Map<String, Object> resultMapOrList = refs.schema();
		 
		CrosswalkEditorSchemaDTO dto = new CrosswalkEditorSchemaDTO();
		dto.setMetadata(metadata);
		
		Map<String, Object> tree = createTree(resultMapOrList);
		
		resultMapOrList.put("tree", tree);
		
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
