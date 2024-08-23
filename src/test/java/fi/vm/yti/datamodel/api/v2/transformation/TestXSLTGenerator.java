package fi.vm.yti.datamodel.api.v2.transformation;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.Test;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.dto.MappingInfoDTO;
import fi.vm.yti.datamodel.api.v2.mapper.MappingMapper;

class TestXSLTGenerator {

	@Test
	void testGenerate() throws Exception {
		
		MappingMapper mappingMapper = new MappingMapper();
		Model model = RDFDataMgr.loadModel("mappings/simple-mappings-only.ttl");
		
		String pid = "mscr:crosswalk:f52f0312-a214-4cc7-afea-2e0eb0e06c77";
		List<MappingInfoDTO> mappings = new ArrayList<MappingInfoDTO>();
		NodeIterator i = model.listObjectsOfProperty(model.getResource(pid), MSCR.mappings);
		while(i.hasNext()) {
			Resource mappingResource = i.next().asResource();			
			MappingInfoDTO dto = mappingMapper.mapToMappingDTO(
					mappingResource.getURI(), 
					model);
			mappings.add(dto);
		}		
		
		XSLTGenerator generator = new XSLTGenerator();
		
		String r = generator.generate(mappings);
		System.out.println(r);
	}

}
