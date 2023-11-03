package fi.vm.yti.datamodel.api.v2.transformation.researchfi;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.service.DataTransformationService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.impl.FnoDataTransformationServiceImpl;

@ExtendWith(SpringExtension.class)
@Import({
	JenaService.class,		
	CoreRepository.class,
	FnoDataTransformationServiceImpl.class
})
public class Researchfi2CERIF2Test {

	@Autowired
	private DataTransformationService service;
	
	@Test
	public void testAffiliationMapping() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/researchfi/cerif2_affiliation_sample.xml");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/researchfi/cerif2_affiliation-mapping1.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "xml", mappings, "xml");		
		System.out.println(output);		
	}
}
