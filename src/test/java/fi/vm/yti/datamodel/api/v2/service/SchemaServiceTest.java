package fi.vm.yti.datamodel.api.v2.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.UUID;

import org.apache.jena.rdf.model.Model;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import fi.vm.yti.datamodel.api.v2.mapper.ClassMapper;
import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;

@ExtendWith(SpringExtension.class)
@Import({
	SchemaService.class,
	ClassMapper.class,
	ResourceMapper.class,
	CoreRepository.class
})
public class SchemaServiceTest {

	
	@Autowired
	private SchemaService service;
	
	
	@Test
	void testTransforJSONSchemaToInternal() throws Exception, IOException {
		var inputStream = getClass().getResourceAsStream("/test_jsonschema_b2share.json");
		assertNotNull(inputStream);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, inputStream.readAllBytes());
		
		model.write(System.out, "TURTLE");
		
	}
}

