package fi.vm.yti.datamodel.api.v2.service;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;


@ExtendWith(SpringExtension.class)
@Import({
	JsonSchemaWriter.class,
	

})
public class JsonSchemaWriterTest {

	@Autowired
	private JsonSchemaWriter service;
	
	@Test
	public void testSimpleNested() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/simple_nested.ttl");		
		String json = service.newModelSchema("urn:IAMNOTAPID:1e20ca42-66ce-4233-a3ee-b8f61f6e5571", model, "en");
		System.out.println(json);
		// TODO: Add assertons
	}	
	
	
	@Test
	public void testTTV_CRIS() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/ttv_cris.ttl");		
		String json = service.newModelSchema("urn:IAMNOTAPID:4b28461f-396d-485a-8cac-31ca6d091c00", model, "en");
		// TODO: Add assertions
	}
	
}
