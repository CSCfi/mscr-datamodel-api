package fi.vm.yti.datamodel.api.v2.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;


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
		model.read("models/mscr/jsonschema/simple_nested.ttl");		
		String json = service.newModelSchema("urn:IAMNOTAPID:1e20ca42-66ce-4233-a3ee-b8f61f6e5571", model, "en");
		// TODO: Add assertons
	}	
	
	
	@Test
	public void testTTV_CRIS() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/jsonschema/ttv_cris.ttl");		
		String json = service.newModelSchema("urn:IAMNOTAPID:4b28461f-396d-485a-8cac-31ca6d091c00", model, "en");
		// TODO: Add assertions
		
	}
	
	
	@Test
	public void tesCSDToolTasks() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/skos/CSDToolTasks.ttl");		
		String json = service.skosSchema("urn:IAMNOTAPID:de6b13b7-ff43-47cc-9df9-a69476796c97", model, "en");
		// TODO: Add assertions
	}	
	
	
	@Test
	public void tesCSDToolTasksSmall() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/skos/CSDToolTasks-small.ttl");		
		String json = service.skosSchema("urn:IAMNOTAPID:a0a4c59a-4c57-4e44-a633-276557692372", model, "en");
		// TODO: Add assertions
	}	
	
	@Test
	public void testOKM() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/skos/okm-tieteenala.ttl");		
		String json = service.skosSchema("urn:IAMNOTAPID:3cbbd7ed-11f7-401d-bad0-69114fbd2c69", model, "en");
		// TODO: Add assertions
	}
	
	@Test
	public void testSimpleNoTopConcepts() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/skos/simple-no-topconcepts.ttl");		
		String json = service.newModelSchema("urn:IAMNOTAPID:41ce9689-b3b2-440d-ad62-633dc734a88d", model, "en");
		// TODO: Add assertions
	}
	
	@Test
	public void testCERIF2() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("rdfs/cerif2/core.ttl");		
		String json = service.rdfs("urn:IAMNOTAPID:cerif2", model, "en");		
		System.out.println(json);
		//FileUtils.write(new File("RDFS-test.json"), json);
		ObjectMapper mapper = new ObjectMapper();
		JsonNode j = mapper.readTree(json);
		
		// check that each class can be found in the root object
		assertEquals(
				model.listSubjectsWithProperty(RDF.type, RDFS.Class).toList().size(), 
				((JsonNode)j.get("properties")).size()
		);
		
	}

	

}
