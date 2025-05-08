package fi.vm.yti.datamodel.api.v2.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;


@ExtendWith(SpringExtension.class)
@Import({
	JsonSchemaWriter.class,	

})
public class JsonSchemaWriterTest {

	@Autowired
	private JsonSchemaWriter service;
	
    @MockBean
    JenaService jenaService;
    
    ObjectMapper om = new ObjectMapper();
    
	@BeforeEach
    void init () {
		
		Model dtrDatatypeModel = ModelFactory.createDefaultModel();	
		dtrDatatypeModel.add(dtrDatatypeModel.createResource("https://hdl.handle.net/21.11104/3626040cadcac1571685"), dtrDatatypeModel.createProperty("mscr:jsonschema:type"), "string");
		when(jenaService.getSchema(any(String.class))).thenReturn(dtrDatatypeModel);
    }
	
	@Test
	public void testSimpleNested() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/jsonschema/simple_nested.ttl");		
		String json = service.newModelSchema("mscr:schema:a47357ec-e3ee-419c-a709-2490562e8d62", model, "en", SchemaFormat.JSONSCHEMA);
		assertNotNull(json);
	}	
	
	
	@Test
	public void testTTV_CRIS() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/jsonschema/ttv_cris.ttl");		
		String json = service.newModelSchema("mscr:schema:7f2b7d96-48e7-43b2-a4a0-48f098e1911d", model, "en", SchemaFormat.JSONSCHEMA);
		assertNotNull(json);
		
	}
	
	@Test
	public void tesCSDToolTasksCustomRootNode() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/skos/CSDToolTasks_customroot.ttl");		
		String json = service.skosSchema("urn:IAMNOTAPID:de6b13b7-ff43-47cc-9df9-a69476796c97", model, "en");
		assertNotNull(json);
		assertEquals(58, om.readTree(json).get("definitions").size());
	}	

	
	@Test
	public void tesCSDToolTasks() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/skos/CSDToolTasks.ttl");		
		String json = service.skosSchema("urn:IAMNOTAPID:de6b13b7-ff43-47cc-9df9-a69476796c97", model, "en");
		assertNotNull(json);
		assertEquals(135, om.readTree(json).get("definitions").size());
	}	
	
	@Test
	public void testGCMD() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/skos/gcmd.ttl");		
		String json = service.skosSchema("mscr:schema:f8eb9580-c6a0-4088-bd2c-892563d4ce69", model, "en");
		assertNotNull(json);
		assertEquals(2870, om.readTree(json).get("definitions").size());
	}	
	
	
	
	@Test
	public void tesCSDToolTasksSmall() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/skos/CSDToolTasks-small.ttl");		
		String json = service.skosSchema("urn:IAMNOTAPID:a0a4c59a-4c57-4e44-a633-276557692372", model, "en");
		assertNotNull(json);
		assertEquals(2, om.readTree(json).get("definitions").size());
	}	
	
	@Test
	public void testOKM() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/skos/okm-tieteenala.ttl");		
		String json = service.skosSchema("urn:IAMNOTAPID:3cbbd7ed-11f7-401d-bad0-69114fbd2c69", model, "en");
		assertNotNull(json);
		assertEquals(75, om.readTree(json).get("definitions").size());
	}
	
	@Test
	public void testSimpleNoTopConcepts() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/skos/simple-no-topconcepts.ttl");		
		String json = service.skosSchema("urn:IAMNOTAPID:41ce9689-b3b2-440d-ad62-633dc734a88d", model, "en");
		assertNotNull(json);
		assertEquals(5, om.readTree(json).get("definitions").size());
	}
	
	@Test
	public void testCERIF2() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("rdfs/cerif2/core.ttl");		
		String json = service.rdfs("urn:IAMNOTAPID:cerif2", model, "en");		
		//FileUtils.write(new File("RDFS-test.json"), json);
		ObjectMapper mapper = new ObjectMapper();
		JsonNode j = mapper.readTree(json);
		
		// check that each class can be found in the root object
		assertEquals(
				model.listSubjectsWithProperty(RDF.type, RDFS.Class).toList().size(), 
				((JsonNode)j.get("properties")).size()
		);
		
	}

	
	@Test
	@Disabled
	public void testSchemaWithDTRDatatype() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/mscr_schema_dtr_datatype.ttl");		
		String json = service.newModelSchema("mscr:schema:a4b6d497-aa7e-41c2-aa81-79152d243052", model, "en", SchemaFormat.JSONSCHEMA);
		DocumentContext doc = JsonPath.parse(json);
		Object r = doc.read("$.definitions['mscr:schema:a4b6d497-aa7e-41c2-aa81-79152d243052#root-Root-integrationType'].type");
		assertEquals("string", r);

	}
	
	@Test
	public void testOpenaire40() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/xsd/openaire-4.0.ttl");		
		String json = service.newModelSchema("mscr:schema:43d12c8c-80cd-4790-ab7a-c4979e8f9fb1", model, "en", SchemaFormat.XSD);
		assertNotNull(json);
		
	}

	
	@Test
	public void testLinguisticTrip() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/xsd/fieldtrip.ttl");		
		String json = service.newModelSchema("mscr:schema:cac34cb0-ebf4-49db-8616-b11b79371748", model, "en",SchemaFormat.XSD);
		assertNotNull(json);
	}
	
	@Test
	public void testOpenAlexToInternal() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("shacl/openalex-ontology.ttl");	
		String json = service.shacl("test", model, null);
		assertNotNull(json);
	}
}
