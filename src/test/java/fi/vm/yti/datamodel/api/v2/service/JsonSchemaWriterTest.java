package fi.vm.yti.datamodel.api.v2.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.BeforeEach;
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
		String json = service.newModelSchema("urn:IAMNOTAPID:1e20ca42-66ce-4233-a3ee-b8f61f6e5571", model, "en", SchemaFormat.JSONSCHEMA);
		// TODO: Add assertons		
	}	
	
	
	@Test
	public void testTTV_CRIS() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/jsonschema/ttv_cris.ttl");		
		String json = service.newModelSchema("urn:IAMNOTAPID:4b28461f-396d-485a-8cac-31ca6d091c00", model, "en", SchemaFormat.SHACL);
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
		String json = service.newModelSchema("urn:IAMNOTAPID:41ce9689-b3b2-440d-ad62-633dc734a88d", model, "en", SchemaFormat.SKOSRDF);
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

	
	@Test
	public void testSchemaWithDTRDatatype() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/mscr_schema_dtr_datatype.ttl");		
		String json = service.newModelSchema("mscr:schema:a4b6d497-aa7e-41c2-aa81-79152d243052", model, "en", SchemaFormat.JSONSCHEMA);
		System.out.println(json);
		DocumentContext doc = JsonPath.parse(json);
		Object r = doc.read("$.definitions['mscr:root/Root/integrationType'].type");
		assertEquals("string", r);

	}
	
	@Test
	public void testOpenaire40() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/xsd/openaire-4.0.ttl");		
		String json = service.newModelSchema("pid:test", model, "en", SchemaFormat.XSD);
		System.out.println(json);
		
	}

	
	@Test
	public void testLinguisticTrip() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("models/mscr/xsd/fieldtrip.ttl");		
		String json = service.newModelSchema("mscr:schema:afd463ba-386b-4468-a786-c17e0edf99e5", model, "en",SchemaFormat.XSD);
		//System.out.println(json);
		FileUtils.write(new File("ui-schema.json"), json);
		
	}
	
	@Test
	public void testOpenAlexToInternal() throws Exception {
		Model model = ModelFactory.createDefaultModel();
		model.read("shacl/openalex-ontology.ttl");	
		String json = service.shacl("test", model, null);
		FileUtils.write(new File("openalex-schema.json"), json);
	}
}
