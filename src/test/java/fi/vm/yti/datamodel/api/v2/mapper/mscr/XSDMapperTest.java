package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fi.vm.yti.datamodel.api.v2.dto.SchemaParserResultDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaPart;
import fi.vm.yti.datamodel.api.v2.mapper.ClassMapper;
import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.service.SchemaService;
import fi.vm.yti.datamodel.api.v2.service.dtr.DTRClient;

//import es.weso.xmlschema2shex.parser.XMLSchema2ShexParser;

@ExtendWith(SpringExtension.class)
@Import({
	SchemaService.class,
	XSDMapper.class,
	ClassMapper.class,
	ResourceMapper.class,
	CoreRepository.class,
	XSDMapper.class,
	JSONSchemaMapper.class,
	DTRClient.class
})
@Disabled
public class XSDMapperTest {

	@Autowired
	private SchemaService service;
	
	@Autowired
	private XSDMapper mapper;
	
	ObjectMapper m = new ObjectMapper();
	
	private String getStringFromPath(String schemaPath) throws Exception, IOException {
		InputStream inputSchemaInputStream = getClass().getClassLoader().getResourceAsStream(schemaPath);
		byte[] inputSchemaInByte = inputSchemaInputStream.readAllBytes();
		inputSchemaInputStream.close();

		return new String(inputSchemaInByte);
	}
	
	private JsonNode getJsonNodeFromPath(String schemaPath) throws Exception, IOException {
		InputStream inputSchemaInputStream = getClass().getClassLoader().getResourceAsStream(schemaPath);
		byte[] inputSchemaInByte = inputSchemaInputStream.readAllBytes();
		inputSchemaInputStream.close();
		return service.parseSchema(new String(inputSchemaInByte));		
	}

	
	
	@Test
	void testTraverseTree() throws Exception {
		//String filePath = "src/test/resources/xmlschema/sample.xsd";
		//String filePath = "src/test/resources/xmlschema/eml1/eml.xsd";
		//String filePath = "src/test/resources/xmlschema/clarin/LinguisticFieldtrip.xsd";
		//String filePath = "https://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/1.x/profiles/clarin.eu:cr1:p_1407745712081/xsd";
		//String filePath = "src/test/resources/xmlschema/datacite/4.4/metadata.xsd";
		//String filePath = "https://schema.datacite.org/meta/kernel-4.4/metadata.xsd";
		//String filePath = "https://raw.githubusercontent.com/jkesanie/eml-profile/master/eml.xsd";
        //String filePath = "src/test/resources/xmlschema/dublincore/simpledc20021212.xsd";
		//String filePath = "src/test/resources/xmlschema/dublincore/dcterms.xsd";
		//String filePath = "https://schema.datacite.org/meta/kernel-3.1/metadata.xsd";
		//String filePath = "src/test/resources/xmlschema/eudat-core/eudat-core.xsd";
		//String filePath = "https://raw.githubusercontent.com/OpenEdition/tei.openedition/master/xsd/tei.openedition.1.6.3/document.xsd";
		
		String filePath = "src/test/resources/xmlschema/math/plain.xsd";
		ObjectNode jroot = mapper.mapToInternalJson(filePath);
		String r = m.writeValueAsString(jroot);
		assertNotNull(r);
	}
	
	@Test
	void testTraverseTree2() throws Exception {
		String filePath = "https://raw.githubusercontent.com/OpenEdition/tei.openedition/master/xsd/tei.openedition.1.6.3/document.xsd";
		ObjectNode jroot = mapper.mapToInternalJson(filePath);
		String r = m.writeValueAsString(jroot);
		assertNotNull(r);
	}	
	
	@Test
	void testTraverseTree3() throws Exception {
		String filePath = "src/test/resources/xmlschema/eudat-core/eudat-core.xsd";
		ObjectNode jroot = mapper.mapToInternalJson(filePath);
		String r = m.writeValueAsString(jroot);
		assertNotNull(r);
	}	
	@Test
	void testTraverseTree4() throws Exception {
		String filePath = "https://schema.datacite.org/meta/kernel-3.1/metadata.xsd";
		ObjectNode jroot = mapper.mapToInternalJson(filePath);
		String r = m.writeValueAsString(jroot);
		assertNotNull(r);
	}	
	@Test
	void testTraverseTree5() throws Exception {
		String filePath = "src/test/resources/xmlschema/dublincore/dcterms.xsd";
		ObjectNode jroot = mapper.mapToInternalJson(filePath);
		String r = m.writeValueAsString(jroot);
		assertNotNull(r);
	}	
	@Test
	void testTraverseTree6() throws Exception {
        String filePath = "src/test/resources/xmlschema/dublincore/simpledc20021212.xsd";
		ObjectNode jroot = mapper.mapToInternalJson(filePath);
		String r = m.writeValueAsString(jroot);
		assertNotNull(r);
	}	
	@Test
	void testTraverseTree7() throws Exception {
		String filePath = "src/test/resources/xmlschema/sample.xsd";
		ObjectNode jroot = mapper.mapToInternalJson(filePath);
		String r = m.writeValueAsString(jroot);
		assertNotNull(r);
	}	
	@Test
	void testTraverseTree8() throws Exception {
		String filePath = "src/test/resources/xmlschema/clarin/LinguisticFieldtrip.xsd";
		ObjectNode jroot = mapper.mapToInternalJson(filePath);
		String r = m.writeValueAsString(jroot);
		assertNotNull(r);
	}	
	@Test
	void testTraverseTree9() throws Exception {
		String filePath = "https://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/1.x/profiles/clarin.eu:cr1:p_1407745712081/xsd";
		ObjectNode jroot = mapper.mapToInternalJson(filePath);
		String r = m.writeValueAsString(jroot);
		assertNotNull(r);
	}	

	@Test
	@Disabled
	void testLoadImportDatacite_4_4() throws Exception {		
		//String filePath = "https://schema.datacite.org/meta/kernel-3.1/metadata.xsd";
		String filePath = "src/test/resources/xmlschema/datacite/4.4/metadata.xsd";		
		SchemaParserResultDTO r = mapper.loadSchema(filePath);
		assertTrue(r.isOk());
		assertEquals(11,  r.getTree().getHasPart().size());
		assertEquals("include/xml.xsd", r.getTree().getHasPart().get(0).getPath());
		assertEquals("include/datacite-numberType-v4.xsd", r.getTree().getHasPart().get(10).getPath());
	}
	
	@Test
	@Disabled
	void testLoadImportEml() throws Exception {		
		String filePath = "https://raw.githubusercontent.com/gbif/eml-profile/refs/heads/master/eml.xsd";
		SchemaParserResultDTO r = mapper.loadSchema(filePath);		
		assertTrue(r.isOk());
		assertEquals(1,  r.getTree().getHasPart().size());
		assertEquals("eml-gbif-profile.xsd", r.getTree().getHasPart().get(1).getPath());
		assertEquals("http://www.w3.org/2001/xml.xsd", r.getTree().getHasPart().get(0).getPath());
		
		SchemaPart p = r.getTree().getHasPart().get(1);
		assertEquals(3, p.getHasPart().size());
		assertEquals("eml.xsd", p.getHasPart().get(0).getPath());
		assertEquals("dc.xsd", p.getHasPart().get(1).getPath());
		assertEquals("http://rs.gbif.org/schema/xml.xsd", p.getHasPart().get(2).getPath());
		assertEquals(0, p.getHasPart().get(0).getHasPart().size()); // cycle
		assertEquals(0, p.getHasPart().get(1).getHasPart().size());
		assertEquals(0, p.getHasPart().get(1).getHasPart().size());
		

	}	
	
	@Test
	@Disabled
	void testImportOpenaireToInternalJSON() throws Exception {
		String url = "https://raw.githubusercontent.com/openaire/guidelines-literature-repositories/master/schemas/4.0/openaire.xsd";
		ObjectNode obj = mapper.mapToInternalJson(url);
		
		ObjectMapper m = new ObjectMapper();
		System.out.println(m.writeValueAsString(obj));

		ObjectNode r = (ObjectNode) obj.at("/properties/format");
		assertEquals("http://purl.org/dc/elements/1.1/", r.get("namespace").asText());
		
		ObjectNode r2 = (ObjectNode) obj.at("/properties/identifier");
		assertEquals("http://datacite.org/schema/kernel-4", r2.get("namespace").asText());
	}
	
	@Test
	void testCircularReferences1() throws Exception {

		ObjectNode obj = mapper.mapToInternalJson("src/test/resources/xmlschema/circular-references1.xsd");
		
		ObjectMapper m = new ObjectMapper();
		String r = m.writeValueAsString(obj);
		assertNotNull(r);
		
	}

	
	
}
