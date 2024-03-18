package fi.vm.yti.datamodel.api.v2.service;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.topbraid.shacl.vocabulary.SH;

import com.fasterxml.jackson.databind.JsonNode;

import fi.vm.yti.datamodel.api.v2.mapper.ClassMapper;
import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.JSONSchemaMapper;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.XSDMapper;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.service.dtr.DTRClient;
@ExtendWith(SpringExtension.class)
@Import({
	SchemaService.class,
	ClassMapper.class,
	ResourceMapper.class,
	CoreRepository.class,
	XSDMapper.class,
	JSONSchemaMapper.class,
	DTRClient.class
})
@TestPropertySource(properties = {
	    "dtr.typeAPIEndpoint=https://typeapi.lab.pidconsortium.net/v1/types/schema/",
	})
public class SchemaServiceTest {

	@Autowired
	private SchemaService service;
	
	
	private JsonNode getJsonNodeFromPath(String schemaPath) throws Exception, IOException {
		InputStream inputSchemaInputStream = getClass().getClassLoader().getResourceAsStream(schemaPath);
		byte[] inputSchemaInByte = inputSchemaInputStream.readAllBytes();
		inputSchemaInputStream.close();
		return service.parseSchema(new String(inputSchemaInByte));		
	}	
	
	@Test
	void testSimple1Transformation() throws Exception {
		JsonNode data = getJsonNodeFromPath("jsonschema/test_jsonschema_valid_simple1.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		
		Resource root = model.createResource(schemaPID + "#root/Root");
		
		model.write(System.out, "TURTLE");
		
		assertTrue(model.contains(root, RDF.type, SH.NodeShape));		
		assertEquals(3, model.listSubjectsWithProperty(RDF.type, SH.PropertyShape).toList().size());
		assertTrue(model.contains(root, SH.closed, model.createTypedLiteral(true)));

	}
	
	@Test
	void testSimpleNested() throws Exception {
		JsonNode data = getJsonNodeFromPath("jsonschema/test_jsonschema_valid_simple_nested.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		
		Resource root = model.createResource(schemaPID + "#root/Root");
		model.write(System.out, "TTL");
		assertTrue(model.contains(root, RDF.type, SH.NodeShape));		
		assertEquals(3, model.listSubjectsWithProperty(RDF.type, SH.NodeShape).toList().size());
		assertEquals(7, model.listSubjectsWithProperty(RDF.type, SH.PropertyShape).toList().size());
		
		assertEquals(SH.NodeShape, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/address/Address"), RDF.type).getObject());		
		Resource addressProperty = model.getResource(schemaPID+"#root/Root/address");
		assertEquals(ResourceFactory.createResource(schemaPID + "#root/Root/address/Address"), addressProperty.getProperty(SH.node).getObject().asResource());
		Resource cityProperty = model.getResource(schemaPID+"#root/Root/address/Address/city");
		assertEquals(ResourceFactory.createResource(schemaPID + "#root/Root/address/Address/city/City"), cityProperty.getProperty(SH.node).getObject().asResource());

	}
	
	@Test
	void testClosed() throws Exception {
		JsonNode data = getJsonNodeFromPath("jsonschema/test_jsonschema_valid_simple_nested.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		
		Resource root = model.createResource(schemaPID + "#root/Root");
		
		assertTrue(model.contains(root, SH.closed, model.createTypedLiteral(true)));
	}
	
	@Test
	void testValidDatatypes() throws Exception {
		JsonNode data = getJsonNodeFromPath("jsonschema/test_jsonschema_valid_simple_datatypes.json");
		assertNotNull(data);
				
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		
		model.write(System.out, "TURTLE");
		
		assertEquals(XSD.integer, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/address/Address/house_number"), SH.datatype).getObject());
		assertEquals(XSD.integer, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/address/Address/city/City/population"), SH.datatype).getObject());
		assertEquals(XSD.xfloat, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/height"), SH.datatype).getObject());
		assertEquals(XSD.xboolean, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/has_cats"), SH.datatype).getObject());
		assertEquals("common", model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/lastName"), SH.defaultValue).getString());
		assertEquals("test", model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/address"), SH.defaultValue).getString());
	}
	
	@Test
	void testValidRequired() throws Exception {
		JsonNode data = getJsonNodeFromPath("jsonschema/test_jsonschema_valid_required.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		assertEquals(1, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/lastName"), SH.minCount).getInt());
		assertEquals(1, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/lastName"), SH.maxCount).getInt());
		
	}
	
	@Test
	void testValidArrays() throws Exception {
		JsonNode data = getJsonNodeFromPath("jsonschema/test_jsonschema_valid_arrays.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		model.write(System.out, "TURTLE");

		assertEquals(XSD.xstring, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/firstName"), SH.datatype).getObject());
		// lastName is functional property -> must have maxCount = 1
		assertEquals(1, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/firstName"), SH.maxCount).getInt());
		// lastName is not required -> should not have minCount
		assertFalse(model.contains(model.createResource(schemaPID + "#root/Root/firstName"), SH.minCount));

		// not restrictions on number of items in an array -> no maxCount
		assertFalse(model.contains(model.createResource(schemaPID + "#root/Root/lastNames"), SH.maxCount));
		

		assertEquals(2, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/addresses/Addresses/numbers"), SH.minCount).getInt());
		assertFalse(model.contains(model.createResource(schemaPID + "#root/Root/addresses/Addresses/numbers"), SH.maxCount));

		assertEquals(10, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/addresses/Addresses/city/City/area_codes"), SH.maxCount).getInt());
		assertEquals(1, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/addresses/Addresses/city/City/area_codes"), SH.minCount).getInt());
	}
	
	@Test
	void testNumberRestrictions() throws Exception {
		JsonNode data = getJsonNodeFromPath("jsonschema/test_jsonschema_valid_number_restrictions.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		model.write(System.out, "TURTLE");

		assertEquals(10, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/minNumber"), SH.minInclusive).getInt());		
		assertEquals(100, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/maxNumber"), SH.maxInclusive).getInt());
		assertEquals(10.2, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/numberRange"), SH.minInclusive).getDouble());
		assertEquals(100.1, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/numberRange"), SH.maxInclusive).getDouble());

		assertEquals(10, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/minNumberEx"), SH.minExclusive).getInt());
		assertEquals(100, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/maxNumberEx"), SH.maxExclusive).getInt());
		assertEquals(10, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/numberRangeEx"), SH.minExclusive).getInt());
		assertEquals(100, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/numberRangeEx"), SH.maxExclusive).getInt());

	}
	
	@Test
	void testStrings() throws Exception {
		JsonNode data = getJsonNodeFromPath("jsonschema/test_jsonschema_valid_strings.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
				
		assertEquals(10, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/minString"), SH.minLength).getInt());
		assertEquals(100, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/maxString"), SH.maxLength).getInt());

		assertEquals(10, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/stringLengthRange"), SH.minLength).getInt());
		assertEquals(100, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/stringLengthRange"), SH.maxLength).getInt());

		assertEquals("^(\\([0-9]{3}\\))?[0-9]{3}-[0-9]{4}$", model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/stringPattern"), SH.pattern).getString());
	}
	
	@Test
	void testEnums() throws Exception {
		JsonNode data = getJsonNodeFromPath("jsonschema/test_jsonschema_valid_enums.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		
		// empty enum -> do not generate the property node at all?
		//assertFalse(model.contains(model.createResource(schemaPID + "#root/empty"), RDF.type));
		
		/*
		Bag b = model.createBag();
		b.add("one");
		b.add("two");
		model.add(model.createResource(schemaPID + "#root/string"), SH.in, b);
		*/
//		model.write(System.out, "TURTLE");
		
		assertTrue(model.contains(model.createResource(schemaPID + "#root/Root/string"), SH.in));		
		Bag strings = model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/string"), SH.in).getBag();
		assertEquals(2, strings.size());

		assertTrue(model.contains(model.createResource(schemaPID + "#root/Root/defaultwithouttype"), SH.in));		
		Bag defaultwithouttype = model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/defaultwithouttype"), SH.in).getBag();
		assertEquals(2, defaultwithouttype.size());
		
		List<String> stringList = new ArrayList<String>();
		Iterator<RDFNode> i = strings.iterator();
		while (i.hasNext()) {
			stringList.add(i.next().asLiteral().getString());
		}
		assertArrayEquals(new String[] {"one","two"}, stringList.toArray());
		/*
		Bag b2 = model.createBag();
		b2.add(1);
		b2.add(2);
		b2.add(3);
		model.add(model.createResource(schemaPID + "#root/integer"), SH.in, b2);
		*/

		assertTrue(model.contains(model.createResource(schemaPID + "#root/Root/integer"), SH.in));
		Bag integers = model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/integer"), SH.in).getBag();
		assertEquals(3, integers.size());
		
		
		List<Integer> integerList = new ArrayList<Integer>();
		Iterator<RDFNode> i2 = integers.iterator();
		while (i2.hasNext()) {
			integerList.add(i2.next().asLiteral().getInt());
		}
		assertArrayEquals(new Integer[] {1,2,3}, integerList.toArray());
	}
	
	@Test
	void testDefaultName() throws Exception {
		JsonNode data = getJsonNodeFromPath("jsonschema/test_jsonschema_valid_simple1.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		
		assertEquals("lastName", model.getProperty(schemaPID + "#root/Root/lastName").getLocalName());
		model.write(System.out, "TURTLE");

		
	}
	
	/* 
	 * 
	 *  
	@Test
	void testNot() throws Exception {
		byte[] data = getByteStreamFromPath("jsonschema/test_jsonschema_valid_not.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);		
	}
	*/
	
	@Test
	void testTTV_CRIS() throws Exception {
		JsonNode data = getJsonNodeFromPath("jsonschema/test_jsonschema_ttv_cris_minimal_custom.json");
		
		ValidationRecord validationRecord = JSONValidationService.validateJSONSchema(data);
		assertTrue(validationRecord.isValid());
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		//model.write(System.out, "TTL");
	
	}
	
	//
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
	
	@Test
	void testSampleXMLSchema() throws Exception {
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		String filePath = "src/test/resources/xmlschema/sample.xsd";
		Model m = service.transformXSDToInternal(schemaPID, filePath);
		
		assertEquals(19, m.listSubjectsWithProperty(RDF.type, SH.PropertyShape).toList().size()); // just element instances, no attributes
		assertEquals(6, m.listSubjectsWithProperty(RDF.type, SH.NodeShape).toList().size()); // 5 + root
		
		
		
	}
	
	@Test
	void testDTR1() throws Exception {
		JsonNode data = getJsonNodeFromPath("jsonschema/dtr/db605a11c81e79e1efc4.json");
		
		ValidationRecord validationRecord = JSONValidationService.validateJSONSchema(data);
		assertTrue(validationRecord.isValid());
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);		
		
		//model.write(System.out, "TTL");
		
	}
	
	@Test
	void testFetchAndMapDTRType() throws Exception {
		Model m = service.fetchAndMapDTRType("21.11104/e944e035caf3ec24192c");
		m.write(System.out,  "TURTLE");
	}
}

