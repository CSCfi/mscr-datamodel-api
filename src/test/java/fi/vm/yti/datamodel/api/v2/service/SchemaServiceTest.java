package fi.vm.yti.datamodel.api.v2.service;

import static org.assertj.core.api.Assertions.assertThatIterator;
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
import org.apache.jena.shacl.vocabulary.SHACL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.topbraid.shacl.vocabulary.SH;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
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
	
	
	private byte[] getByteStreamFromPath(String schemaPath) throws Exception, IOException {
		InputStream inputSchemaInputStream = getClass().getClassLoader().getResourceAsStream(schemaPath);
		byte[] inputSchemaInByte = inputSchemaInputStream.readAllBytes();
		inputSchemaInputStream.close();

		return inputSchemaInByte;
	}	
	
	@Test
	void testSimple1Transformation() throws Exception {
		byte[] data = getByteStreamFromPath("jsonschema/test_jsonschema_valid_simple1.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		
		Resource root = model.createResource(schemaPID + "#root/Root");
		
//		model.write(System.out, "TURTLE");
		
		assertTrue(model.contains(root, RDF.type, SH.NodeShape));		
		assertEquals(3, model.listSubjectsWithProperty(RDF.type, SH.PropertyShape).toList().size());
		assertTrue(model.contains(root, SH.closed, model.createTypedLiteral(true)));

	}
	
	@Test
	void testSimpleNested() throws Exception {
		byte[] data = getByteStreamFromPath("jsonschema/test_jsonschema_valid_simple_nested.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		
		Resource root = model.createResource(schemaPID + "#root/Root");
		
		assertTrue(model.contains(root, RDF.type, SH.NodeShape));		
		assertEquals(3, model.listSubjectsWithProperty(RDF.type, SH.NodeShape).toList().size());
		assertEquals(7, model.listSubjectsWithProperty(RDF.type, SH.PropertyShape).toList().size());
		
		assertEquals(SH.NodeShape, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/address/Address"), RDF.type).getObject());
	}
	
	@Test
	void testClosed() throws Exception {
		byte[] data = getByteStreamFromPath("jsonschema/test_jsonschema_valid_simple_nested.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		
		Resource root = model.createResource(schemaPID + "#root/Root");
		
		assertTrue(model.contains(root, SH.closed, model.createTypedLiteral(true)));
	}
	
	@Test
	void testValidDatatypes() throws Exception {
		byte[] data = getByteStreamFromPath("jsonschema/test_jsonschema_valid_simple_datatypes.json");
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
		byte[] data = getByteStreamFromPath("jsonschema/test_jsonschema_valid_required.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
		assertEquals(1, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/lastName"), SH.minCount).getInt());
		assertEquals(1, model.getRequiredProperty(model.createResource(schemaPID + "#root/Root/lastName"), SH.maxCount).getInt());
		
	}
	
	@Test
	void testValidArrays() throws Exception {
		byte[] data = getByteStreamFromPath("jsonschema/test_jsonschema_valid_arrays.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
//		model.write(System.out, "TURTLE");

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
		byte[] data = getByteStreamFromPath("jsonschema/test_jsonschema_valid_number_restrictions.json");
		assertNotNull(data);
		
		String schemaPID = "urn:test:" + UUID.randomUUID().toString();
		Model model = service.transformJSONSchemaToInternal(schemaPID, data);
//		model.write(System.out, "TURTLE");

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
		byte[] data = getByteStreamFromPath("jsonschema/test_jsonschema_valid_strings.json");
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
		byte[] data = getByteStreamFromPath("jsonschema/test_jsonschema_valid_enums.json");
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
	
}

