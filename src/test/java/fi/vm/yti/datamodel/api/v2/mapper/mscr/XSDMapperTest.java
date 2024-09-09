package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.avro.SchemaParseException;
/*
import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQConstants;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQItem;
import javax.xml.xquery.XQItemType;
import javax.xml.xquery.XQPreparedExpression;
import javax.xml.xquery.XQResultSequence;

import com.saxonica.xqj.SaxonXQDataSource;
*/
import org.apache.jena.rdf.model.Model;
import org.apache.jena.shacl.parser.ShapesParser.ParserResult;
import org.apache.jena.sparql.function.library.e;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.w3c.dom.Document;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSInput;
import org.xmlet.xsdparser.core.XsdParser;
import org.xmlet.xsdparser.xsdelements.XsdAbstractElement;
import org.xmlet.xsdparser.xsdelements.XsdAll;
import org.xmlet.xsdparser.xsdelements.XsdAnnotatedElements;
import org.xmlet.xsdparser.xsdelements.XsdAnnotation;
import org.xmlet.xsdparser.xsdelements.XsdBuiltInDataType;
import org.xmlet.xsdparser.xsdelements.XsdChoice;
import org.xmlet.xsdparser.xsdelements.XsdComplexContent;
import org.xmlet.xsdparser.xsdelements.XsdComplexType;
import org.xmlet.xsdparser.xsdelements.XsdElement;
import org.xmlet.xsdparser.xsdelements.XsdExtension;
import org.xmlet.xsdparser.xsdelements.XsdMultipleElements;
import org.xmlet.xsdparser.xsdelements.XsdNamedElements;
import org.xmlet.xsdparser.xsdelements.XsdRestriction;
import org.xmlet.xsdparser.xsdelements.XsdSchema;
import org.xmlet.xsdparser.xsdelements.XsdSequence;
import org.xmlet.xsdparser.xsdelements.XsdSimpleContent;
import org.xmlet.xsdparser.xsdelements.XsdSimpleType;
import org.xmlet.xsdparser.xsdelements.elementswrapper.ReferenceBase;
import org.xmlet.xsdparser.xsdelements.xsdrestrictions.XsdPattern;

import com.apicatalog.jsonld.lang.NodeObject;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;
import com.jayway.jsonpath.JsonPath;

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
	void testXsdParser() throws Exception {
		String filePath = "src/test/resources/xmlschema/eml1/eml.xsd";
		File file = new File(filePath);
		System.out.println(file.exists());
        XsdParser p = new XsdParser(filePath);
        
        
        Optional<XsdElement> el = p.getResultXsdElements().filter(e -> e.getName().equals("dataset")).findFirst();
        System.out.println(el.isPresent());
        
        XsdElement ds = el.get();
        List<XsdAbstractElement> elements =  ds.getXsdComplexType().getXsdElements().collect(Collectors.toList());
        
        XsdSequence dse = ds.getXsdComplexType().getChildAsSequence();

        List<XsdElement> children = dse.getChildrenElements().collect(Collectors.toList());
        children.forEach(c -> {
        	System.out.println(c.getName());
        	if(c.getName().equals("project")) {
        		c.getXsdComplexType().getXsdAttributes().forEach(a -> {System.out.println("attr: " + a.getName());});
        		
        	}
        });
        Optional<XsdElement> el2 = p.getResultXsdElements().filter(e -> e.getName().equals("project")).findFirst();
        
        
        el2.get().getAnnotation().getDocumentations().forEach(d -> { System.out.println(d.getContent()); });
	}

	
	@Test
	void testTraverseTree() throws Exception {
		//String filePath = "src/test/resources/xmlschema/sample.xsd";
		//String filePath = "src/test/resources/xmlschema/eml1/eml.xsd";
		String filePath = "src/test/resources/xmlschema/clarin/LinguisticFieldtrip.xsd";
		//String filePath = "https://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/1.x/profiles/clarin.eu:cr1:p_1407745712081/xsd";
		//String filePath = "src/test/resources/xmlschema/datacite/4.4/metadata.xsd";
		//String filePath = "https://schema.datacite.org/meta/kernel-4.4/metadata.xsd";
		//String filePath = "https://raw.githubusercontent.com/jkesanie/eml-profile/master/eml.xsd";
        //String filePath = "src/test/resources/xmlschema/dublincore/simpledc20021212.xsd";
		//String filePath = "src/test/resources/xmlschema/dublincore/dcterms.xsd";
		//String filePath = "https://schema.datacite.org/meta/kernel-3.1/metadata.xsd";
		//String filePath = "src/test/resources/xmlschema/eudat-core/eudat-core.xsd";
		//String filePath = "https://raw.githubusercontent.com/OpenEdition/tei.openedition/master/xsd/tei.openedition.1.6.3/document.xsd";
		ObjectNode jroot = mapper.mapToInternalJson(filePath);
        ObjectWriter writer = m.writer(new DefaultPrettyPrinter());
        //writer.writeValue(new File("xmlschema-to-jsonschema.json"), jroot);
	}
	
	

	

	
	
	@Test
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
	void testLoadImportEml() throws Exception {		
		//String filePath = "https://schema.datacite.org/meta/kernel-3.1/metadata.xsd";
		String filePath = "src/test/resources/xmlschema/eml1/eml.xsd";		
		SchemaParserResultDTO r = mapper.loadSchema(filePath);
		assertTrue(r.isOk());
		assertEquals(2,  r.getTree().getHasPart().size());
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
	void testImportOpenaireToInternalJSON() throws Exception {
		String url = "https://raw.githubusercontent.com/openaire/guidelines-literature-repositories/master/schemas/4.0/openaire.xsd";
		ObjectNode obj = mapper.mapToInternalJson(url);
		
		ObjectMapper m = new ObjectMapper();
		System.out.println(m.writeValueAsString(obj));

		ObjectNode r = (ObjectNode) obj.at("/properties/format");
		assertEquals("http://purl.org/dc/elements/1.1/format", r.get("@id").asText());
		ObjectNode r2 = (ObjectNode) obj.at("/properties/identifier");
		assertEquals("http://datacite.org/schema/kernel-4identifier", r2.get("@id").asText());
	}
	
	@Test
	void testCircularReferences1() throws Exception {

		ObjectNode obj = mapper.mapToInternalJson("src/test/resources/xmlschema/circular-references1.xsd");
		
		ObjectMapper m = new ObjectMapper();
		System.out.println(m.writeValueAsString(obj));
		
	}

	
	
}
