package fi.vm.yti.datamodel.api.v2.transformation;

import static org.junit.jupiter.api.Assertions.*;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.dto.MappingInfoDTO;
import fi.vm.yti.datamodel.api.v2.mapper.MappingMapper;
import jakarta.json.JsonObject;

class TestXSLTGenerator {

	public static final String xslNS = "http://www.w3.org/1999/XSL/Transform";
	private static Document toDoc(String s) throws Exception {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		Document doc = builder.parse(new InputSource(new StringReader(s)));
		return doc;

	}

	private int getNumberOfImmediateChildren(Node node) {
		
		int i = 0;
		Node childNode = node.getFirstChild();     
	    while( childNode !=null ){ 
	    	if(childNode .getNodeType() == Node.ELEMENT_NODE) {
	    		i++;	
	    	}
	    	
	        childNode = childNode.getNextSibling();         

	    }
	    return i;
	}
	private List<MappingInfoDTO> getMappings(String modelSource, String pid) {
		MappingMapper mappingMapper = new MappingMapper();
		Model model = RDFDataMgr.loadModel(modelSource);

		List<MappingInfoDTO> mappings = new ArrayList<MappingInfoDTO>();
		NodeIterator i = model.listObjectsOfProperty(model.getResource(pid), MSCR.mappings);
		while (i.hasNext()) {
			Resource mappingResource = i.next().asResource();
			MappingInfoDTO dto = mappingMapper.mapToMappingDTO(mappingResource.getURI(), model);
			mappings.add(dto);
		}
		return mappings;
	}
	/*
	@Test
	void testGenerateSimple1() throws Exception {
		String pid = "mscr:crosswalk:f52f0312-a214-4cc7-afea-2e0eb0e06c77";
		String modelSource = "mappings/simple-mappings-only.ttl";
		
		XSLTGenerator generator = new XSLTGenerator();
		String r = generator.generate(getMappings(modelSource, pid));
		Document doc = toDoc(r);
		assertEquals(15, getNumberOfImmediateChildren(doc.getFirstChild()));
	}
	
	@Test
	void testGenerateSimple2() throws Exception {
		String pid = "mscr:crosswalk:d897f3d9-5d52-42f6-98ff-0ce51bbdffc4";
		String modelSource = "mappings/simple-mappings-only2.ttl";
		
		XSLTGenerator generator = new XSLTGenerator();
		String r = generator.generate(getMappings(modelSource, pid));
		Document doc = toDoc(r);
		assertEquals(6, getNumberOfImmediateChildren(doc.getFirstChild()));
	}	
	@Test
	void testGenerateSimple3() throws Exception {
		String pid = "mscr:crosswalk:ceca0ba1-3076-4e26-94e9-2fce83a569ad";
		String modelSource = "mappings/simple-mappings-only3.ttl";
		
		XSLTGenerator generator = new XSLTGenerator();
		String r = generator.generate(getMappings(modelSource, pid));
		Document doc = toDoc(r);
		assertEquals(5, getNumberOfImmediateChildren(doc.getFirstChild()));
	}
	*/

}
