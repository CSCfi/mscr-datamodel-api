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
	@Test
	void testGenerate() throws Exception {
		MappingMapper mappingMapper = new MappingMapper();
		Model model = RDFDataMgr.loadModel("mappings/simple-mappings-only3.ttl");

		String pid = "mscr:crosswalk:ceca0ba1-3076-4e26-94e9-2fce83a569ad";
		List<MappingInfoDTO> mappings = new ArrayList<MappingInfoDTO>();
		NodeIterator i = model.listObjectsOfProperty(model.getResource(pid), MSCR.mappings);
		while (i.hasNext()) {
			Resource mappingResource = i.next().asResource();
			MappingInfoDTO dto = mappingMapper.mapToMappingDTO(mappingResource.getURI(), model);
			mappings.add(dto);
		}

		XSLTGenerator generator = new XSLTGenerator();

		String r = generator.generate(mappings);
		Document doc = toDoc(r);
		assertEquals(5, getNumberOfImmediateChildren(doc.getFirstChild()));
	}

}
