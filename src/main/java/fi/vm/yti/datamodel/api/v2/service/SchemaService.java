package fi.vm.yti.datamodel.api.v2.service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;
import org.apache.jena.vocabulary.VOID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import fi.vm.yti.datamodel.api.v2.mapper.mscr.CSVMapper;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.JSONSchemaMapper;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.SKOSMapper;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.XSDMapper;
import io.zenwave360.jsonrefparser.$RefParser;
import io.zenwave360.jsonrefparser.$Refs;


@Service
public class SchemaService {

	
	@Autowired
	private JSONSchemaMapper jsonSchemaMapper;
	
	@Autowired
	private XSDMapper xsdMapper;


	/**
	 * Transforms a JSON schema into an internal RDF model.
	 *
	 * @param schemaPID The schema PID.
	 * @param data      The byte array containing the JSON schema data that comes in
	 *                  request
	 * @return The transformed RDF model.
	 * @throws Exception   If an error occurs during the transformation process.
	 * @throws IOException If an I/O error occurs while reading the JSON schema
	 *                     data.
	 */
	public Model transformJSONSchemaToInternal(String schemaPID, JsonNode root) throws Exception, IOException {

		Model model = ModelFactory.createDefaultModel();
	
		// ObjectMapper is required to parse the JSON data		
		//ObjectMapper mapper = new ObjectMapper();		
		//JsonNode root = mapper.readTree(mapper.writeValueAsBytes(jsonObj));
		Resource modelResource = model.createResource(schemaPID);
		modelResource.addProperty(DCTerms.language, "en");

		// TODO: make this general
		// Handling of oneOf in the root element with single value - research.fi case 
		if(root.get("oneOf") != null && root.get("oneOf").size() == 1) {
			root = root.get("oneOf").get(0);
		}
		// Adding the schema to a corresponding internal model
		jsonSchemaMapper.handleObject("root", root, schemaPID, model);
		
		modelResource.addProperty(VOID.rootResource, ResourceFactory.createResource(schemaPID+"#root/Root"));
		return model;

	}
	
	public Model transformCSVSchemaToInternal(String schemaPID, byte[] data, String delimiter) throws Exception, IOException {
		CSVMapper mapper = new CSVMapper();		
		Model model = mapper.mapToModel(schemaPID, data, delimiter);
		Resource modelResource = model.createResource(schemaPID);
		Resource rootResource = model.createResource(schemaPID + "#root/Root");
		modelResource.addProperty(VOID.rootResource, rootResource);
		
		return model;
		
	}
	
	public JsonNode parseSchema(String data) throws Exception {
		// TODO: change this hacky way of utilizing the $RefParser		
		$RefParser parser = new $RefParser(data);
		$Refs refs = parser.parse().dereference().mergeAllOf().getRefs();
		Object resultMapOrList = refs.schema();
		System.out.println(((Map)resultMapOrList).keySet().size());
		
		ObjectMapper mapper = new ObjectMapper(); 
		return mapper.valueToTree(resultMapOrList);
	}

	public Model transformSKOSRDFToInternal(String pid, byte[] fileInBytes) throws Exception {		
		SKOSMapper m = new SKOSMapper();
		Model model = m.mapToModel(pid, fileInBytes);
		return model;
	}

	public Model addSKOSVocabulary(String pid, byte[] fileInBytes) throws Exception {
		Model m = ModelFactory.createDefaultModel();
		ByteArrayInputStream input = new ByteArrayInputStream(fileInBytes);
		m.read(input, null, "TTL");
		input.close();

		ResIterator schemes =  m.listSubjectsWithProperty(RDF.type, SKOS.ConceptScheme);
		if(!schemes.hasNext()) {
			throw new Exception("No ConceptScheme found.");
		}
		Resource scheme = schemes.next(); // just getting the first one
		Resource schema = m.createResource(pid);
		schema.addProperty(VOID.rootResource, scheme);
		
		
		
		return m;
	}
	
	public Model transformXSDToInternal(String schemaPID, String filePath) throws Exception {
		ObjectNode jroot = xsdMapper.mapToInternalJson(filePath);
		return transformJSONSchemaToInternal(schemaPID, jroot);
		
	}

	public Model transformXSDToInternal(String pid, byte[] fileInBytes) throws Exception {
		File tempFile = null;
		try {
			tempFile = File.createTempFile("schema", "xsd");
			FileUtils.writeByteArrayToFile(tempFile, fileInBytes);
			Model m = transformXSDToInternal(pid, tempFile.getPath());
			return m;
		}catch(Exception ex) {
			ex.printStackTrace();
			throw new Exception("Could not transform schema file." + ex.getMessage());	
		}	
		finally {
			if(tempFile != null) { tempFile.delete();}			
		}
		
	}

	public Model addRDFS(String pid, byte[] fileInBytes) throws Exception {
		Model m = ModelFactory.createDefaultModel();
		ByteArrayInputStream input = new ByteArrayInputStream(fileInBytes);
		m.read(input, null, "TTL");
		input.close();
		
		// add all classes as root resources 
		Resource schema = m.createResource(pid);
		m.listSubjectsWithProperty(RDF.type, RDFS.Class).forEach(resource -> {
			schema.addProperty(VOID.rootResource, resource);
		});
		return m;
	}

	public Model addSHACL(String pid, byte[] fileInBytes) throws Exception {
		Model m = ModelFactory.createDefaultModel();
		ByteArrayInputStream input = new ByteArrayInputStream(fileInBytes);
		m.read(input, null, "TTL");
		input.close();
		
		// add all nodeshapes as root resources
		Resource schema = m.createResource(pid);
		m.listSubjectsWithProperty(RDF.type, SH.NodeShape).forEach(resource -> {
			schema.addProperty(VOID.rootResource, resource);
		});		
		
		return m;
		
	}
}
