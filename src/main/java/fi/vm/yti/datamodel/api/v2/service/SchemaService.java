package fi.vm.yti.datamodel.api.v2.service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;
import org.apache.jena.vocabulary.VOID;
import org.coode.owlapi.turtle.TurtleOntologyFormat;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.jsonldjava.utils.JsonUtils;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.CSVMapper;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.JSONSchemaMapper;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.SKOSMapper;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.XSDMapper;
import fi.vm.yti.datamodel.api.v2.service.dtr.DTRClient;
import io.zenwave360.jsonrefparser.$RefParser;
import io.zenwave360.jsonrefparser.$Refs;


@Service
public class SchemaService {

	
	@Autowired
	private JSONSchemaMapper jsonSchemaMapper;
	
	@Autowired
	private XSDMapper xsdMapper;

	@Autowired
	private DTRClient dtrClient;

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
		addDefaultRootResourceForJSONSchema(modelResource, model);		
		return model;

	}
	
	public Model transformCSVSchemaToInternal(String schemaPID, byte[] data, String delimiter) throws Exception, IOException {
		CSVMapper mapper = new CSVMapper();		
		Model model = mapper.mapToModel(schemaPID, data, delimiter);
		Resource modelResource = model.createResource(schemaPID);
		addDefaultRootResourceForCSV(modelResource, model);
		
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

	public Model addOWL(String pid, String url, byte[] bytes) throws Exception {		
		OWLOntologyManager manager =OWLManager.createOWLOntologyManager();
		OWLOntology ont = null;
		if(url != null) {
			IRI ontologyIRI = IRI.create(url);
			ont = manager.loadOntologyFromOntologyDocument(ontologyIRI);
		}
		else if(bytes != null) {
			InputStream is = new ByteArrayInputStream(bytes);
			ont = manager.loadOntologyFromOntologyDocument(is);
			is.close();
		}
		else {
			throw new RuntimeException("Must provide either url or bytes input");
		}
		
		 
		TurtleOntologyFormat outputFormat = new TurtleOntologyFormat();
		Path file = Files.createTempFile("prov", ".ttl");
		try (OutputStream outputStream = Files.newOutputStream(file)) {
			manager.saveOntology(ont, outputFormat,				
					outputStream);
		}
		Model model = ModelFactory.createDefaultModel();
		model.read(file.toUri().toString());
		Resource schema = model.createResource(pid);
		addDefaultRootResourceForOWL(schema, model);
		return model;		
	}
	
	
	public Model addRDFS(String pid, byte[] fileInBytes) throws Exception {
		Model m = ModelFactory.createDefaultModel();
		ByteArrayInputStream input = new ByteArrayInputStream(fileInBytes);
		m.read(input, null, "TTL");
		input.close();
		
		// add all classes as root resources 
		Resource schema = m.createResource(pid);
		addDefaultRootResourceForRDFS(schema, m);
		return m;
	}

	public Model addSHACL(String pid, byte[] fileInBytes) throws Exception {
		Model m = ModelFactory.createDefaultModel();
		ByteArrayInputStream input = new ByteArrayInputStream(fileInBytes);
		m.read(input, null, "TTL");
		input.close();
		
		// add all nodeshapes as root resources
		Resource schema = m.createResource(pid);
		addDefaultRootResourceForSHACL(schema, m);
		
		return m;
		
	}

	public void updatePropertyDataTypeFromDTR(Model model, String propID, String datatypeURI) throws Exception {
		Resource prop = ResourceFactory.createResource(propID);
		if(!model.containsResource(prop)) {
			throw new Exception("Property " + prop + " not found");
		}		
		prop = model.getResource(propID);
		Resource datatype = model.createResource(datatypeURI);
		model.removeAll(prop, SH.datatype, null);
		model.add(prop, SH.datatype, datatype);

	}
	public Model fetchAndMapDTRType(String newPropID) throws Exception {		
		// fetch json schema version through the typeapi 
		String json = dtrClient.getTypeAsJSONSchema(newPropID);
		ObjectMapper m = new ObjectMapper();
		ObjectNode propertyNode = (ObjectNode)m.readTree(json);
		if(propertyNode.get("type") == null) {
			throw new Exception("JSON Schema representation of a DTR type must have property type");
		}
		if(propertyNode.get("type").asText().equals("object")) {
			throw new Exception("Complex DTR types are not supported yet");
		}
		
		ObjectNode context = m.createObjectNode();
		context.put("title", "mscr:jsonschema:title");

		context.put("description", "mscr:jsonschema:description");
		context.put("type", "mscr:jsonschema:type");
		context.put("pattern", "mscr:jsonschema:pattern");
		context.put("$schema", "mscr:jsonschema:schema");
		context.put("hdl", "https://hdl.handle.net/");
		
		propertyNode.set("@context", context);
		propertyNode.put("@id", "hdl:" +newPropID);
		propertyNode.put("@type", "mscr:JSONSchema");
		// transform json schema to RDF
		Model propModel = ModelFactory.createDefaultModel();
		String newJson = m.writeValueAsString(propertyNode);
	
		StringReader reader = new StringReader(newJson);
		propModel.read(reader, null, "JSON-LD");
		reader.close();
		
		return propModel;
		
	}

	public String dtrSearchBasicInfoTypes(String queryBy, String query, int page, int pageSize) throws Exception {
		return dtrClient.searchTypes(queryBy, query, "type:BasicInfoType", page, pageSize);
	}
	
	private void addDefaultRootResourceForSHACL(Resource schema, Model m) {
		m.listSubjectsWithProperty(RDF.type, SH.NodeShape).forEach(resource -> {
			schema.addProperty(VOID.rootResource, resource);
		});	
	}
	private void addDefaultRootResourceForRDFS(Resource schema, Model m) {
		m.listSubjectsWithProperty(RDF.type, RDFS.Class).forEach(resource -> {
			schema.addProperty(VOID.rootResource, resource);
		});			
		
	}
	private void addDefaultRootResourceForOWL(Resource schema, Model m) {
		m.listSubjectsWithProperty(RDF.type, OWL.Class).forEach(resource -> {
			schema.addProperty(VOID.rootResource, resource);
		});
	}	
	private void addDefaultRootResourceForJSONSchema(Resource schema, Model m) {
		schema.addProperty(VOID.rootResource, m.getResource(schema.getURI()+"#root/Root"));
	}	
	private void addDefaultRootResourceForCSV(Resource schema, Model m) {
		schema.addProperty(VOID.rootResource, m.getResource(schema.getURI()+"#root/Root"));
	}	
	private void addDefaultRootResourceForSKOS(Resource schema, Model m) {
		schema.addProperty(VOID.rootResource, m.getResource(schema.getURI()+"#root/Root"));
	}	
		

	public Model resetRootResource(String schemaInternalId, SchemaFormat format, Model model) {
		Resource schema = model.getResource(schemaInternalId);
		model.removeAll(schema, VOID.rootResource, null);
		if(format == SchemaFormat.SHACL) {
			addDefaultRootResourceForSHACL(schema, model);
		}
		else if(format == SchemaFormat.OWL) {
			addDefaultRootResourceForOWL(schema, model);
		}
		else if(format == SchemaFormat.RDFS) {
			addDefaultRootResourceForRDFS(schema, model);
		}
		else if(format == SchemaFormat.SKOSRDF) {
			// do nothing
		}		
		else {
			schema.addProperty(VOID.rootResource, model.getResource(schemaInternalId +"#root/Root"));	
		}		
		return model;
	}
	public Model setRootResource(String schemaInternalId, SchemaFormat format, String newRootResourceId, Model model) throws Exception {
		
		Resource newRootResource = ResourceFactory.createResource(newRootResourceId);
		if(!model.containsResource(newRootResource)) {
			throw new Exception("Resource " + newRootResourceId + " not found");
		}
		// TODO: Add check for sh:NodeShape or sh:PropertyShape OR skos:Concept
		Resource schema = model.getResource(schemaInternalId);
		schema.removeAll(VOID.rootResource);
		schema.addProperty(VOID.rootResource, newRootResource);
		
		return model;
	}
	
	public Model updateRootResourceMetadata(String rootResource, String schemaID, Model model) {
		Resource modelResource = model.getResource(schemaID);
		modelResource.removeAll(MSCR.customRoot);
		if(rootResource != null) {
			modelResource.addProperty(MSCR.customRoot, model.createResource(rootResource));	
		}
		
		return model;
	}
}
	