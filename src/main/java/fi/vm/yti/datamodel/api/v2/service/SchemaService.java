package fi.vm.yti.datamodel.api.v2.service;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shacl.vocabulary.SHACL;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;

@Service
public class SchemaService {

	private Resource addDatatypeProperty(String propID, JsonNode node, Model model, String schemaPID) {
		Resource propertyResource = model.createResource(schemaPID + "#" + propID);
		propertyResource.addProperty(RDF.type, model.getResource(SHACL.PropertyShape.getURI()));
		propertyResource.addProperty(DCTerms.type, OWL.DatatypeProperty);		
		propertyResource.addProperty(model.getProperty(SHACL.maxCount.getURI()), model.createTypedLiteral(1));
		propertyResource.addProperty(model.getProperty(SHACL.minCount.getURI()), model.createTypedLiteral(1));
		propertyResource.addProperty(model.getProperty(SHACL.name.getURI()), propID);
		propertyResource.addProperty(model.getProperty(SHACL.datatype.getURI()), XSD.xstring);
		propertyResource.addProperty(model.getProperty(SHACL.path.getURI()), propID);		
		return propertyResource;
		
	}
	
	
	private Resource addObjectProperty(String propID, JsonNode node, Model model, String schemaPID, String targetShape) {
		Resource propertyResource = model.createResource(schemaPID + "#" + propID);
		propertyResource.addProperty(RDF.type, model.getResource(SHACL.PropertyShape.getURI()));
		propertyResource.addProperty(DCTerms.type, OWL.ObjectProperty);
		if(node.get("type").asText().equals("array")) {
			if(node.has("maxItems"))
				propertyResource.addProperty(model.getProperty(SHACL.maxCount.getURI()), model.createTypedLiteral(node.get("maxItems").asInt()));
			if(node.has("minItems"))
				propertyResource.addProperty(model.getProperty(SHACL.minCount.getURI()), model.createTypedLiteral(node.get("minItems").asInt()));

		}
		else {
			propertyResource.addProperty(model.getProperty(SHACL.maxCount.getURI()), model.createTypedLiteral(1));
			propertyResource.addProperty(model.getProperty(SHACL.minCount.getURI()), model.createTypedLiteral(1));
			
		}
		propertyResource.addProperty(model.getProperty(SHACL.name.getURI()), propID);
		propertyResource.addProperty(model.getProperty(SHACL.path.getURI()), propID);
		propertyResource.addProperty(model.getProperty(SHACL.node.getURI()), model.createResource(targetShape));
				
		return propertyResource;
		
	}		
	
	
	private void handleObject(String propID, JsonNode node, String schemaPID, Model model) {

		Resource nodeShapeResource = model.createResource(schemaPID + "#" + propID);
		
		nodeShapeResource.addProperty(RDF.type, model.getResource(SHACL.NodeShape.getURI()));
		nodeShapeResource.addProperty(MSCR.localName, propID);
		if(node.has("description"))
			nodeShapeResource.addProperty(DCTerms.description, node.get("description").asText());
		nodeShapeResource.addProperty(model.getProperty(SHACL.name.getURI()), propID);
		if(node == null || node.get("properties") == null) {
			return;
		}
		Iterator<Entry<String, JsonNode>> propertiesIterator = node.get("properties").fields();
		while(propertiesIterator.hasNext()) {
			Entry<String, JsonNode> entry = propertiesIterator.next();
			if(entry.getKey().startsWith("_") || entry.getKey().startsWith("$"))
				continue;
			//System.out.println(entry.getValue());
			if(entry.getValue().get("type") != null && entry.getValue().get("type").asText().equals("object")) {
				// add object property 
				Resource propertyShape = addObjectProperty(entry.getKey(), entry.getValue(), model, schemaPID, schemaPID + "#" + propID + "/" + entry.getKey());
				nodeShapeResource.addProperty(model.getProperty(SHACL.property.getURI()), propertyShape);
				handleObject(propID + "/" + entry.getKey(), entry.getValue(), schemaPID, model);
			}
			else if(entry.getValue().get("type") != null && entry.getValue().get("type").asText().equals("array") && entry.getValue().get("items").has("type") && entry.getValue().get("items").get("type").asText().equals("object")) {
				
				Resource propertyShape = addObjectProperty(entry.getKey(), entry.getValue(), model, schemaPID, schemaPID + "#" + propID + "/" + entry.getKey());
				nodeShapeResource.addProperty(model.getProperty(SHACL.property.getURI()), propertyShape);	
				handleObject(propID + "/" + entry.getKey(), entry.getValue().get("items"), schemaPID, model);	
				
			}
			else {
				Resource propertyShape = addDatatypeProperty(propID + "/" + entry.getKey(), entry.getValue(), model, schemaPID);
				nodeShapeResource.addProperty(model.getProperty(SHACL.property.getURI()), propertyShape);
			}
			
		}	

	}
	public Model transformJSONSchemaToInternal(String schemaPID, byte[] data) throws Exception, IOException {
		
		
		Model model = ModelFactory.createDefaultModel();
		
		ObjectMapper mapper = new ObjectMapper();
		JsonNode root = mapper.readTree(data);
		
		String schemaVersion  =root.get("$schema").asText();
		if(!schemaVersion.equals("http://json-schema.org/draft-04/schema#")) {
			throw new Exception("Unsupported JSON schema version");
		}
		
		var modelResource = model.createResource(schemaPID);
		modelResource.addProperty(DCTerms.language, "en");
		
		
		handleObject("root", root, schemaPID, model);
			
		
		return model;
		
	}
	
}
