package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import org.eclipse.rdf4j.model.vocabulary.DCTERMS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;

import com.fasterxml.jackson.databind.JsonNode;

import fi.vm.yti.datamodel.api.index.OpenSearchConnector;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;

@Service
public class JSONSchemaMapper {
	

	private static final Logger logger = LoggerFactory.getLogger(JSONSchemaMapper.class);
	
	private final Map<String, Resource> XSDTypesMap = Map.ofEntries(Map.entry("string", XSD.xstring),
			Map.entry("number", XSD.xfloat), Map.entry("integer", XSD.integer), Map.entry("boolean", XSD.xboolean),
			Map.entry("null", MSCR.NULL), Map.entry("object", XSD.anyURI));

	private final Map<String, Property> JSONSchemaToSHACLMap = Map.ofEntries(
			Map.entry("description", SH.description), Map.entry("default", SH.defaultValue),
			Map.entry("title", SH.name), Map.entry("additionalProperties", SH.closed),
			Map.entry("enum", SH.in),
//			Map.entry("format", SHACL.format, -- no SHACL type - look into it
			Map.entry("maximum", SH.maxInclusive), Map.entry("minimum", SH.minInclusive),
			Map.entry("exclusiveMaximum", SH.maxExclusive),
			Map.entry("exclusiveMinimum", SH.minExclusive),
			Map.entry("minItems", SH.minCount), Map.entry("maxLength", SH.maxLength),
			Map.entry("minLength", SH.minLength), Map.entry("not", SH.not),
			Map.entry("pattern", SH.pattern )

	);

	private final Set<String> JSONSchemaNumericalProperties = Set.of("maximum", "minimum", "exclusiveMaximum",
			"exclusiveMinimum", "minItems", "minLength", "maxLength");
	
	private final Set<String> JSONSchemaBooleanProperties = Set.of("additionalProperties");

	private void checkAndAddPropertyFeature(JsonNode node, Model model, Resource propertyResource, String propID) {
		for (String key : JSONSchemaToSHACLMap.keySet()) {
			JsonNode propertyNode = node.get(key);
			// the second condition ensures that an object's children properties are not added to that object
			if (propertyNode != null & node.has(key)) {
				if (JSONSchemaNumericalProperties.contains(key)) {
					// special handing for exclusiveMaximum and exclusiveMinimum because they work differently with Jsonschema and SHACL (Draft 04!)
					if(key.equals("exclusiveMaximum")) {
						if(node.has("maximum")) {
							propertyResource.addProperty(JSONSchemaToSHACLMap.get("exclusiveMaximum"),
									model.createTypedLiteral(node.get("maximum").numberValue()));	
						}
					}
					else if(key.equals("exclusiveMinimum")) {
						if(node.has("minimum")) {
							propertyResource.addProperty(JSONSchemaToSHACLMap.get("exclusiveMinimum"),
									model.createTypedLiteral(node.get("minimum").numberValue()));	
						}
					}					
					else {
						propertyResource.addProperty(JSONSchemaToSHACLMap.get(key),
								model.createTypedLiteral(propertyNode.numberValue()));						
					}

				} else if (JSONSchemaBooleanProperties.contains(key)) {
					propertyResource.addProperty(JSONSchemaToSHACLMap.get(key),
							model.createTypedLiteral(propertyNode.asBoolean()));
				} 
				else if(key == "enum") {
					if (!propertyNode.isEmpty()) {
						Bag bag = model.createBag();
						String nodeType = node.has("type") ? node.get("type").asText() : "string";
						for(int i = 0; i < propertyNode.size(); i++){
							if (nodeType.equals("boolean"))
								bag.add(model.createTypedLiteral(propertyNode.get(i).asBoolean())); 
							else if (nodeType.equals("integer"))
								bag.add(model.createTypedLiteral(propertyNode.get(i).numberValue()));
							else 
								bag.add(model.createLiteral(propertyNode.get(i).asText()));
						}	
						
						propertyResource.addProperty(SH.in,	bag);
					} 
				}
				else {
					propertyResource.addProperty(JSONSchemaToSHACLMap.get(key),
							propertyNode.asText());
				}
			}
		}
		checkAndDefaultName(propertyResource, propID);
	}

	private void checkAndDefaultName(Resource propertyResource, String propID) {
		if(!propertyResource.hasProperty(SH.name)) {
			String defaultName = propID.substring(propID.lastIndexOf("/")+1);
			propertyResource.addLiteral(SH.name, ResourceFactory.createPlainLiteral(defaultName));
		}
		
	}

	private void handleRequiredProperty(JsonNode node, Model model, Resource propertyResource, boolean isRequired) {		//
		if (isRequired) {
			propertyResource.addProperty(SH.minCount, model.createTypedLiteral(1));
		}
	}

	/**
	 * Adds a datatype property to the RDF model.
	 * 
	 * @param propID    The property ID.
	 * @param node      The JSON node containing the property details.
	 * @param model     The RDF model.
	 * @param schemaPID The schema PID.
	 * @return The created resource representing the datatype property.
	 */
	private Resource addDatatypeProperty(String propID, JsonNode node, Model model, String schemaPID, String type) {
		Resource propertyResource = model.createResource(schemaPID + "#" + propID);
		if(node.has("@id")) {
			propertyResource.addProperty(MSCR.qname, model.createResource(node.get("@id").asText()));			
		}
		
		propertyResource.addProperty(RDF.type, SH.PropertyShape);
		if(!model.contains(propertyResource, DCTerms.type, OWL.ObjectProperty)) {
			propertyResource.addProperty(DCTerms.type, OWL.DatatypeProperty);
		}
		
		if(node.has("@type") && node.get("@type").asText().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")) {
			propertyResource.addProperty(SH.datatype, RDF.langString);	
		}
		else {
			Resource typeResource = XSDTypesMap.get(type);
			if(typeResource == null) {
				typeResource = XSD.xstring;
			}
			propertyResource.addProperty(SH.datatype, typeResource);
		}
		
		propertyResource.addProperty(SH.path, ResourceFactory.createResource(schemaPID + "#" + propID));

		checkAndAddPropertyFeature(node, model, propertyResource, propID);

		return propertyResource;
	}

	/**
	 * 
	 * Adds an object property to the RDF model.
	 * 
	 * @param propID      The property ID.
	 * @param node        The JSON node containing the property details.
	 * @param model       The RDF model.
	 * @param schemaPID   The schema PID.
	 * @param targetShape The target shape for the object property.
	 * @return The created resource representing the object property.
	 */
	private Resource addObjectProperty(String propID, JsonNode node, Model model, String schemaPID,
			String targetShape) {
		Resource propertyResource = model.createResource(schemaPID + "#" + propID);
		propertyResource.addProperty(RDF.type, SH.PropertyShape);
		propertyResource.addProperty(DCTerms.type, OWL.ObjectProperty);
		checkAndAddPropertyFeature(node, model, propertyResource, propID);
		propertyResource.addProperty(SH.path, ResourceFactory.createResource(schemaPID + "#" + propID));
		propertyResource.addProperty(SH.node, model.createResource(targetShape));

		return propertyResource;
	}

	private Resource handleDatatypeProperty(String propID, Entry<String, JsonNode> entry, Model model, String schemaPID,
			Resource nodeShapeResource, boolean isRequired, boolean isArrayItem) {
		
		String entryType = entry.getValue().has("type") ? entry.getValue().get("type").asText() : "string"; 
		final String key = URLEncoder.encode(entry.getKey());
		Resource propertyResource = addDatatypeProperty(propID + "/" + key, entry.getValue(), model,
				schemaPID, entryType);
		nodeShapeResource.addProperty(SH.property, propertyResource);
		if (!isArrayItem) {
			handleRequiredProperty(entry.getValue(), model, propertyResource, isRequired);
		} 
		if (entry.getValue().get("type") != null && entry.getValue().get("type").asText().equals("string") & entry.getValue().has("pattern")) {
			propertyResource.addProperty(SH.pattern, entry.getValue().get("pattern").asText());
		}
		return propertyResource;
	}
	
	private String capitaliseNodeIdentifier(String propID) {
		int lastSlash = propID.lastIndexOf('/');
		String stringAfterSlash = propID.substring(lastSlash + 1);
		char firstChar = Character.toUpperCase(stringAfterSlash.charAt(0));
		return propID + "/" + firstChar + stringAfterSlash.substring(1);
	}

	private boolean isLangString(Entry <String, JsonNode> entry) {
		if(entry.getValue().has("@type")) {			
			return entry.getValue().get("@type").asText().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString");
		}
		return false;
	}
	
	private boolean isArray(Entry <String, JsonNode> entry) {
		return entry.getValue().get("type").asText().equals("array");
	}
	
	private boolean isObject(Entry <String, JsonNode> entry) {
		return entry.getValue().get("type").asText().equals("object");
	}
	
	private boolean hasObjectItems(Entry <String, JsonNode> entry) {
		
		return (entry.getValue().has("items") && entry.getValue().get("items").has("type") 
			 && entry.getValue().get("items").get("type").asText().equals("object"));
	}
	
	/**
	 * 
	 * Handles an object property and creates the corresponding SHACL (Node)Shape.
	 * 
	 * @param propID    The property ID.
	 * @param node      The JSON node containing the property details.
	 * @param schemaPID The schema PID.
	 * @param model     The RDF model.
	 */
	public void handleObject(String propID, JsonNode node, String schemaPID, Model model, Map<String, JsonNode> definitions) {

		String propIDCapitalised = capitaliseNodeIdentifier(propID);
		String nameProperty = propID.substring(propID.lastIndexOf("/") + 1);		
		Resource nodeShapeResource = model.createResource(schemaPID + "#" + propIDCapitalised);
		
		nodeShapeResource.addProperty(RDF.type, (SH.NodeShape));
		
		if(schemaPID.indexOf(":definition") < 0) {
			nodeShapeResource.addProperty(SH.name, nameProperty);
			nodeShapeResource.addProperty(MSCR.localName, nameProperty);
		}
		
		
		if (node == null || node.get("properties") == null) 
			return;
		if (node.has("description"))
			nodeShapeResource.addProperty(DCTerms.description, node.get("description").asText());
		if (node.has("additionalProperties"))
			nodeShapeResource.addProperty(SH.closed, model.createTypedLiteral(!node.get("additionalProperties").asBoolean()));
		if(node.has("@id")) {
			nodeShapeResource.addProperty(MSCR.qname, model.createResource(node.get("@id").asText()));			
		}
		
		/*
		 * Iterate over properties If a property is an array or object – add and
		 * recursively iterate over them. If a property is a datatype or literal – it's just added.
		 */

		Iterator<Entry<String, JsonNode>> propertiesIterator = node.get("properties").fields();
		while (propertiesIterator.hasNext()) {
			Entry<String, JsonNode> entry = propertiesIterator.next();
			String valueType = "string"; // default value
			if (entry.getKey().startsWith("_") || entry.getKey().startsWith("$"))
				continue;
			if (entry.getValue().get("type") != null) {
				valueType = entry.getValue().get("type").asText();
			}
			final String key = URLEncoder.encode(entry.getKey());
			Resource propertyShape = null;
			if (valueType.equals("object")) {
				propertyShape = addObjectProperty(propIDCapitalised + "/" + key, entry.getValue(), model, schemaPID,
						schemaPID + "#" + propIDCapitalised + "/" + key +"/" + StringUtils.capitalise(key));
				if(entry.getValue().has("@id")) {
					propertyShape.addProperty(MSCR.qname, model.createResource(entry.getValue().get("@id").asText()));			
				}

				// default max
				if(!entry.getValue().has("maxItems")) {
					propertyShape.addLiteral(SH.maxCount, model.createTypedLiteral(1));	
				}
				else {
					if(entry.getValue().get("maxItems").asText(null) != null &&  !entry.getValue().get("maxItems").asText().equals("unbounded")) {
						propertyShape.addLiteral(SH.maxCount, model.createTypedLiteral(entry.getValue().get("maxItems").asInt()));
					}
					
				}
				
				nodeShapeResource.addProperty(SH.property, propertyShape);
				handleObject(propIDCapitalised + "/" + key, entry.getValue(), schemaPID, model,definitions);	
			}
			else if (valueType.equals("array")) {
				if(isLangString(entry)) {
					Entry<String, JsonNode> item = Map.entry(key, entry.getValue());
					propertyShape = handleDatatypeProperty(propIDCapitalised, item, model, schemaPID, nodeShapeResource, false, true);
				}					
				else {
					propertyShape = addObjectProperty(propIDCapitalised + "/" + key, entry.getValue(), model, schemaPID,
							schemaPID + "#" + propIDCapitalised + "/" + key +"/" + StringUtils.capitalise(key));
					if(entry.getValue().has("@id")) {
						propertyShape.addProperty(MSCR.qname, model.createResource(entry.getValue().get("@id").asText()));			
					}
					
					nodeShapeResource.addProperty(SH.property, propertyShape);
					
					if (hasObjectItems(entry)) {
						handleObject(propIDCapitalised + "/" + key, entry.getValue().get("items"), schemaPID, model, definitions);
					}
					else {
						if(!entry.getValue().has("items")) {
							logger.warn("Array property " + entry.getKey() + " does not have any items. Skipping.");
						}
						else {
							Entry<String, JsonNode> arrayItem = Map.entry(key, entry.getValue().get("items"));
							propertyShape = handleDatatypeProperty(propIDCapitalised, arrayItem, model, schemaPID, nodeShapeResource, false, true);
							
						}
					}
									
					if(entry.getValue().get("maxItems") != null && entry.getValue().get("maxItems").asText(null) != null &&  !entry.getValue().get("maxItems").asText().equals("unbounded")) {
						propertyShape.addLiteral(SH.maxCount, model.createTypedLiteral(entry.getValue().get("maxItems").asInt()));
					}					
					
				}
				
			}
			else {
				boolean isRequired = (entry.getValue().has("required") && (entry.getValue().get("required").asBoolean() == true));								
				propertyShape = handleDatatypeProperty(propIDCapitalised, entry, model, schemaPID, nodeShapeResource, isRequired, false);
				// default max
				if(!entry.getValue().has("maxItems")) {
					propertyShape.addLiteral(SH.maxCount, model.createTypedLiteral(1));	
				}
				else {					
					if(entry.getValue().get("maxItems").asText(null) != null &&  !entry.getValue().get("maxItems").asText().equals("unbounded")) {
						propertyShape.addLiteral(SH.maxCount, model.createTypedLiteral(entry.getValue().get("maxItems").asInt()));
					}					
				}
				
			}

			if (entry.getValue().get("$ref") != null) {
				String ref = entry.getValue().get("$ref").asText();
				// TODO: set the class and datatype according to the references definition
				String shapeName = ref.substring(ref.lastIndexOf("/")+1);
				JsonNode defObj = definitions.get(shapeName);
				if(defObj != null) {
					String targetType = defObj.get("type") != null ? defObj.get("type").asText() : "string";
					propertyShape.removeAll(SH.datatype);
					propertyShape.removeAll(DCTerms.type);
					if(targetType.equals("object")) {
												
						propertyShape.addProperty(SH.node, model.createResource(schemaPID + ":definition#" + shapeName + "/" +  shapeName));
						propertyShape.addProperty(DCTerms.type, OWL.ObjectProperty);
					}
					else {
						Resource typeResource = XSDTypesMap.get(targetType);
						propertyShape.addProperty(SH.datatype, typeResource);
						propertyShape.addProperty(DCTerms.type, OWL.DatatypeProperty);
					}
				}
				else {
					throw new RuntimeException("Referenced object "+ shapeName + " not found in definitions.");
				}
			}

		}
	
	}
	
	public void handleDefinitions(Map<String, JsonNode> defs, String schemaPID, Model model) {
		if(defs.size() == 0) {
			return;
		}
		Iterator<String> defNames = defs.keySet().iterator();
		while(defNames.hasNext()) {
			String defName = defNames.next();
			//System.out.println(defName);
			Model m = ModelFactory.createDefaultModel();
			handleObject(defName, defs.get(defName), schemaPID + ":definition", m, defs);
			//m.write(System.out, "TURTLE");
			model.add(m);
		}
		
		
	}
}
