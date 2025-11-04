package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFList;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;

import com.fasterxml.jackson.databind.JsonNode;

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

	private void checkAndAddPropertyFeature(JsonNode node, Model model, Resource propertyResource, String propID, String propKey) {
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
						RDFList list = model.createList(new RDFNode[] {});					
						String nodeType = node.has("type") ? node.get("type").asText() : "string";
						for(int i = 0; i < propertyNode.size(); i++){
							if (nodeType.equals("boolean"))
								list = list.with(model.createTypedLiteral(propertyNode.get(i).asBoolean())); 
							else if (nodeType.equals("integer"))
								list = list.with(model.createTypedLiteral(propertyNode.get(i).numberValue()));
							else 
								list = list.with(model.createLiteral(propertyNode.get(i).asText()));
						}	
						
						propertyResource.addProperty(SH.in,	list);
					} 
				}
				else {
					propertyResource.addProperty(JSONSchemaToSHACLMap.get(key),
							propertyNode.asText());
				}
			}
		}
		checkAndDefaultName(propertyResource, propID, propKey);
	}

	private void checkAndDefaultName(Resource propertyResource, String propID, String propKey) {
		if(!propertyResource.hasProperty(SH.name)) {
			propKey = URLDecoder.decode(propKey);
			propertyResource.addLiteral(SH.name, ResourceFactory.createPlainLiteral(propKey));
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
	private Resource addDatatypeProperty(String propID, JsonNode node, Model model, String schemaPID, String type, String propKey) {
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

		checkAndAddPropertyFeature(node, model, propertyResource, propID, propKey);

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
			String targetShape, String propKey) {
		propID = URLEncoder.encode(propID);		
		Resource propertyResource = model.createResource(schemaPID + "#" + propID);
		propertyResource.addProperty(RDF.type, SH.PropertyShape);
		propertyResource.addProperty(DCTerms.type, OWL.ObjectProperty);
		checkAndAddPropertyFeature(node, model, propertyResource, propID, propKey);
		propertyResource.addProperty(SH.path, ResourceFactory.createResource(schemaPID + "#" + propID));
		propertyResource.addProperty(SH.node, model.createResource(targetShape));

		return propertyResource;
	}

	private Resource handleDatatypeProperty(String propID, JsonNode entry, String key,  Model model, String schemaPID,
			Resource nodeShapeResource, boolean isRequired, boolean isArrayItem) {
		
		String entryType = entry.has("type") ? entry.get("type").asText() : "string"; 
		//final String key = URLEncoder.encode(entryKey);
		Resource propertyResource = addDatatypeProperty(propID + "-" + key, entry, model,
				schemaPID, entryType, key);
		nodeShapeResource.addProperty(SH.property, propertyResource);
		if (!isArrayItem) {
			handleRequiredProperty(entry, model, propertyResource, isRequired);
		} 
		if (entry.get("type") != null && entry.get("type").asText().equals("string") & entry.has("pattern")) {
			propertyResource.addProperty(SH.pattern, entry.get("pattern").asText());
		}
		return propertyResource;
	}	
	private Resource handleDatatypeProperty(String propID, Entry<String, JsonNode> entry, Model model, String schemaPID,
			Resource nodeShapeResource, boolean isRequired, boolean isArrayItem) {
		final String key = URLEncoder.encode(entry.getKey());
		return handleDatatypeProperty(propID, entry.getValue(), key, model, schemaPID, nodeShapeResource, isRequired, isArrayItem);
	}
	
	private String capitaliseNodeIdentifier(String propID) {
		int lastSlash = propID.lastIndexOf('-');
		String stringAfterSlash = propID.substring(lastSlash + 1);
		char firstChar = Character.toUpperCase(stringAfterSlash.charAt(0));
		return propID + "-" + firstChar + stringAfterSlash.substring(1);
	}

	private boolean isLangString(Entry <String, JsonNode> entry) {
		return isLangString(entry.getValue());
	}
	
	private boolean isLangString(JsonNode entry) {
		if(entry.has("@type")) {			
			return entry.get("@type").asText().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString");
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
		
		return hasObjectItems(entry.getValue());
	}
	
	private boolean hasObjectItems(JsonNode entry) {
		
		return (entry.has("items") && entry.get("items").has("type") 
			 && entry.get("items").get("type").asText().equals("object"));
	}
	
	private void handleArray(String propID, JsonNode entry, String key, String schemaPID, Model model, Map<String, JsonNode> definitions, String schemaPath, String instancePath) {
		String propIDCapitalised = capitaliseNodeIdentifier(propID);
		Resource nodeShapeResource = model.createResource(schemaPID + "#" + propIDCapitalised);
		
		Resource propertyShape = null;
		if(isLangString(entry)) {
			propertyShape = handleDatatypeProperty(propIDCapitalised, entry, key, model, schemaPID, nodeShapeResource, false, true);
		}					
		else {
			propertyShape = addObjectProperty(propIDCapitalised + "-" + key, entry, model, schemaPID,
					schemaPID + "#" + propIDCapitalised + "-" + key +"-" + StringUtils.capitalise(key), key);
			
			propertyShape.addLiteral(MSCR.schemaPath, model.createLiteral(schemaPath));
			propertyShape.addLiteral(MSCR.instancePath, model.createLiteral(instancePath));
			
			if(entry.has("@id")) {
				propertyShape.addProperty(MSCR.qname, model.createResource(entry.get("@id").asText()));			
			}
			if(entry.has("namespace")) {
				propertyShape.addProperty(MSCR.namespace, model.createResource(entry.get("namespace").asText()));			
			}

			nodeShapeResource.addProperty(SH.property, propertyShape);
			
			if (hasObjectItems(entry)) {
				handleObject(propIDCapitalised + "-" + key, entry.get("items"), schemaPID, model, definitions, schemaPath, instancePath, key);
			}
			else {
				if(!entry.has("items")) {
					logger.warn("Array property " + key + " does not have any items. Skipping.");
				}
				else {
					Entry<String, JsonNode> arrayItem = Map.entry(key, entry.get("items"));
					propertyShape.removeAll(DCTerms.type); // TODO: remove this hack
					propertyShape.removeAll(SH.node);
					propertyShape = handleDatatypeProperty(propIDCapitalised, arrayItem, model, schemaPID, nodeShapeResource, false, true);					
				}
			}							
			if(entry.get("maxItems") != null && entry.get("maxItems").asText(null) != null &&  !entry.get("maxItems").asText().equals("unbounded")) {
				propertyShape.addLiteral(SH.maxCount, model.createTypedLiteral(entry.get("maxItems").asInt()));
			}								
		}
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
	public void handleObject(String propID, JsonNode node, String schemaPID, Model model, Map<String, JsonNode> definitions, String schemaPath, String instancePath) {
		handleObject(propID, node, schemaPID, model, definitions, schemaPath, instancePath, null); 
	}

	public void handleObject(String propID, JsonNode node, String schemaPID, Model model, Map<String, JsonNode> definitions, String schemaPath, String instancePath, String propKey) {
		String propIDCapitalised = capitaliseNodeIdentifier(propID);		
		String nameProperty = propID.substring(propID.lastIndexOf("-") + 1);		
		Resource nodeShapeResource = model.createResource(schemaPID + "#" + propIDCapitalised);
		
		
		nodeShapeResource.addProperty(RDF.type, (SH.NodeShape));
		
		if(schemaPID.indexOf(":definition") < 0) {
			if(node.has("title")) {
				String nodeTitle = node.get("title").textValue(); 
				nodeShapeResource.addProperty(SH.name, nodeTitle);
				nodeShapeResource.addProperty(MSCR.localName, nodeTitle);
			}
			else {
				if(propKey != null) {
					propKey = URLDecoder.decode(propKey);
					nodeShapeResource.addProperty(SH.name, propKey);
					nodeShapeResource.addProperty(MSCR.localName, propKey);
					
				}
				else {
					nodeShapeResource.addProperty(SH.name, nameProperty);
					nodeShapeResource.addProperty(MSCR.localName, nameProperty);
					
				}
			}
		}
		
		
		
		if (node == null || (node.get("properties") == null && node.get("items") == null)) 
			return;
		if (node.has("description"))
			nodeShapeResource.addProperty(SH.description, node.get("description").asText());
		if (node.has("additionalProperties"))
			nodeShapeResource.addProperty(SH.closed, model.createTypedLiteral(!node.get("additionalProperties").asBoolean()));
		if(node.has("namespace")) {
			nodeShapeResource.addProperty(MSCR.namespace, model.createResource(node.get("namespace").asText()));			
		}
		JsonNode type = node.get("type");

		
		/*
		 * Iterate over properties If a property is an array or object – add and
		 * recursively iterate over them. If a property is a datatype or literal – it's just added.
		 */
		
		Iterator<Entry<String, JsonNode>> propertiesIterator = null;
		if(node.has("properties")) {
			propertiesIterator = node.get("properties").fields();	
		}
		else {
			if(node.get("items").has("properties")) {
				propertiesIterator = node.get("items").get("properties").fields();	
			}
			
		}
		
		if(propertiesIterator != null) {
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
				
				
				String newSchemaPath = schemaPath + "." + entry.getKey();
				String newInstancePath = instancePath + "." + entry.getKey();		
				
				if (valueType.equals("object")) {
					propertyShape = addObjectProperty(propIDCapitalised + "-" + key, entry.getValue(), model, schemaPID,
							schemaPID + "#" + propIDCapitalised + "-" + key +"-" + StringUtils.capitalise(key), key);
					if(entry.getValue().has("@id")) {
						propertyShape.addProperty(MSCR.qname, model.createResource(entry.getValue().get("@id").asText()));			
					}
					if(entry.getValue().has("namespace")) {
						propertyShape.addProperty(MSCR.namespace, model.createResource(entry.getValue().get("namespace").asText()));			
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
					handleObject(propIDCapitalised + "-" + key, entry.getValue(), schemaPID, model,definitions, newSchemaPath, newInstancePath, entry.getKey());	
				}
				else if (valueType.equals("array")) {
					newInstancePath = newInstancePath + "[*]";
					
					handleArray(propID, entry.getValue(), key, schemaPID, model, definitions, newSchemaPath, newInstancePath);
				}
				else {
					boolean isRequired = (entry.getValue().has("required") && (entry.getValue().get("required").asBoolean() == true));								
					propertyShape = handleDatatypeProperty(propIDCapitalised, entry, model, schemaPID, nodeShapeResource, isRequired, false);
					
					if(entry.getValue().has("sourceType")) {
						propertyShape.addProperty(MSCR.sourceType, MSCR.sourceTypeAttribute);
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
					if(entry.getValue().has("@id")) {
						propertyShape.addProperty(MSCR.qname, model.createResource(entry.getValue().get("@id").asText()));			
					}
					if(entry.getValue().has("namespace")) {
						propertyShape.addProperty(MSCR.namespace, model.createResource(entry.getValue().get("namespace").asText()));			
					}
	
					
				}
				if(propertyShape != null) {
					if(entry.getValue().has("schemaPath")) {
						propertyShape.addLiteral(MSCR.schemaPath, model.createLiteral(entry.getValue().get("schemaPath").asText() ));
					}
					else {
						propertyShape.addLiteral(MSCR.schemaPath, model.createLiteral(newSchemaPath));	
					}
					if(entry.getValue().has("instancePath")) {
						propertyShape.addLiteral(MSCR.instancePath, model.createLiteral(entry.getValue().get("instancePath").asText() ));
					}
					else {
						propertyShape.addLiteral(MSCR.instancePath, model.createLiteral(newInstancePath));	
					}					
				}

				
				
				if(entry.getValue().get("order") != null) {
					propertyShape.addLiteral(SH.order, ResourceFactory.createTypedLiteral(entry.getValue().get("order").asInt()));
				}
				if(entry.getValue().get("depth") != null) {
					propertyShape.addLiteral(MSCR.depth, ResourceFactory.createTypedLiteral(entry.getValue().get("depth").asInt()));
				}
	
				if (entry.getValue().get("$ref") != null) {
					String ref = entry.getValue().get("$ref").asText();
					// TODO: set the class and datatype according to the references definition
					String shapeName = "";
					if(ref.indexOf("-") >= 0) {
						shapeName = ref.substring(ref.lastIndexOf("-")+1);
					}
					else if(ref.indexOf("/") >= 0) {
						shapeName = ref.substring(ref.lastIndexOf("/")+1);
					}
					 
					JsonNode defObj = definitions.get(shapeName);
					if(defObj != null) {
						String targetType = defObj.get("type") != null ? defObj.get("type").asText() : "string";
						propertyShape.removeAll(SH.datatype);
						propertyShape.removeAll(DCTerms.type);
						if(targetType.equals("object")) {
													
							propertyShape.addProperty(SH.node, model.createResource(schemaPID + ":definition#" + shapeName + "-" +  shapeName));
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
			handleObject(defName, defs.get(defName), schemaPID + ":definition", m, defs, "", "", defName);
			//m.write(System.out, "TURTLE");
			model.add(m);
		}
		
		
	}
}
