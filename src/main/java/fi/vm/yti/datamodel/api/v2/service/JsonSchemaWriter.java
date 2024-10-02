package fi.vm.yti.datamodel.api.v2.service;

import java.io.StringWriter;
import java.net.URLDecoder;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.sparql.resultset.ResultSetPeekable;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;
import org.apache.jena.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;

import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import jakarta.annotation.Nonnull;

/*
 * Licensed under the European Union Public Licence (EUPL) V.1.1
 */
import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonValue;
import jakarta.json.JsonWriter;
import jakarta.json.JsonWriterFactory;
import jakarta.json.stream.JsonGenerator;

@Service
public class JsonSchemaWriter {

	private final JenaService jenaService;

	private static final Logger logger = LoggerFactory.getLogger(JsonSchemaWriter.class.getName());

	// private JsonWriterFactory jsonWriterFactory;

	JsonWriterFactory jsonWriterFactory() {
		Map<String, Object> config = new HashMap<>();
		config.put(JsonGenerator.PRETTY_PRINTING, true);
		return Json.createWriterFactory(config);
	}

	public JsonSchemaWriter(JenaService jenaService) {
		this.jenaService = jenaService;
	}

	public static final Map<String, String> PREFIX_MAP = Collections.unmodifiableMap(new HashMap<>() {
		{
			put("owl", "http://www.w3.org/2002/07/owl#");
			put("xsd", "http://www.w3.org/2001/XMLSchema#");
			put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
			put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
			put("foaf", "http://xmlns.com/foaf/0.1/");
			put("dcterms", "http://purl.org/dc/terms/");
			put("adms", "http://www.w3.org/ns/adms#");
			put("dc", "http://purl.org/dc/elements/1.1/");
			put("void", "http://rdfs.org/ns/void#");
			put("sd", "http://www.w3.org/ns/sparql-service-description#");
			put("text", "http://jena.apache.org/text#");
			put("sh", "http://www.w3.org/ns/shacl#");
			put("iow", "http://uri.suomi.fi/datamodel/ns/iow#");
			put("skos", "http://www.w3.org/2004/02/skos/core#");
			put("prov", "http://www.w3.org/ns/prov#");
			put("dcap", "http://purl.org/ws-mmi-dc/terms/");
			put("afn", "http://jena.hpl.hp.com/ARQ/function#");
			put("schema", "http://schema.org/");
			put("ts", "http://www.w3.org/2003/06/sw-vocab-status/ns#");
			put("dcam", "http://purl.org/dc/dcam/");
			put("at", "http://publications.europa.eu/ontology/authority/");
			put("skosxl", "http://www.w3.org/2008/05/skos-xl#");
			put("httpv", "http://www.w3.org/2011/http#");
		}
	});

	private static final Map<String, String> DATATYPE_MAP = Collections.unmodifiableMap(new HashMap<>() {
		{
			put("http://www.w3.org/2001/XMLSchema#int", "integer");
			put("http://www.w3.org/2001/XMLSchema#integer", "integer");
			put("http://www.w3.org/2001/XMLSchema#long", "integer");
			put("http://www.w3.org/2001/XMLSchema#float", "number");
			put("http://www.w3.org/2001/XMLSchema#double", "number");
			put("http://www.w3.org/2001/XMLSchema#decimal", "number");
			put("http://www.w3.org/2001/XMLSchema#boolean", "boolean");
			put("http://www.w3.org/2001/XMLSchema#date", "string");
			put("http://www.w3.org/2001/XMLSchema#dateTime", "string");
			put("http://www.w3.org/2001/XMLSchema#time", "string");
			put("http://www.w3.org/2001/XMLSchema#gYear", "string");
			put("http://www.w3.org/2001/XMLSchema#gMonth", "string");
			put("http://www.w3.org/2001/XMLSchema#gDay", "string");
			put("http://www.w3.org/2001/XMLSchema#string", "string");
			put("http://www.w3.org/2001/XMLSchema#anyURI", "string");
			put("http://www.w3.org/2001/XMLSchema#hexBinary", "string");
			put("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString", "langString");
			put("http://www.w3.org/2000/01/rdf-schema#Literal", "string");
		}
	});

	private static final Map<String, String> FORMAT_MAP = Collections.unmodifiableMap(new HashMap<>() {
		{
			put("http://www.w3.org/2001/XMLSchema#dateTime", "date-time");
			put("http://www.w3.org/2001/XMLSchema#date", "date");
			put("http://www.w3.org/2001/XMLSchema#time", "time");
			put("http://www.w3.org/2001/XMLSchema#anyURI", "uri");
		}
	});

	public String jsonObjectToPrettyString(JsonObject object) {
		StringWriter stringWriter = new StringWriter();
		JsonWriter writer = jsonWriterFactory().createWriter(stringWriter);
		writer.writeObject(object);
		writer.close();
		return stringWriter.getBuffer().toString();
	}

	public JsonArray getSchemeValueList(String schemeID, Model model) {
		JsonArrayBuilder builder = Json.createArrayBuilder();
		/*
		 * ParameterizedSparqlString pss = new ParameterizedSparqlString();
		 * 
		 * String selectList = "SELECT ?value " + "WHERE { " + "GRAPH ?scheme { " +
		 * "?code dcterms:identifier ?value . " + "} " + "} ORDER BY ?value";
		 * 
		 * pss.setIri("scheme", schemeID); pss.setNsPrefixes(PREFIX_MAP);
		 * pss.setCommandText(selectList);
		 * 
		 * try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(),
		 * model)) {
		 * 
		 * ResultSet results = qexec.execSelect();
		 * 
		 * if (!results.hasNext()) return null;
		 * 
		 * while (results.hasNext()) { QuerySolution soln = results.next(); if
		 * (soln.contains("value")) { builder.add(soln.getLiteral("value").getString());
		 * } }
		 * 
		 * }
		 */
		return builder.build();
	}

	public JsonArray getValueList(Model model, String classID, String propertyID) {
		JsonArrayBuilder builder = Json.createArrayBuilder();
		/*
		 * ParameterizedSparqlString pss = new ParameterizedSparqlString();
		 * 
		 * String selectList =
		 * 
		 * 
		 * pss.setIri("resource", classID); pss.setIri("property", propertyID);
		 * pss.setNsPrefixes(PREFIX_MAP); pss.setCommandText(selectList);
		 * 
		 * try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(),
		 * model)) {
		 * 
		 * ResultSet results = qexec.execSelect();
		 * 
		 * if (!results.hasNext()) return null;
		 * 
		 * while (results.hasNext()) { QuerySolution soln = results.next(); if
		 * (soln.contains("value")) { builder.add(soln.getLiteral("value").getString());
		 * } } }
		 */
		return builder.build();
	}

	/*
	 * Ways to describe codelists, by "type"-list.
	 * 
	 * { type:[ {enum:["22PC"], description:"a description for the first enum"},
	 * {enum:["42GP"], description:"a description for the second enum"},
	 * {enum:["45GP"], description:"a description for the third enum"},
	 * {enum:["45UP"], description:"a description for the fourth enum"},
	 * {enum:["22GP"], description:"a description for the fifth enum"} ] }
	 * 
	 * or by using custom parameters:
	 * 
	 * enum:[1,2,3], options:[{value:1,descrtiption:"this is one"},{value:2,
	 * description:"this is two"}],
	 * 
	 * 
	 */

	public List<String> getModelRoots(String graph, Model model) {

		ParameterizedSparqlString pss = new ParameterizedSparqlString();
		String selectResources = "SELECT ?root WHERE {" + "?graph void:rootResource ?root . " + "}";

		pss.setNsPrefixes(PREFIX_MAP);
		pss.setCommandText(selectResources);
		pss.setIri("graph", graph);

		List<String> roots = new ArrayList<String>();
		try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(), model)) {

			ResultSet results = qexec.execSelect();

			if (!results.hasNext())
				return roots;
			else {
				while (results.hasNext()) {
					QuerySolution soln = results.next();
					if (soln.contains("root")) {
						roots.add(soln.getResource("root").toString());
					}

				}
			}
		}
		return roots;
	}

	private void handleProperties(Resource node, Model model, JsonObjectBuilder properties, JsonObjectBuilder definitions) {
		NodeIterator propi = model.listObjectsOfProperty(node, SH.property);
		while (propi.hasNext()) {				
			Resource propRes = propi.next().asResource();
			String refKey = propRes.getURI().replace("/", "-");
			String propName = propRes.getProperty(SH.name).getString();
			if (propRes.getPropertyResourceValue(DCTerms.type).equals(OWL.ObjectProperty)) {
				// add ref
				String targetDef = propRes.getPropertyResourceValue(SH.path).getURI().replace("/", "-");
				properties.add(refKey, Json.createObjectBuilder().add("$ref", "#/definitions/" + targetDef).build());
			} else {
				// add property
				JsonObjectBuilder prop = Json.createObjectBuilder();
				prop.add("@id", propRes.getURI());
				prop.add("title", propName);				
				String pqname = propRes.getProperty(MSCR.qname) != null ? propRes.getRequiredProperty(MSCR.qname).getResource().getURI(): propRes.getURI().substring(propRes.getURI().lastIndexOf("/")+1);
				String pnamespace = propRes.getProperty(MSCR.namespace) != null ? propRes.getProperty(MSCR.namespace).getObject().asResource().getURI(): null;
				Integer pmaxCount = propRes.getProperty(SH.maxCount) != null && !propRes.getProperty(SH.maxCount).getLiteral().getDatatypeURI().equals("http://www.w3.org/2001/XMLSchema#string") ? propRes.getProperty(SH.maxCount).getInt() : null;
				Integer pminCount = propRes.getProperty(SH.minCount) != null ? propRes.getProperty(SH.minCount).getInt() : null;
				String datatype = propRes.getProperty(SH.datatype).getResource().getURI();
				prop.add("qname", pqname);
				if (pmaxCount != null) {
					prop.add("maxCount", ""+pmaxCount);
				}
				if (pminCount != null) {
					prop.add("minCount", ""+pminCount);
				}
				if(pnamespace != null) {
					prop.add("namespace", pnamespace);
				}
				prop.add("@type", datatype);
				String jsonDatatype = DATATYPE_MAP.get(datatype);
				if (jsonDatatype == null) {
					jsonDatatype = getDTRDatatype(datatype);
				}
				prop.add("type", jsonDatatype);

				// enum
				// pattern
				// maxLength
				// minLength
				
				JsonObject propObj = prop.build();
				properties.add(refKey, propObj);
				definitions.add(refKey, propObj);
			}				
		}
	}
	public JsonObjectBuilder getClassDefinitions(String modelID, Model model, String lang) {
		// generate definitions for each propertyShape/NodeShape pair
		JsonObjectBuilder definitions = Json.createObjectBuilder();

		ResIterator pi = model.listSubjectsWithProperty(DCTerms.type, OWL.ObjectProperty);
		while (pi.hasNext()) {
			Resource objectPropRes = pi.next();

			JsonObjectBuilder def = Json.createObjectBuilder();
			

			String qname = objectPropRes.getProperty(MSCR.qname) != null ? objectPropRes.getProperty(MSCR.qname).getObject().asResource().getURI() : objectPropRes.getURI().substring(objectPropRes.getURI().lastIndexOf("/")+1);
			String namespace = objectPropRes.getProperty(MSCR.namespace) != null ? objectPropRes.getProperty(MSCR.namespace).getObject().asResource().getURI(): null;
			Integer maxCount = objectPropRes.getProperty(SH.maxCount) != null && !objectPropRes.getProperty(SH.maxCount).getLiteral().getDatatypeURI().equals("http://www.w3.org/2001/XMLSchema#string") ? objectPropRes.getProperty(SH.maxCount).getInt() : null;
			Integer minCount = objectPropRes.getProperty(SH.minCount) != null ? objectPropRes.getProperty(SH.minCount).getInt() : null;
			String name = objectPropRes.getProperty(SH.name).getString();

			def.add("@id", objectPropRes.getURI());
			def.add("type", "object");
			def.add("title", name);
			def.add("qname", qname);
			if (maxCount != null) {
				def.add("maxCount", ""+maxCount);
			}
			if (minCount != null) {
				def.add("minCount", ""+minCount);
			}
			if(namespace != null) {
				def.add("namespace", namespace);
			}
			
			JsonObjectBuilder properties = Json.createObjectBuilder();
			Resource node = objectPropRes.getPropertyResourceValue(SH.node);
			handleProperties(node, model, properties, definitions);
			
			def.add("properties", properties.build());
			definitions.add(objectPropRes.getURI().replace("/", "-"), def);
		}
		// add root last
		
		Resource rootResource = model.getResource(modelID + "#root/Root");
		JsonObjectBuilder rootProperties = Json.createObjectBuilder();
		JsonObjectBuilder rootDef = Json.createObjectBuilder();
		rootDef.add("type", "object");
		rootDef.add("title", "root");	
		handleProperties(rootResource, model, rootProperties, definitions);
		rootDef.add("properties", rootProperties);
		definitions.add(modelID + "#root-Root", rootDef.build());
		return definitions;

	}

	private String getDTRDatatype(String datatype) {
		Model m = jenaService.getSchema(datatype);
		return MapperUtils.propertyToString(m.getResource(datatype), m.getProperty("mscr:jsonschema:type"));
	}

	/**
	 * Removes invalid characters from resource names
	 *
	 * @param name resource name
	 * @return stripped resource name
	 */
	public static String removeInvalidCharacters(String name) {
		name = removeAccents(name);
		name = name.replaceAll("[^a-zA-Z0-9_-]", "");
		return name;
	}

	/**
	 * Removes accents from string
	 *
	 * @param text input text
	 * @return stripped text
	 */
	public static String removeAccents(@Nonnull String text) {
		return Normalizer.normalize(text, Form.NFD).replaceAll("\\p{InCombiningDiacriticalMarks}+", "");
	}

	public String newModelSchema(String modelID, Model model, String lang) {

		JsonObjectBuilder schema = Json.createObjectBuilder();

		ParameterizedSparqlString pss = new ParameterizedSparqlString();

		String selectClass = "SELECT ?label ?description " + "WHERE { " + "?modelID rdfs:label ?label . "
				+ "FILTER (langMatches(lang(?label),?lang))" + "OPTIONAL { ?modelID rdfs:comment ?description . "
				+ "FILTER (langMatches(lang(?description),?lang))" + "} " + "} ";

		pss.setIri("modelID", modelID);
		if (lang != null)
			pss.setLiteral("lang", lang);
		pss.setNsPrefixes(PREFIX_MAP);

		pss.setCommandText(selectClass);

		try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(), model)) {

			ResultSet results = qexec.execSelect();

			if (!results.hasNext()) {
				logger.debug("No results from model: " + modelID);
				return null;
			}

			while (results.hasNext()) {

				QuerySolution soln = results.nextSolution();
				String title = soln.getLiteral("label").getString();

				logger.info("Building JSON Schema from " + title);

				if (soln.contains("description")) {
					String description = soln.getLiteral("description").getString();
					schema.add("description", description);
				}

				if (!modelID.endsWith("/") || !modelID.endsWith("#"))
					schema.add("@id", modelID + "#");
				else
					schema.add("@id", modelID);

				schema.add("title", title);

				Date modified = new Date(); // graphManager.modelContentModified(modelID);
				SimpleDateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");

				if (modified != null) {
					String dateModified = format.format(modified);
					schema.add("modified", dateModified);
				}

			}

			JsonObjectBuilder definitions = getClassDefinitions(modelID, model, lang);

			List<String> modelRoots = getModelRoots(modelID, model);

			if (modelRoots.size() == 0) {
				throw new RuntimeException("Schema is missing root element");
			}

			JsonObjectBuilder modelProperties = Json.createObjectBuilder();
//                modelProperties.add("$ref", "#/definitions/" + SplitIRI.localname(modelRoot));
			modelProperties.add("$ref", "#/definitions/Root");

			String r = createModelSchemaWithRoot(schema, modelProperties, definitions, modelRoots);
			return r;

		}
	}

	public JsonObject getLangStringObject() {

		/*
		 * Regexp for validating language codes ? For example: "langString":{
		 * "type":"object",
		 * "patternProperties":{"^[a-z]{2,3}(?:-[A-Z]{2,3}(?:-[a-zA-Z]{4})?)?$":{"type":
		 * "string"}}, "additionalProperties":false
		 * 
		 * }
		 */

		JsonObjectBuilder builder = Json.createObjectBuilder();
		JsonObjectBuilder add = Json.createObjectBuilder();
		builder.add("type", "object");
		builder.add("title", "Multilingual string");
		builder.add("description", "Object type for localized strings");
		builder.add("additionalProperties", add.add("type", "string").build());
		return builder.build();
	}

	private String createModelSchemaWithRoot(JsonObjectBuilder schema, JsonObjectBuilder properties,
			JsonObjectBuilder definitions, List<String> roots) {

		schema.add("$schema", "http://json-schema.org/draft-04/schema#");

		JsonObject definitionsObj = null;
		if (definitions != null) {
			definitions.add("langString", getLangStringObject());
			definitionsObj = definitions.build();

			schema.add("definitions", definitionsObj);

		}

		if (roots.size() == 1) {
			schema.add("type", "object");
			if (definitionsObj != null) {
				String rootDefinitionCandidate = roots.get(0);
				String rootDefinition = rootDefinitionCandidate.replace("/", "-");
				// String lastPart =
				// rootDefinitionCandidate.substring(rootDefinitionCandidate.lastIndexOf("/")+1);
				// if(lastPart.equals("Root")) {
				// rootDefinition = "Root";
				// }
				/*
				 * if(Character.isUpperCase(rootDefinition.charAt(0))) {
				 * 
				 * rootDefinition = rootDefinitionCandidate.substring(0,
				 * rootDefinitionCandidate.lastIndexOf("/")); }
				 */
				JsonObject rootObj = definitionsObj.getJsonObject(rootDefinition);
				if (rootObj == null) {
					rootDefinition = rootDefinitionCandidate.substring(rootDefinitionCandidate.lastIndexOf("/") + 1);
					rootObj = definitionsObj.getJsonObject(rootDefinition);
				}
				if (rootObj.containsKey("$ref")) {
					rootObj = definitionsObj.getJsonObject(rootObj.getString("$ref").substring(14));
				}

				if (rootObj.getString("type").equals("array")) {
					// TODO: fix this
					throw new RuntimeException("Not implemented yet! Root element of type array.");
				}

				JsonValue props = rootObj.get("properties");
				if (props != null) {
					schema.add("properties", props.asJsonObject());
				}

			}
		} else {
			throw new RuntimeException(
					"Jsonschemawriter requires exactly on root element identified by void:rootResource. Number of root elements detected: "
							+ roots.size());
		}

		// schema.add("oneOf",
		// Json.createArrayBuilder().add(properties.build()).build());

		return jsonObjectToPrettyString(schema.build());
	}

	private List<Resource> getLeafConcepts(Model inputModel) {
		List<Resource> r = new ArrayList<Resource>();

		String queryString = """
				prefix skos: <http://www.w3.org/2004/02/skos/core#>
				select ?uri
				where {
				  ?uri a skos:Concept
				  minus {
				    ?uri skos:narrower ?other .
				    ?other2 skos:broader ?uri .
				  }
				}

				""";
		Query qry = QueryFactory.create(queryString);
		QueryExecution qe = QueryExecutionFactory.create(qry, inputModel);
		ResultSet rs = qe.execSelect();
		while (rs.hasNext()) {
			QuerySolution qs = rs.next();
			r.add(inputModel.getResource(qs.get("uri").toString()));
		}
		return r;
	}

	private String getPrefLabel(Resource r) {

		if (r.getProperty(SKOS.prefLabel, "en") != null) {
			return r.getRequiredProperty(SKOS.prefLabel, "en").getString();
		} else if (r.getProperty(SKOS.prefLabel) != null) {
			return r.getProperty(SKOS.prefLabel).getString();
		} else if (r.getLocalName() != null && !r.getLocalName().equals("")) {
			return r.getLocalName();
		} else {
			return r.getURI();
		}
	}

	private String getLocalName(Resource r) {
		if (r.getLocalName() != null && !r.getLocalName().equals("")) {
			return r.getLocalName();
		} else if (!r.getURI().substring(r.getURI().lastIndexOf("/") + 1).equals("")) {
			return r.getURI().substring(r.getURI().lastIndexOf("/") + 1);
		} else {
			return r.getURI();
		}
	}

	private boolean isTopConcept(Resource r, Model model) {
		return !r.hasProperty(SKOS.broader) && !model.listSubjectsWithProperty(SKOS.narrower, r).hasNext();

	}

	private Resource getParent(Resource concept, Model model) {
		if (concept.hasProperty(SKOS.broader)) {
			return concept.getPropertyResourceValue(SKOS.broader);
		} else if (model.listSubjectsWithProperty(SKOS.narrower, concept).hasNext()) {
			return model.listSubjectsWithProperty(SKOS.narrower, concept).next();
		}
		return null;
	}

	private List<Resource> getChildren(Resource concept, Model model) {
		List<Resource> children = new ArrayList<Resource>();
		ResIterator i = model.listSubjectsWithProperty(SKOS.broader, concept);
		NodeIterator i2 = model.listObjectsOfProperty(concept, SKOS.narrower);
		while (i.hasNext()) {
			children.add(i.next());
		}
		while (i2.hasNext()) {
			Resource candidate = i2.next().asResource();
			if (!children.contains(candidate)) {
				children.add(candidate);
			}

		}

		return children;
	}

	private Map<String, Object> handleConcept(Resource concept, Model model, Map<String, Object> definitions) {
		String prefLabel = getPrefLabel(concept);
		String description = MapperUtils.propertyToString(concept, SKOS.definition);
		if (description == null) {
			description = "";
		}
		String uri = concept.getURI();
		String localName = getLocalName(concept);
		Map<String, Object> o = new HashMap<String, Object>();

		o.put("@id", uri);
		o.put("title", prefLabel);
		o.put("description", description);
		if (hasChildren(concept, model)) {
			o.put("type", "object");
		} else {
			o.put("type", "string");
		}

		o.put("@type", "http://www.w3.org/2001/XMLSchema#anyURI");

		if (model.qnameFor(uri) != null) {
			o.put("qname", model.qnameFor(uri));
		} else {
			o.put("qname", ":" + concept.getLocalName());
		}

		if (!definitions.containsKey(localName)) {
			definitions.put(localName, o);
		}

		return o;

	}

	private boolean hasChildren(Resource r, Model model) {
		return r.hasProperty(SKOS.narrower) && !model.listSubjectsWithProperty(SKOS.narrower, r).hasNext();

	}

	private void traverseUp(Resource r, Model inputModel, Map<String, Object> definitions,
			Map<String, Object> rootProps) throws Exception {
		// Add n to A to maintain bottom up nature
		if (r == null)
			return;
		// Go to parent
		Resource parent = getParent(r, inputModel);
		if (parent == null) {
			// we are at a top concept
			String localName = getLocalName(r);
			Object obj = definitions.get(localName);
			if (obj == null) {
				obj = handleConcept(r, inputModel, definitions);
			}
			rootProps.put(localName, obj);
			return;
		}

		List<Resource> children = getChildren(parent, inputModel);
		boolean hasChildren = children.size() > 0;
		Map<String, Object> parentObj = handleConcept(parent, inputModel, definitions);
		Map<String, Object> props = new HashMap<String, Object>();
		// For each child of p other than n, do a post order traversal

		for (Resource child : children) {
			handleConcept(child, inputModel, definitions);
			// add properties to parent'
			String localName = getLocalName(child);
			Map<String, Object> ref = new HashMap<String, Object>();
			ref.put("$ref", "#/definitions/" + localName);
			props.put(localName, ref);
		}
		parentObj.put("properties", props);
		// When done with adding all p's children, continue traversing up
		traverseUp(parent, inputModel, definitions, rootProps);
	}

	private void traverseDown(Resource r, Model inputModel, Map<String, Object> definitions,
			Map<String, Object> rootProps) throws Exception {

		String localName = getLocalName(r);
		Map<String, Object> obj = handleConcept(r, inputModel, definitions);
		rootProps.put(localName, obj);
		Map<String, Object> props = new HashMap<String, Object>();

		List<Resource> children = getChildren(r, inputModel);
		for (Resource child : children) {
			handleConcept(child, inputModel, definitions);
			String childlocalName = getLocalName(child);
			Map<String, Object> ref = new HashMap<String, Object>();
			ref.put("$ref", "#/definitions/" + childlocalName);
			props.put(childlocalName, ref);
			traverseDown(child, inputModel, definitions, props);
		}
		obj.put("properties", props);
	}

	public String skosSchema(String pid, Model model, String string) throws Exception {
		Resource metadataResource = model.getResource(pid);
		Resource rootConcept = metadataResource.getPropertyResourceValue(VOID.rootResource);

		List<Resource> leafs = getLeafConcepts(model);
		Map<String, Object> definitions = new HashMap<String, Object>();

		Map<String, Object> rootDefinition = new HashMap<String, Object>();
		Map<String, Object> rootProperties = new HashMap<String, Object>();
		rootDefinition.put("properties", rootProperties);
		definitions.put("Root", rootDefinition);

		Map<String, Object> schema = new HashMap<String, Object>();
		if (rootConcept == null) {
			for (Resource leaf : leafs) {
				traverseUp(leaf, model, definitions, rootProperties);
			}

		} else {
			traverseDown(rootConcept, model, definitions, rootProperties);
		}
		schema.put("definitions", definitions);
		schema.put("$schema", "http://json-schema.org/draft-04/schema#");
		schema.put("type", "object");

		schema.put("properties", rootProperties);

		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsString(schema);
	}

	private void addRDFSProps(Model model, Resource s, Map<String, Object> props, Map<String, Object> definitions)
			throws Exception {
		Resource parent = s.getPropertyResourceValue(RDFS.subClassOf);
		if (parent != null) {
			try {
				addRDFSProps(model, parent, props, definitions);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		model.listSubjectsWithProperty(RDFS.domain, s).forEach(ps -> {
			String psID = ps.getURI();
			Map<String, Object> psProps = new LinkedHashMap<String, Object>();

			String range = RDFS.Literal.getURI(); // default range
			if (ps.hasProperty(RDFS.range)) {
				range = ps.getPropertyResourceValue(RDFS.range).getURI();
			}

			Map<String, String> titles = MapperUtils.localizedPropertyToMap(ps, RDFS.label);
			Map<String, String> descs = MapperUtils.localizedPropertyToMap(ps, RDFS.comment);

			if (titles.isEmpty()) {
				titles.put("en", psID);
			}
			psProps.put("datatype", range);
			psProps.put("description", descs.get("en"));
			psProps.put("title", titles.get("en"));
			psProps.put("@id", psID);
			if (model.qnameFor(psID) != null) {
				psProps.put("qname", model.qnameFor(psID));
			} else {
				psProps.put("qname", ":" + ps.getLocalName());
			}
			definitions.put(psID, psProps);
			props.put(psID, psProps);
		});

	}
	
	private void addClassMappingSourceProps(String classURI, Map<String, Object> classProps, Map<String, Object> definitions) {
		Map<String, Object> iteratorProp = new LinkedHashMap();
		iteratorProp.put("title", "iterator source");
		iteratorProp.put("type", "string");	
		String iteratorPropID = "iterator:" + classURI;
		iteratorProp.put("qname", iteratorPropID);
		iteratorProp.put("@id", iteratorPropID);
		classProps.put(iteratorPropID, iteratorProp);
		definitions.put(iteratorPropID, iteratorProp);

		Map<String, Object> subjectProp = new LinkedHashMap();
		subjectProp.put("title", "subject source");
		subjectProp.put("type", "string");
		String subjectPropID = "subject:" + classURI;
		subjectProp.put("qname", subjectPropID);
		subjectProp.put("@id", subjectPropID);
		classProps.put(subjectPropID, subjectProp);
		definitions.put(subjectPropID, subjectProp);		
	}

	public String rdfs(String pid, Model model, String string) throws Exception {
		Map<String, Object> definitions = new HashMap<String, Object>();

		Map<String, Object> rootDefinition = new HashMap<String, Object>();
		Map<String, Object> rootProperties = new LinkedHashMap<String, Object>();
		rootDefinition.put("properties", rootProperties);

		Map<String, Object> schema = new HashMap<String, Object>();
		schema.put("definitions", definitions);
		schema.put("$schema", "http://json-schema.org/draft-04/schema#");
		schema.put("type", "object");

		schema.put("properties", rootProperties);

		model.listSubjectsWithProperty(RDF.type, RDFS.Class).forEach(s -> {
			String className = s.getURI();
			if (className == null) {
				// blank node

			} else {

				String qName = model.qnameFor(className);

				Map<String, Object> classDef = new HashMap<String, Object>();
				Map<String, Object> classProps = new LinkedHashMap<String, Object>();			

				addClassMappingSourceProps(className, classProps, definitions);
				
				Map<String, String> titles = MapperUtils.localizedPropertyToMap(s, RDFS.label);
				Map<String, String> descs = MapperUtils.localizedPropertyToMap(s, RDFS.comment);

				classDef.put("title", titles.get("en"));
				classDef.put("description", descs.get("en"));
				classDef.put("qname", qName);
				classDef.put("@id", className);
				rootProperties.put(className, classDef);
				try {
					addRDFSProps(model, s, classProps, definitions);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (classProps.keySet().size() > 0) {
					classDef.put("type", "object");
					classDef.put("@type", model.qnameFor(className));
					classDef.put("properties", classProps);
				} else {
					// what happens here?
				}
				definitions.put(className, classDef);
			}

		});

		ObjectMapper mapper = new ObjectMapper();

		return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
	}

	public String shacl(String pid, Model model, String string) throws Exception {
		Map<String, Object> definitions = new HashMap<String, Object>();

		Map<String, Object> rootDefinition = new HashMap<String, Object>();
		Map<String, Object> rootProperties = new HashMap<String, Object>();
		rootDefinition.put("properties", rootProperties);

		Map<String, Object> schema = new HashMap<String, Object>();
		schema.put("definitions", definitions);
		schema.put("$schema", "http://json-schema.org/draft-04/schema#");
		schema.put("type", "object");
		schema.put("title", "root");
		schema.put("description", "");
		schema.put("properties", rootProperties);
		model.listSubjectsWithProperty(RDF.type, SH.NodeShape).forEach(s -> {
			String shapeID = s.getURI();
			
			//shapeID = shapeID.replace("/", "-");
			Map<String, Object> shapeDef = new LinkedHashMap<String, Object>();
			Map<String, Object> shapeProps = new LinkedHashMap<String, Object>();
			
			
			shapeDef.put("@id", shapeID);
			
			// System.out.println(shapeID);
			// TODO: if sh:desc not found check sh:class and sh:node
			
			// we need to get the name (and qname) of the target class 
			String qname = model.qnameFor(shapeID);
			String title = model.qnameFor(shapeID); // default name
			if(s.hasProperty(SH.targetClass)) {
				Resource targetClass = s.getPropertyResourceValue(SH.targetClass);
				if(targetClass.hasProperty(RDFS.label)) {
					title = targetClass.getProperty(RDFS.label).getLiteral().getString();
				}
				else if (model.qnameFor(targetClass.getURI()) != null) {
					title = model.qnameFor(targetClass.getURI());
				}
				else if (targetClass.getLocalName() != null){
					title = targetClass.getLocalName();
				}
				else {
					title = targetClass.getURI();
				}

				qname = targetClass.getURI();
			}
			else {
				logger.warn("No target class found for node shape " + s.getURI());
				
			}
			addClassMappingSourceProps(shapeID, shapeProps, definitions);

			
			shapeDef.put("qname", qname);
			shapeDef.put("title", title);	
			
			Map<String, String> descs = MapperUtils.localizedPropertyToMap(s, SH.description);
			shapeDef.put("description", descs.get("en"));
			rootProperties.put(shapeID, shapeDef);
			try {
				addSHACLProps(model, s, shapeProps, definitions);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			if (shapeProps.keySet().size() > 0) {
				shapeDef.put("type", "object");
				
				if (s.hasProperty(SH.targetClass)) {
					RDFNode typeNode = s.getProperty(SH.targetClass).getObject();
					if (typeNode.isLiteral()) {
						shapeDef.put("@type", typeNode.asLiteral().getString());
					} else if (typeNode.isResource()) {
						shapeDef.put("@type", model.qnameFor(typeNode.asResource().getURI()));
					}

				}
				//
				shapeDef.put("properties", shapeProps);
			} else {
				// what happens here?
			}
			definitions.put(shapeID, shapeDef);
			
		});

		ObjectMapper mapper = new ObjectMapper();

		return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
	}

	private void addSHACLProps(Model model, Resource s, Map<String, Object> props, Map<String, Object> definitions) {
		model.listObjectsOfProperty(s, SH.property).forEach(_ps -> {
			Resource ps = _ps.asResource();
			String psID = ps.getURI();
			if(ps.isAnon()) {
				psID = s.getURI() + "#" + ps.getId().toString(); //  ps.getPropertyResourceValue(SH.path).getURI();
				//psID = ps.getPropertyResourceValue(SH.path).getURI();
			}
			//psID = psID.replace("/", "-");
			
			

			Map<String, Object> psProps = new LinkedHashMap<String, Object>();
			// System.out.println(ps.getPropertyResourceValue(SH.path).getLocalName());
			// System.out.println(model.getNsURIPrefix(ps.getPropertyResourceValue(SH.path).getNameSpace()));
			psProps.put("qname", ps.getPropertyResourceValue(SH.path).getURI());

			Map<String, String> titles = MapperUtils.localizedPropertyToMap(ps, SH.name);
			Map<String, String> descs = MapperUtils.localizedPropertyToMap(ps, SH.description);

			if (titles.isEmpty()) {
				if (model.qnameFor(ps.getPropertyResourceValue(SH.path).getURI()) != null) {
					psProps.put("title", model.qnameFor(ps.getPropertyResourceValue(SH.path).getURI()));
				} else {
					psProps.put("title", ps.getPropertyResourceValue(SH.path).getLocalName());
				}
			}
			else {
				psProps.put("title", titles.get("en"));
			}
			psProps.put("description", descs.get("en"));
			

			String datatype = "object";
			if (ps.hasProperty(SH.node)) {

				datatype = model.qnameFor(ps.getPropertyResourceValue(SH.node).getURI());
			}else if(ps.hasProperty(SH.class_)) { 
				// see if node shape with a proper target class exists
				ResIterator ti = model.listResourcesWithProperty(SH.targetClass, ps.getPropertyResourceValue(SH.class_));
				if(ti.hasNext()) {
					psProps.put("shape", model.qnameFor(ti.next().getURI()));
				}
				else {
				}
			
			}else if (ps.hasProperty(SH.datatype)) {
				datatype = ps.getPropertyResourceValue(SH.datatype).getLocalName();
			}

			psProps.put("datatype", datatype);			
			psProps.put("@id", psID);
			definitions.put(psID, psProps);
			props.put(psID, psProps);
		});

	}

	public String owl(String pid, Model model, String string) throws Exception {
		Map<String, Object> definitions = new HashMap<String, Object>();

		Map<String, Object> rootDefinition = new HashMap<String, Object>();
		Map<String, Object> rootProperties = new LinkedHashMap<String, Object>();
		rootDefinition.put("properties", rootProperties);

		Map<String, Object> schema = new HashMap<String, Object>();
		schema.put("definitions", definitions);
		schema.put("$schema", "http://json-schema.org/draft-04/schema#");
		schema.put("type", "object");

		schema.put("properties", rootProperties);

		model.listObjectsOfProperty(VOID.rootResource).forEach(obj -> {
			Resource s = (Resource) obj;
			String className = s.getURI();
			if (className == null) {
				// what now?

			} else {

				String qName = model.qnameFor(className);

				Map<String, Object> classDef = new HashMap<String, Object>();
				Map<String, Object> classProps = new LinkedHashMap<String, Object>();
				
				addClassMappingSourceProps(className, classProps, definitions);
				
				Map<String, String> titles = MapperUtils.localizedPropertyToMap(s, RDFS.label);
				Map<String, String> descs = MapperUtils.localizedPropertyToMap(s, RDFS.comment);

				
				classDef.put("title", titles.get("en"));
				classDef.put("description", descs.get("en"));
				classDef.put("qname", qName);
				classDef.put("@id", className);
				rootProperties.put(className, classDef);
				try {
					addRDFSProps(model, s, classProps, definitions);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (classProps.keySet().size() > 0) {
					classDef.put("type", "object");
					classDef.put("@type", model.qnameFor(className));
					classDef.put("properties", classProps);
				} else {
					// what happens here?
				}
				definitions.put(className, classDef);
			}

		});

		ObjectMapper mapper = new ObjectMapper();

		return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);
	}

}
