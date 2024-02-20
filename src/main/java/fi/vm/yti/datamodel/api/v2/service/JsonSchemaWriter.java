package fi.vm.yti.datamodel.api.v2.service;

import java.io.StringWriter;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.jena.query.ParameterizedSparqlString;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.resultset.ResultSetPeekable;
import org.apache.jena.vocabulary.SKOS;
import org.apache.jena.vocabulary.VOID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

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

	private static final Logger logger = LoggerFactory.getLogger(JsonSchemaWriter.class.getName());

	// private JsonWriterFactory jsonWriterFactory;

	JsonWriterFactory jsonWriterFactory() {
		Map<String, Object> config = new HashMap<>();
		config.put(JsonGenerator.PRETTY_PRINTING, true);
		return Json.createWriterFactory(config);
	}

	JsonSchemaWriter() {

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
				return null;
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

	public JsonObjectBuilder getClassDefinitions(String modelID, Model model, String lang) {

		ParameterizedSparqlString pss = new ParameterizedSparqlString();

		String selectResources = "SELECT ?resource ?targetClass ?className ?localClassName ?classTitle ?classDeactivated ?classDescription ?minProperties ?maxProperties ?property ?propertyDeactivated ?valueList ?schemeList ?predicate ?id ?title ?description ?predicateName ?datatype ?shapeRef ?shapeRefName ?min ?max ?minLength ?maxLength ?pattern ?idBoolean ?example \n"
				+ "WHERE {\n" + "?resource a sh:NodeShape . " 
				+ "OPTIONAL { ?resource sh:name ?classTitle . }"
				+ "OPTIONAL { ?resource sh:description ?classDescription } "
				+ "BIND(afn:localname(?resource) as ?className) . "
				+ "OPTIONAL { ?resource iow:localName ?localClassName . } "
				+ "OPTIONAL	{ ?resource sh:property ?property . " 
					+ "OPTIONAL {   ?property sh:path ?predicate . }"
					+ "OPTIONAL {?property sh:name ?title . }\n" 
					+ "OPTIONAL { ?property sh:description ?description . }\n"
					+ "OPTIONAL { ?property sh:datatype ?datatype . }"
					+ "OPTIONAL { ?property sh:node ?shapeRef . BIND(afn:localname(?shapeRef) as ?shapeRefName) }"
					+ "OPTIONAL { ?property sh:maxCount ?max . }" + "OPTIONAL { ?property sh:minCount ?min . }"
					+ "OPTIONAL { ?property sh:pattern ?pattern . }" + "OPTIONAL { ?property sh:minLength ?minLength . }"
					+ "OPTIONAL { ?property sh:maxLength ?maxLength . }" + "OPTIONAL { ?property skos:example ?example . }"
					+ "OPTIONAL { ?property sh:in ?valueList . } " + "BIND(STR(?predicate) as ?predicateName)"				
				+ "} }";

		pss.setIri("modelPartGraph", modelID + "#HasPartGraph");

		if (lang != null) {
			pss.setLiteral("lang", lang);
		}

		pss.setCommandText(selectResources);
		pss.setNsPrefixes(PREFIX_MAP);

		try (QueryExecution qexec = QueryExecutionFactory.create(pss.toString(), model)) {

			ResultSet results = qexec.execSelect();
			ResultSetPeekable pResults = ResultSetFactory.makePeekable(results);

			if (!pResults.hasNext()) {
				return null;
			}

			JsonObjectBuilder definitions = Json.createObjectBuilder();
			JsonObjectBuilder properties = Json.createObjectBuilder();

			JsonObjectBuilder predicate = Json.createObjectBuilder();

			HashSet<String> exampleSet = new HashSet<>();
			HashSet<String> requiredPredicates = new HashSet<>();

			JsonArrayBuilder exampleList = Json.createArrayBuilder();
			JsonObjectBuilder typeObject = Json.createObjectBuilder();

			boolean arrayType = false;

			int pIndex = 1;
			String predicateName = null;
			String predicateID = null;
			String className;

			while (pResults.hasNext()) {
				QuerySolution soln = pResults.nextSolution();

				if (!soln.contains("className")) {
					return null;
				}

				String localClassName = soln.contains("localClassName") ? soln.getLiteral("localClassName").getString()
						: null;

				if (!soln.contains("classDeactivated")
						|| (soln.contains("classDeactivated") && !soln.getLiteral("classDeactivated").getBoolean())) {

					className = soln.getLiteral("className").getString();

					if (soln.contains("property")
							&& (!soln.contains("propertyDeactivated") || (soln.contains("propertyDeactivated")
									&& !soln.getLiteral("propertyDeactivated").getBoolean()))) {

						/* First run per predicate */

						if (pIndex == 1) {

							predicateID = soln.getResource("predicate").toString();

							predicate.add("@id", predicateID);

							predicateName = soln.getLiteral("predicateName").getString();

							if (soln.contains("id")) {
								predicateName = soln.getLiteral("id").getString();
							}

							if (soln.contains("title")) {
								String title = soln.getLiteral("title").getString();
								predicate.add("title", title);
							}

							if (soln.contains("min")) {
								int min = soln.getLiteral("min").getInt();
								if (min > 0) {
									requiredPredicates.add(predicateName);
								}
							}

							if (soln.contains("description")) {
								String description = soln.getLiteral("description").getString();
								predicate.add("description", description);
							}

							if (soln.contains("valueList")) {
								JsonArray valueList = getValueList(model, soln.getResource("resource").toString(),
										soln.getResource("property").toString());
								if (valueList != null) {
									predicate.add("enum", valueList);
								}
							} else if (soln.contains("schemeList")) {
								JsonArray schemeList = getSchemeValueList(soln.getResource("schemeList").toString(),
										model);
								if (schemeList != null) {
									predicate.add("enum", schemeList);
								}
							}

							if (soln.contains("datatype")) {

								String datatype = soln.getResource("datatype").toString();

								if (soln.contains("idBoolean")) {
									Boolean isId = soln.getLiteral("idBoolean").getBoolean();
									if (isId) {
										predicate.add("@type", "@id");
									} else
										predicate.add("@type", datatype);
								} else {
									predicate.add("@type", datatype);
								}

								String jsonDatatype = DATATYPE_MAP.get(datatype);

								if (soln.contains("maxLength")) {
									predicate.add("maxLength", soln.getLiteral("maxLength").getInt());
								}

								if (soln.contains("minLength")) {
									predicate.add("minLength", soln.getLiteral("minLength").getInt());
								}

								if (soln.contains("pattern")) {
									predicate.add("pattern", soln.getLiteral("pattern").getString());
								}

								if (soln.contains("max") && soln.getLiteral("max").getInt() <= 1) {

									// predicate.add("maxItems",1);

									if (jsonDatatype != null) {
										if (jsonDatatype.equals("langString")) {
											predicate.add("type", "object");
											predicate.add("$ref", "#/definitions/langString");
										} else {
											predicate.add("type", jsonDatatype);
										}
									}

								} else {

									if (soln.contains("max") && soln.getLiteral("max").getInt() > 1) {
										predicate.add("maxItems", soln.getLiteral("max").getInt());
									}

									if (soln.contains("min") && soln.getLiteral("min").getInt() > 0) {
										predicate.add("minItems", soln.getLiteral("min").getInt());
									}

									predicate.add("type", "array");

									arrayType = true;

									if (jsonDatatype != null) {

										if (jsonDatatype.equals("langString")) {
											typeObject.add("type", "object");
											typeObject.add("$ref", "#/definitions/langString");
										} else {
											typeObject.add("type", jsonDatatype);
										}

									}

								}

								if (FORMAT_MAP.containsKey(datatype)) {
									predicate.add("format", FORMAT_MAP.get(datatype));
								}

							} else {

								if (soln.contains("shapeRefName")) {
									predicate.add("@type", "@id");

									String shapeRefName = soln.getLiteral("shapeRefName").getString();

									if (!soln.contains("max") || soln.getLiteral("max").getInt() > 1) {
										if (soln.contains("min")) {
											predicate.add("minItems", soln.getLiteral("min").getInt());
										}
										if (soln.contains("max")) {
											predicate.add("maxItems", soln.getLiteral("max").getInt());

										}
										predicate.add("type", "array");

										predicate.add("items", Json.createObjectBuilder().add("type", "object")
												.add("$ref", "#/definitions/" + shapeRefName).build());
									} else {
										predicate.add("type", "object");
										predicate.add("$ref", "#/definitions/" + shapeRefName);
									}
								}
							}

						}

						/* Every run per predicate */

						if (soln.contains("example")) {
							String example = soln.getLiteral("example").getString();
							exampleSet.add(example);
						}

						if (pResults.hasNext() && className.equals(pResults.peek().getLiteral("className").getString())
								&& (pResults.peek().contains("predicate")
										&& predicateID.equals(pResults.peek().getResource("predicate").toString()))) {

							pIndex += 1;

						} else {

							/* Last run per class */

							if (!exampleSet.isEmpty()) {

								Iterator<String> i = exampleSet.iterator();

								while (i.hasNext()) {
									String ex = i.next();
									exampleList.add(ex);
								}

								predicate.add("example", exampleList.build());

							}

							if (arrayType) {
								predicate.add("items", typeObject.build());
							}
							JsonObject predicateObject = predicate.build();
							properties.add(predicateName, predicateObject);
							definitions.add(predicateName, predicateObject);
							
							predicate = Json.createObjectBuilder();
							typeObject = Json.createObjectBuilder();
							arrayType = false;
							pIndex = 1;
							exampleSet = new HashSet<>();
							exampleList = Json.createArrayBuilder();
							
							
						}
					}

					/* If not build props and requires */
					if (!pResults.hasNext() || !className.equals(pResults.peek().getLiteral("className").getString())) {
						predicate = Json.createObjectBuilder();
						JsonObjectBuilder classDefinition = Json.createObjectBuilder();

						if (soln.contains("classTitle")) {
							classDefinition.add("title", soln.getLiteral("classTitle").getString());
						}
						classDefinition.add("type", "object");
						if (soln.contains("targetClass")) {
							classDefinition.add("@id", soln.getResource("targetClass").toString());
						} else {
							classDefinition.add("@id", soln.getResource("resource").toString());
						}
						if (soln.contains("classDescription")) {
							classDefinition.add("description", soln.getLiteral("classDescription").getString());
						}
						if (soln.contains("minProperties")) {
							classDefinition.add("minProperties", soln.getLiteral("minProperties").getInt());
						}

						if (soln.contains("maxProperties")) {
							classDefinition.add("maxProperties", soln.getLiteral("maxProperties").getInt());
						}

						JsonObject classProps = properties.build();
						if (!classProps.isEmpty())
							classDefinition.add("properties", classProps);

						JsonArrayBuilder required = Json.createArrayBuilder();

						Iterator<String> ri = requiredPredicates.iterator();

						while (ri.hasNext()) {
							String ex = ri.next();
							required.add(ex);
						}

						JsonArray reqArray = required.build();

						if (!reqArray.isEmpty()) {
							classDefinition.add("required", reqArray);
						}

						definitions.add(localClassName != null && localClassName.length() > 0
								? removeInvalidCharacters(localClassName)
								: className, classDefinition.build());

						properties = Json.createObjectBuilder();
						requiredPredicates = new HashSet<>();
					}
				}
			}

			return definitions;

		}
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
				JsonValue props = definitionsObj.get("Root").asJsonObject().get("properties");
				if (props != null) {
					schema.add("properties", props.asJsonObject());
				}

			}
		} else {
			schema.add("type", "array");
			JsonObjectBuilder itemsBuilder = Json.createObjectBuilder();

			itemsBuilder.add("type", "object");
			// get one object that is
			// itemsBuilder.add("properties",
			// definitionsObj.get("Root").asJsonObject().get("properties"));

			schema.add("items", itemsBuilder.build());

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

	public String skosSchema(String pid, Model model, String string) throws Exception {
		Resource metadataResource = model.getResource(pid);
		Resource scheme = metadataResource.getPropertyResourceValue(VOID.rootResource);

		List<Resource> leafs = getLeafConcepts(model);
		Map<String, Object> definitions = new HashMap<String, Object>();

		Map<String, Object> rootDefinition = new HashMap<String, Object>();
		Map<String, Object> rootProperties = new HashMap<String, Object>();
		rootDefinition.put("properties", rootProperties);
		definitions.put("Root", rootDefinition);

		Map<String, Object> schema = new HashMap<String, Object>();
		for (Resource leaf : leafs) {
			traverseUp(leaf, model, definitions, rootProperties);
		}
		schema.put("definitions", definitions);
		schema.put("$schema", "http://json-schema.org/draft-04/schema#");
		schema.put("type", "object");

		schema.put("properties", rootProperties);

		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsString(schema);
	}

}
