package fi.vm.yti.datamodel.api.v2.transformation;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.core.io.ClassPathResource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import com.github.underscore.U;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.transformer.TransformationSpec;

import be.ugent.idlab.knows.functions.agent.Agent;
import be.ugent.idlab.knows.functions.agent.AgentFactory;
import be.ugent.idlab.knows.functions.agent.Arguments;
import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import io.micrometer.core.instrument.util.IOUtils;
import net.minidev.json.JSONArray;

@TestInstance(Lifecycle.PER_CLASS)
public class FnoMappingTester {

	private Agent agent;
	
	
	@BeforeAll
	public void createAgent()  throws Exception{		
	    this.agent = AgentFactory.createFromFnO("src/test/resources/fno/test1.ttl");
	}
	/*
	
	private String generateTargetDocument(Map<String, Object> columnsMap) {
		String json = "{ }";
		Configuration configuration = Configuration.builder()
	    		.options(Option.CREATE_MISSING_PROPERTIES_ON_DEFINITE_PATH).build();
		for (Entry<String, Object> entry : columnsMap.entrySet()) {
		  JsonPath compiledPath = JsonPath.compile(entry.getKey());
		  Object parsedJsonPath =
		      compiledPath.set(configuration.jsonProvider().parse(json), entry.getValue(), configuration);
		  json = JsonPath.parse(parsedJsonPath).jsonString();
		}
		return json;
	}
	
	private String getInputParamURI(Object value) {
		if(value instanceof String) {
			return "http://uri.suomi.fi/datamodel/ns/mscr#inputString";
		}
		if(value instanceof Double) {
			return "http://uri.suomi.fi/datamodel/ns/mscr#inputDouble";
		}
			
		return "http://uri.suomi.fi/datamodel/ns/mscr#inputObject";
	}
	
	private void handleSimpleMapping(Map<String, Object> m, MappingDTO mapping, Object value, Integer index) {
		// preprocess original value
		if(mapping.getSourcePreprocessing() != null) {				
			Arguments arguments = new Arguments()
			        .add(getInputParamURI(value), value);
		    try {
				value = this.agent.execute(mapping.getSourcePreprocessing(), arguments);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	    Object newValue = "";
	    // transform 
	    final Arguments arguments = new Arguments();
	    if(mapping.getProcessingParams() != null) {
		    if(mapping.getProcessingParams().isEmpty()) {
		    }
		    else {
		    	arguments.add("http://uri.suomi.fi/datamodel/ns/mscr#nodeObject", value);
		    	
		    	mapping.getProcessingParams().keySet().forEach(_key -> {
		    		arguments.add(_key, mapping.getProcessingParams().get(_key));
		    	});
		    	
		    }	    	
	    }
	    else {
	    	arguments.add(getInputParamURI(value), value);
	    	
	    }

		    
	    try {
			newValue = this.agent.execute(mapping.getProcessing(), arguments);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    // preprocess newValue
	    if(mapping.getTargettPreprocessing() != null) {
			Arguments arguments3 = new Arguments()
			        .add(getInputParamURI(newValue), newValue);
		    try {
				newValue = this.agent.execute(mapping.getTargettPreprocessing(), arguments3);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		    	
	    }

	    // add to columns
	    if(index == null) {
	    	m.put(mapping.getTarget(), newValue);
	    }
	    else {
	    	m.put(mapping.getTarget().replace("*", index +""), newValue);
	    }
	    
	}
	private Map<String, Object> generateColumnsMap(Set<MappingDTO> mappings, DocumentContext source) throws Exception {
		return generateColumnsMap(mappings, source, null, null);
	}
	private Map<String, Object> generateColumnsMap(Set<MappingDTO> mappings, DocumentContext source, String rootElement, Map<String, String> ns) throws Exception {
		Map<String, Object> m = new LinkedHashMap<>();
		
		
		mappings.forEach(mapping -> {
			// get the value using source path 
			Object value = source.read(mapping.getSource());
			// check if we are 
			if(value instanceof JSONArray) {
				final AtomicInteger index = new AtomicInteger(0);
				
				((JSONArray) value).forEach(_value -> {					
					handleSimpleMapping(m, mapping, _value, index.getAndAdd(1) );
					
				});
			}
			else {
				handleSimpleMapping(m, mapping, value, null);
			}

		    

		});
		if(ns != null && !m.isEmpty() && rootElement != null) {
			ns.keySet().forEach(_nsKey -> {
				if(!_nsKey.equals("")) {
					m.put("$." +rootElement + ".-xmlns:" + _nsKey, ns.get(_nsKey));	
				}
				else {
					m.put("$." +rootElement + ".-xmlns", ns.get(_nsKey));
				}
				
			});
					
			
		}

		return m;
	}
	*/
	/*
	
	@Test
	public void testJsonToJsonMappingWithInputAndOutputPreprocessing() throws Exception {
		MappingDTO mapping = new MappingDTO(
				"$.temperature", // source 
				"temperature",
				"http://uri.suomi.fi/datamodel/ns/mscr#celsiusToFahrenheitFunc", // value transformation 
				"$.temperature2",  // target
				"temperature2",
				"http://uri.suomi.fi/datamodel/ns/mscr#stringToDoubleFunc", // source preprocessing
				"http://uri.suomi.fi/datamodel/ns/mscr#anyToStringFunc"); // target preprocessing
		MappingDTO mapping2 = new MappingDTO(
				"$.temperature", // source 
				"temperature",
				"http://uri.suomi.fi/datamodel/ns/mscr#celsiusToFahrenheitFunc", // value transformation 
				"$.temperature3",  // target
				"temperature3",
				"http://uri.suomi.fi/datamodel/ns/mscr#stringToDoubleFunc", // source preprocessing
				null); // target preprocessing

		DocumentContext source = JsonPath.parse(getClass().getClassLoader().getResourceAsStream("fno/sourceTemperature.json"));
		Map<String, Object> columnsMap = generateColumnsMap(Set.of(mapping, mapping2), source);
		String json = generateTargetDocument(columnsMap);
		System.out.println(json);
	}
	
	@Test
	public void testJsonToCsvMappingValueProcessing() throws Exception {
		MappingDTO mapping = new MappingDTO(
				"$.temperature", // source 
				"temperature",
				"http://uri.suomi.fi/datamodel/ns/mscr#celsiusToFahrenheitFunc", // value transformation 
				"$.converted_temperature",  // target
				"converted_temperature",
				"http://uri.suomi.fi/datamodel/ns/mscr#stringToDoubleFunc", // source preprocessing
				null); // target preprocessing
		MappingDTO mapping2 = new MappingDTO(
				"$.temperature", // source 
				"temperature",
				"http://uri.suomi.fi/datamodel/ns/mscr#stringToStringFunc", // value transformation 
				"$.original_temperature",  // target
				"original_temperature",
				null, // source preprocessing
				null); // target preprocessing
		Set<MappingDTO> mappings = Set.of(mapping, mapping2);
		DocumentContext source = JsonPath.parse(getClass().getClassLoader().getResourceAsStream("fno/sourceTemperature.json"));
		Map<String, Object> columnsMap = generateColumnsMap(mappings, source);
		String json = generateTargetDocument(columnsMap);
		
		JsonNode jsonTree = new ObjectMapper().readTree(json);
		Builder csvSchemaBuilder = CsvSchema.builder();
		mappings.forEach(_m -> {csvSchemaBuilder.addColumn(_m.getObjectLabel());} );
		CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();		
		CsvMapper csvMapper = new CsvMapper();
		csvMapper.writerFor(JsonNode.class)
		  .with(csvSchema)
		  .writeValue(System.out, jsonTree);
	}	
	
	@Test
	public void testJsonToCsv2() throws Exception {

		MappingDTO mapping = new MappingDTO(
				"$.temperature.value", // source 
				"temperature",
				"http://uri.suomi.fi/datamodel/ns/mscr#anyToStringFunc", // value transformation 
				"$.temperature_celcius",  // target
				"temperature_celcius",
				null, // source preprocessing
				null); // target preprocessing
		Set<MappingDTO> mappings = Set.of(mapping);
		DocumentContext source = JsonPath.parse(getClass().getClassLoader().getResourceAsStream("fno/sourceTemperatureComplex.json"));
		Map<String, Object> columnsMap = generateColumnsMap(mappings, source);
		String json = generateTargetDocument(columnsMap);
		
		JsonNode jsonTree = new ObjectMapper().readTree(json);
		Builder csvSchemaBuilder = CsvSchema.builder();
		mappings.forEach(_m -> {csvSchemaBuilder.addColumn(_m.getObjectLabel());} );
		CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();		
		CsvMapper csvMapper = new CsvMapper();
		csvMapper.writerFor(JsonNode.class)
		  .with(csvSchema)
		  .writeValue(System.out, jsonTree);
	}		
	
	
	@Test
	public void testJsonToCsvComplexObject() throws Exception {

		MappingDTO mapping = new MappingDTO(
				"$.coordinate", // source 
				"coordinate",
				"http://uri.suomi.fi/datamodel/ns/mscr#customCoordinateToStringFunc", // value transformation 
				"$.coordinate",  // target
				"coordinate",
				null, // source preprocessing
				null); // target preprocessing
		Set<MappingDTO> mappings = Set.of(mapping);
		DocumentContext source = JsonPath.parse(getClass().getClassLoader().getResourceAsStream("fno/sourceTemperatureComplex.json"));
		Map<String, Object> columnsMap = generateColumnsMap(mappings, source);
		String json = generateTargetDocument(columnsMap);
		System.out.println(json);
		JsonNode jsonTree = new ObjectMapper().readTree(json);
		Builder csvSchemaBuilder = CsvSchema.builder();
		mappings.forEach(_m -> {csvSchemaBuilder.addColumn(_m.getObjectLabel());} );
		CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();		
		CsvMapper csvMapper = new CsvMapper();
		csvMapper.writerFor(JsonNode.class)
		  .with(csvSchema)
		  .writeValue(System.out, jsonTree);
	}
	
	@Test
	public void testJsonToCsvComplexObjectArray() throws Exception {

		MappingDTO mapping = new MappingDTO(
				"$.coordinates", // source 
				"coordinates",
				"http://uri.suomi.fi/datamodel/ns/mscr#customCoordinateToStringFunc", // value transformation 
				"$.coordinates",  // target
				"coordinates",
				null, // source preprocessing
				null); // target preprocessing
		Set<MappingDTO> mappings = Set.of(mapping);
		DocumentContext source = JsonPath.parse(getClass().getClassLoader().getResourceAsStream("fno/sourceTemperatureComplex.json"));
		Map<String, Object> columnsMap = generateColumnsMap(mappings, source);
		String json = generateTargetDocument(columnsMap);
		System.out.println(json);
		JsonNode jsonTree = new ObjectMapper().readTree(json);
		Builder csvSchemaBuilder = CsvSchema.builder();
		mappings.forEach(_m -> {csvSchemaBuilder.addColumn(_m.getObjectLabel());} );
		CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();		
		CsvMapper csvMapper = new CsvMapper();
		csvMapper.writerFor(JsonNode.class)
		  .with(csvSchema)
		  .writeValue(System.out, jsonTree);
	}
	
	@Test
	public void testJsonToJsonComplexObject() throws Exception {

		MappingDTO mapping = new MappingDTO(
				"$.persons[*].name", // source 
				"name",
				"http://uri.suomi.fi/datamodel/ns/mscr#stringToStringFunc", // value transformation 
				"$.persons[*].fullName",  // target
				"fullName",
				null, // source preprocessing
				null); // target preprocessing
		
		MappingDTO mapping2 = new MappingDTO(
				"$.persons[*].org.name", // source 
				"name",
				"http://uri.suomi.fi/datamodel/ns/mscr#stringToStringFunc", // value transformation 
				"$.persons[*].orgName",  // target
				"orgName",
				null, // source preprocessing
				null); // target preprocessing
		MappingDTO mapping3 = new MappingDTO(
				"$.persons[*].org", // source 
				"org",
				"http://uri.suomi.fi/datamodel/ns/mscr#configurableObjectToStringFunc", // value transformation 
				"$.persons[*].orgName2",  // target
				"orgName2",
				null, // source preprocessing
				null); // target preprocessing	
		mapping3.predicateParams.put("http://uri.suomi.fi/datamodel/ns/mscr#inputObject", List.of("name", "address"));
		mapping3.predicateParams.put("http://uri.suomi.fi/datamodel/ns/mscr#inputString", ", ");
		Set<MappingDTO> mappings = Set.of(mapping, mapping2, mapping3);
		DocumentContext source = JsonPath.parse(getClass().getClassLoader().getResourceAsStream("fno/sourceTemperatureComplex.json"));
		Map<String, Object> columnsMap = generateColumnsMap(mappings, source);
		String json = generateTargetDocument(columnsMap);
		System.out.println(json);
	}	
	
	@Test
	public void testJsonToXmlComplexObject() throws Exception {

		MappingDTO mapping = new MappingDTO(
				"$.persons[*].name", // source 
				"name",
				"http://uri.suomi.fi/datamodel/ns/mscr#stringToStringFunc", // value transformation 
				"$.persons.person[*].fullName",  // target
				"fullName",
				null, // source preprocessing
				null); // target preprocessing
		
		MappingDTO mapping2 = new MappingDTO(
				"$.persons[*].org.name", // source 
				"name",
				"http://uri.suomi.fi/datamodel/ns/mscr#stringToStringFunc", // value transformation 
				"$.persons.person[*].ex:orgName",  // target
				"orgName",
				null, // source preprocessing
				null); // target preprocessing
		MappingDTO mapping3 = new MappingDTO(
				"$.persons[*].org", // source 
				"org",
				"http://uri.suomi.fi/datamodel/ns/mscr#configurableObjectToStringFunc", // value transformation 
				"$.persons.person[*].-mscr:orgName2",  // target
				"orgName2",
				null, // source preprocessing
				null); // target preprocessing	
		mapping3.predicateParams.put("http://uri.suomi.fi/datamodel/ns/mscr#inputObject", List.of("name", "address"));
		mapping3.predicateParams.put("http://uri.suomi.fi/datamodel/ns/mscr#inputString", ", ");
		
		Map<String, String> namespaces = Map.of(
				"ex", "http://example.com/",
				"mscr", "http://uri.suomi.fi/datamodel/ns/mscr#"
				);
		
		
		Set<MappingDTO> mappings = Set.of(mapping, mapping2, mapping3);
		DocumentContext source = JsonPath.parse(getClass().getClassLoader().getResourceAsStream("fno/sourceTemperatureComplex.json"));
		Map<String, Object> columnsMap = generateColumnsMap(mappings, source, "persons", namespaces);
		String json = generateTargetDocument(columnsMap);
		System.out.println(json);
		String xml = U.jsonToXml(json);
		System.out.println(xml);
	}
	
	@Test
	public void testXmlToXmlComplex() throws Exception {
		String xmlString = IOUtils.toString(getClass().getClassLoader().getResourceAsStream("fno/datacite-example-full-v4.1.xml"));
		String jsonString = U.xmlToJson(xmlString);
		System.out.println(jsonString);
		Map<String, String> namespaces = Map.of(
				"", "http://schema.eudat.eu/schemas/v1.1"				
				);		
		
		MappingDTO mapping = new MappingDTO(
				"$.resource.geoLocations.geoLocation[*]", // source 
				"geoLocation",
				"http://uri.suomi.fi/datamodel/ns/mscr#copyMapFunc", // value transformation 
				"$.resource.spatialCoverages.spatialCoverage",  // target
				"spatialCoverage",
				null, // source preprocessing
				null); // target preprocessing
		Set<MappingDTO> mappings = Set.of(mapping);
		DocumentContext source = JsonPath.parse(jsonString);
		Map<String, Object> columnsMap = generateColumnsMap(mappings, source, "resource", namespaces);
		String json = generateTargetDocument(columnsMap);
		System.out.println(json);
		String xml = U.jsonToXml(json);
		System.out.println(xml);		
	}
	*/
	/*
	@Test
	public void testDataciteXmlToB2FINDJson1() throws Exception {
		String xmlString = IOUtils.toString(getClass().getClassLoader().getResourceAsStream("fno/datacite-example-full-v4.1.xml"));
		String jsonString = U.xmlToJson(xmlString);
		System.out.println(jsonString);
		Map<String, String> namespaces = Map.of(
				"", "http://schema.eudat.eu/schemas/v1.1"				
				);		
		
		MappingDTO titleM = new MappingDTO(
				"$.resource.titles.title[?(!@.-titleType)].#text", // source 
				"title",
				"$.resource.title[*]",  // target
				"title",
				"http://uri.suomi.fi/datamodel/ns/mscr#stringToStringFunc", // value transformation 
				null,
				null,
				null,
				null,
				null);
		
		MappingDTO descM = new MappingDTO(
				"$.resource.descriptions[*].#text", // source 
				"description",
				"$.resource.description[*]",  // target
				"description",
				"http://uri.suomi.fi/datamodel/ns/mscr#formatStringFunc", // value transformation 
				null,
				null,
				null,
				null,
				null);
		MappingDTO descM2 = new MappingDTO(
				"$.resource.descriptions.description[*].#text", // source 
				"description",
				"$.resource.description[*]",  // target
				"description",
				"http://uri.suomi.fi/datamodel/ns/mscr#formatStringFunc", // value transformation 
				null,
				null,
				null,
				null,
				null);	

		MappingDTO doiM = new MappingDTO(
				"$.resource.identifier[?(@.-identifierType=='DOI')].#text", 
				"doi",
				"$.resource.doi", 
				"doi",
				"http://uri.suomi.fi/datamodel/ns/mscr#prefixStringFunc",  
				Map.of("http://uri.suomi.fi/datamodel/ns/mscr#inputString", "https://doi.org/"),
				null, 
				null,
				null,
				null); 		
		
		MappingDTO keywordM = new MappingDTO(
				"$.resource.subjects[*].#text", // source 
				"keyword",
				"$.resource.keywords[*]",  // target
				"keywords",
				"http://uri.suomi.fi/datamodel/ns/mscr#formatStringFunc", // value transformation 
				null,
				null,
				null,
				null,
				null);	
		MappingDTO keywordM2 = new MappingDTO(
				"$.resource.subjects.subject[*].#text", // source 
				"keyword",
				"$.resource.keywords[*]",  // target
				"keywords",
				"http://uri.suomi.fi/datamodel/ns/mscr#formatStringFunc", // value transformation 
				null,
				null,
				null,
				null,
				null);	
		MappingDTO disciplineM = new MappingDTO(
				"$.resource.subjects[*].#text", // source 
				"subject",
				"$.resource.discipline[*]",  // target
				"discipline",
				"http://uri.suomi.fi/datamodel/ns/mscr#similarityBasedValueMappingFunc", // value transformation 
				Map.of("http://uri.suomi.fi/datamodel/ns/mscr#inputDouble",  ((Object)0.9),
						"http://uri.suomi.fi/datamodel/ns/mscr#inputString", (Object)"pid:voc1"),
				null,
				null,
				null,
				null);			
		
		Set<MappingDTO> mappings = Set.of(
				titleM,
				descM,
				descM2,
				doiM,
				keywordM,
				keywordM2
				);
				
		DocumentContext source = JsonPath.parse(jsonString);
		Map<String, Object> columnsMap = generateColumnsMap(mappings, source, "resource", namespaces);
		String json = generateTargetDocument(columnsMap);
		System.out.println(json);
		
	}
	*/
}
