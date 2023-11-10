package fi.vm.yti.datamodel.api.v2.service.impl;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import com.github.underscore.U;
import com.github.underscore.U.JsonToXmlMode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.PathNotFoundException;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import be.ugent.idlab.knows.functions.agent.Agent;
import be.ugent.idlab.knows.functions.agent.AgentFactory;
import be.ugent.idlab.knows.functions.agent.Arguments;
import be.ugent.idlab.knows.functions.agent.functionModelProvider.fno.exception.FnOException;
import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.dto.MappingFilterDTO;
import fi.vm.yti.datamodel.api.v2.dto.NodeInfo;
import fi.vm.yti.datamodel.api.v2.dto.OneOfDTO;
import fi.vm.yti.datamodel.api.v2.dto.ProcessingInfo;
import fi.vm.yti.datamodel.api.v2.service.DataTransformationService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;


@Service
public class FnoDataTransformationServiceImpl implements DataTransformationService {

	//private final CrosswalkMapper mapper;
	private Agent agent;
	
	public FnoDataTransformationServiceImpl() {
		try {
			this.agent = AgentFactory.createFromFnO("src/main/resources/fno/functions.ttl");
		} catch (FnOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	private String CSVtoIntermediateJSON(String source) throws IOException, CsvException {
		CSVReader reader = new CSVReader(new StringReader(source));
		List<String[]> rows = reader.readAll();
		reader.close();
		
		String[] cols = rows.remove(0);
		JSONObject obj = new JSONObject();
		String[] row = rows.get(0);
		for(int i = 0; i < cols.length; i++) {				
			obj.appendField(cols[i], row[i]);	
		}
		return obj.toJSONString();
	}
	
	private String XMLtoIntermediateJSON(String source) throws Exception {
		return U.xmlToJson(source);
	}
	
	private Object doProcessing(ProcessingInfo pi, Object value) {
		final Arguments arguments = new Arguments();		
		arguments.add("http://uri.suomi.fi/datamodel/ns/mscr#inputObject", value);
		if(pi.getParams() != null) {			
			arguments.add("http://uri.suomi.fi/datamodel/ns/mscr#paramsMap", pi.getParams());	
		}
		
		Object newValue = value;
		try {
			newValue = this.agent.execute(pi.getId(), arguments);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return newValue;
	}

	
	private Map<String, Object> collectDataForSingleMapping(List<NodeInfo> sources, DocumentContext doc) {
		Map<String, Object> paramMap = new HashMap<String, Object>();
		for(int i = 0; i < sources.size(); i++)  {
			try {
				String path = sources.get(i).getId();
				int pindex = 0;
				if(path.indexOf("[*]") > 0 && path.indexOf("[*]") != path.length() - 3) { // first level
					
					String subPath = path.substring(pindex, path.indexOf("[*]"));
					String restPath = "$." + path.substring(path.indexOf("[*]") + 4);
					//System.out.println(subPath);
					//System.out.println(restPath);
					Object subObject = doc.read(subPath);
					
					if(subObject instanceof JSONArray) {
						JSONArray values = new JSONArray();
						String paramKey = sources.get(i).getLabel();
						for(Object obj : (JSONArray)subObject) {
							DocumentContext doc2 = JsonPath.parse(obj);
							if(restPath.indexOf("*") > 0) {
								String subPath2 = restPath.substring(0, restPath.indexOf("[*]"));
								String restPath2 = "$." + restPath.substring(restPath.indexOf("[*]") + 4);
								//System.out.println(subPath2);
								//System.out.println(restPath2);
								Object subObject2 = "##missing##";
								try {
									subObject2 = doc2.read(subPath2);
								}catch(PathNotFoundException ex) {
									JSONArray values2 = new JSONArray();
									values2.add("##missing##");
									values.add(values2);
								}								

								
								if(subObject2 instanceof JSONArray) {
									System.out.println("JSONArray");
									System.out.println(subObject2);
									paramKey = "[*]." + paramKey;
									JSONArray values2 = new JSONArray();
									for(Object obj2 : (JSONArray)subObject2) {
										DocumentContext doc3 = JsonPath.parse(obj2);
										Object value = "##missing##";
										try {
											value = doc3.read(restPath2);
										}catch(PathNotFoundException ex) {
											System.out.println("ex");
										}
			
										NodeInfo source = sources.get(i);
										if(source.getProcessing() != null && value != null) {	
											value = doProcessing(source.getProcessing(), value);
										}
										if(value != null) {
											values2.add(value);	
										}
																				
										
										
									}
									values.add(values2);
									
								}
								else {
									//System.out.println(subObject2);
									paramKey = "[*]." + paramKey;
									JSONArray values2 = new JSONArray();
									DocumentContext doc3 = JsonPath.parse(subObject2);
									Object value = "##missing##";
									try {
										value = doc3.read(restPath2);
									}catch(PathNotFoundException ex) {
										System.out.println("ex");
									}
		
									NodeInfo source = sources.get(i);
									if(source.getProcessing() != null && value != null) {	
										value = doProcessing(source.getProcessing(), value);
									}
									if(value != null) {
										values2.add(value);	
									}
										
									values.add(values2);
								}
							}
							else {
							
							
								
								Object value = "##missing##";
								try {
									value = doc2.read(restPath);
								}catch(PathNotFoundException ex) {
									
								}
	
								NodeInfo source = sources.get(i);
								if(source.getProcessing() != null && value != null) {	
									value = doProcessing(source.getProcessing(), value);
								}
								if(value != null) {
									values.add(value);	
								}
								
							}
						}
						paramMap.put(paramKey, values);
					}
					else {
						// single object
						Configuration configuration = Configuration.builder()
					    		.options(Option.ALWAYS_RETURN_LIST).build();
						
						DocumentContext doc2 = JsonPath.parse(subObject, configuration);
						Object value = "##missing##";
						try {
							value = doc2.read(restPath);
						}catch(PathNotFoundException ex) {
							
						}

						NodeInfo source = sources.get(i);
						if(source.getProcessing() != null && value != null) {	
							value = doProcessing(source.getProcessing(), value);
						}
						if(value != null) {
							if(paramMap.containsKey(sources.get(i).getLabel()) ) {
								if(!(paramMap.get(sources.get(i).getLabel()) instanceof List)) {
									List<Object> params = new ArrayList<Object>();
									params.add(paramMap.get(sources.get(i).getLabel()));
									paramMap.put(sources.get(i).getLabel(), params);
								}
								
								((List<Object>)paramMap.get(sources.get(i).getLabel())).add(value);
								
							}
							else {
								paramMap.put(sources.get(i).getLabel(), value);	
							}
														
						}

						

						
					}

					
					
				}
				else {
					Object value = doc.read(sources.get(i).getId());
					
					NodeInfo source = sources.get(i);


					if(source.getProcessing() != null && value != null) {	
						value = doProcessing(source.getProcessing(), value);
					}
					if(value != null) {
						if(paramMap.containsKey(sources.get(i).getLabel()) ) {
							if(!(paramMap.get(sources.get(i).getLabel()) instanceof List)) {
								List<Object> params = new ArrayList<Object>();
								params.add(paramMap.get(sources.get(i).getLabel()));
								paramMap.put(sources.get(i).getLabel(), params);
							}
							if(sources.size() == 1) {
								((List<Object>)paramMap.get("value")).add(value);
							}
							else {
								((List<Object>)paramMap.get(sources.get(i).getLabel())).add(value);	
							}
							
							
						}
	
						else {
							if(sources.size() == 1) {
								paramMap.put("value", value);
							}
							else {
								paramMap.put(sources.get(i).getLabel(), value);	
							}
								
						}
					}
					
					
				}
			}catch(PathNotFoundException pnf) {
				System.out.println("Path not found: "+ sources.get(i).getId());
				
			}
		}
		if(paramMap.isEmpty()) {
			return null;
		}
		return paramMap;
		
	}

	private String generateCSV(List<MappingDTO> mappings,  String json) throws Exception {
		System.out.println(json);
		JsonNode jsonTree = new ObjectMapper().readTree(json);
		
		Builder csvSchemaBuilder = CsvSchema.builder();
		mappings.forEach(_m -> { 
			_m.getTarget().forEach(_t -> {
				
				csvSchemaBuilder.addColumn(_t.getLabel());
				
			});		
		});
		CsvSchema csvSchema = csvSchemaBuilder.build().withHeader();		
		CsvMapper csvMapper = new CsvMapper();
		
		return csvMapper.writerFor(JsonNode.class)
		  .with(csvSchema)
		  .writeValueAsString(jsonTree);
		  	
	}
	
	private String generateJSON(Map<String, Object> columnsMap) {
		String json = "{}";
		//if(keySample.startsWith("$.[") || keySample.startsWith("$[")) {
		//	json = "[]";
		//}
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
	
	private String generateXML(String json) {
		String temp = U.jsonToXml(json, JsonToXmlMode.REMOVE_ATTRIBUTES);
		return temp;
	}
	
	private String generateTargetDocument(List<MappingDTO> mappings, Map<String, Object> columnsMap, String format) throws Exception {
		switch (format) {
		case "csv": {		
			return generateCSV(mappings, generateJSON(columnsMap));
		}
		case "json": {
			return generateJSON(columnsMap);
		}
		case "xml": {
			return generateXML(generateJSON(columnsMap)); // needs a specialized handling for xml
		}
		default:
			throw new IllegalArgumentException("Unexpected value: " + format);
		}
	}
		
	public String transform(String sourceDocument, String sourceFormat,  List<MappingDTO> mappings, String targetFormat) throws Exception {
		return transform(sourceDocument, sourceFormat, mappings, targetFormat, null, null);
	}
	
	@Override
	public String transform(String sourceDocument, String sourceFormat, List<MappingDTO> mappings, String targetFormat, Map<String, String> namespaces, String rootElement) throws Exception {

		String jsonStr = null;		
		switch (sourceFormat) {
		case "csv": {			
			jsonStr = CSVtoIntermediateJSON(sourceDocument);
			//jsonStr = "{\"root\":" + jsonStr + "}";
			break;
		}
		case "xml": {
			jsonStr = XMLtoIntermediateJSON(sourceDocument);
			break;
		}
		case "json": {
			jsonStr = sourceDocument;
			break;
		}
		
		default:
			throw new IllegalArgumentException("Unexpected value: " + sourceFormat);
		}
		
		//System.out.println(jsonStr);
		DocumentContext source = JsonPath.parse(jsonStr);		
		Map<String, Object> columnsMap = generateColumnsMap2(mappings, source, rootElement, namespaces);
		String output = generateTargetDocument(mappings, columnsMap, targetFormat);

		return output;
	}
	
	private Object removeParamKeyForSingleOutput(Map<String, Object> values) {
		Object newValue = null;
		if(values != null && values.keySet().size() == 1 ) { // TODO why only size == 1?
			// turn single values into "just" values
			String key = values.keySet().iterator().next();
			newValue = values.get(key);
			if(newValue instanceof JSONArray) {
				JSONArray a = (JSONArray)newValue;
				if(a.size() == 1) {
					newValue = a.get(0);
				}
			}
			return newValue;
		}		
		else {
			return values;
		}
		
	}
	
	private boolean isWellFormedUriString(final String uriString) {
        try {
            new java.net.URI(uriString);
            return true;
        } catch (final URISyntaxException ignored) {
            return false;
        }
    }	
	private boolean isFilteredResult(Object result, MappingFilterDTO filter) {
		if(result != null && filter != null) {
			if(filter.getOperator().equals("=")) {
				return result.equals(filter.getValue());
			}
			else if(filter.getOperator().equals("!=")) {
				return !result.equals(filter.getValue());
			}	
			else if(filter.getOperator().equals("in")) {
				return ((List<Object>)filter.getValue()).indexOf(result) >=0;
			}	
			else if(filter.getOperator().equals("startsWith")) {
				return result.toString().startsWith(filter.getValue().toString());
			}	
			else if(filter.getOperator().equals("isURI")) {
				return isWellFormedUriString(result.toString());
			}			
			else if(filter.getOperator().equals("contains")) {
				return result.toString().indexOf(filter.getValue().toString()) >= 0;
			}						
			else if(filter.getOperator().equals("!contains")) {
				return !(result.toString().indexOf(filter.getValue().toString()) >= 0);
			}			
			else {
				throw new RuntimeException("Unknown operator "+ filter.getOperator());
			}			
		}
		return true;		
	}	
	private boolean isIncluded(Object value, MappingDTO mapping) {
		if(value != null && mapping.getFilter() != null) {
			MappingFilterDTO filter = mapping.getFilter();
			DocumentContext doc = JsonPath.parse(value);
			Object result = null;
			try {
				result = doc.read(filter.getPath());
			}catch(PathNotFoundException ex) {
				result = "null";
			}
			if(filter.getOperator().equals("=")) {
				return result.equals(filter.getValue());
			}
			else if(filter.getOperator().equals("!=")) {
				return !result.equals(filter.getValue());
			}	
			else if(filter.getOperator().equals("in")) {
				return ((List<Object>)filter.getValue()).indexOf(result) >=0;
			}	
			else if(filter.getOperator().equals("startsWith")) {
				return result.toString().startsWith(filter.getValue().toString());
			}	
			else if(filter.getOperator().equals("isURI")) {
				return isWellFormedUriString(result.toString());
			}			
			else if(filter.getOperator().equals("contains")) {
				return result.toString().indexOf(filter.getValue().toString()) >= 0;
			}						
			else if(filter.getOperator().equals("!contains")) {
				return !(result.toString().indexOf(filter.getValue().toString()) >= 0);
			}			
			else {
				throw new RuntimeException("Unknown operator "+ filter.getOperator());
			}			
		}
		return true;		
	}
	
	private Map<String, Object> sortColumnMapKeys(Map<String, Object> c) {
		// order the keys according to collection indices
		Map<String, Map<Object, Map<String, Object>>> groups = new LinkedHashMap<String, Map<Object, Map<String, Object>>>();
		for(String key : c.keySet()) {
			if(key.indexOf("]") > 0 ) {
				Integer orderingKey = Integer.parseInt(key.substring(key.indexOf("[") + 1, key.indexOf("]")));
				String group = key.substring(0, key.indexOf("[") + 1);
				if(!groups.containsKey(group)) {
					groups.put(group, new TreeMap<Object, Map<String, Object>>());
				}
				if(!groups.get(group).containsKey(orderingKey)) {					
					groups.get(group).put(orderingKey, new LinkedHashMap<String, Object>());
				}
				groups.get(group).get(orderingKey).put(key, c.get(key));
				

			}
			else {
				if(!groups.containsKey(key)) {
					groups.put(key, new LinkedHashMap<Object, Map<String, Object>>());
					groups.get(key).put(key, new LinkedHashMap<String, Object>());					
				}
				groups.get(key).get(key).put(key, c.get(key));

			}
			
		
		}
		Map<String, Object> c2 = new LinkedHashMap<String, Object>();
		for(String key : groups.keySet()) {
			for(Object key2: groups.get(key).keySet()) {
				c2.putAll(groups.get(key).get(key2));	
			}
			
		}
		return c2;
	}
	
	private void addNamespaces(Map<String, Object> c, String rootElement, Map<String, String> namespaces) {
		if(namespaces != null && !c.isEmpty() && rootElement != null) {
			namespaces.keySet().forEach(_nsKey -> {
				if(!_nsKey.equals("")) {
					c.put("$." +rootElement + ".-xmlns:" + _nsKey, namespaces.get(_nsKey));	
				}
				else {
					c.put("$." +rootElement + ".-xmlns", namespaces.get(_nsKey));
				}
				
			});
					
			
		}		
	}
	
	private boolean handleMapping(Map<String, Object> c, MappingDTO mapping, Set<String> addedCollections, DocumentContext doc) {
		int initialC = c.keySet().size();
		Map<String, Object> values = null;
		// get the value using source path 
		List<NodeInfo> sources = mapping.getSource();
		for(NodeInfo source : sources ) {
			String sourcePath = source.getId();
			if(sourcePath.indexOf("[*") > 0) {
				addedCollections.add(sourcePath.substring(0, sourcePath.indexOf("]") ));
			}
			/*
			else {
				for(NodeInfo target: mapping.getTarget()) {
					String targetPath = target.getId();
					if(targetPath.indexOf("[*") > 0) {
						addedCollections.add(sourcePath);		
					}
					
				}					
			}
			*/
		}
		values = collectDataForSingleMapping(sources, doc);
		// this is done in order to make most of the functions simpler
		Object values2 = removeParamKeyForSingleOutput(values);
		//System.out.print("original value");
		//System.out.println(values2);

		// processing step takes as an input either single object or and array, depending on the sources 
		//System.out.print("after processing");
		//System.out.println(values2);
		
		if(values2 instanceof JSONArray) {
			JSONArray values2Array = (JSONArray)values2;
			int valueIndex = 0;
			for(Object value : values2Array) {
				// check if value should be filtered of not
				//System.out.println(value);
				if(isIncluded(value, mapping)) {
					if(mapping.getProcessing() != null && value != null) {				
						value = doProcessing(mapping.getProcessing(), value);	
					}						
					addTargetNodes(c, mapping, value, valueIndex, addedCollections);
					valueIndex++;
				}
				else {
					if(!mapping.getFilter().isDistinctValues()) {
						valueIndex++;	
					}
					
				}
				
			}
		}
		else {
			if(isIncluded(values2, mapping)) {
				if(mapping.getProcessing() != null && values2 != null) {				
					values2 = doProcessing(mapping.getProcessing(), values2);	
				}
				
				addTargetNodes(c, mapping, values2, 0, addedCollections);
			}
		}
		return c.keySet().size() > initialC;
	}
 	
	private Map<String, Object> generateColumnsMap2(List<MappingDTO> mappings, DocumentContext doc,
			String rootElement, Map<String, String> namespaces) {
		Map<String, Object> c = new LinkedHashMap<String, Object>();
		Set<String> addedCollections = new HashSet<String>();
		for(MappingDTO mapping : mappings) {
			if(mapping.getOneOf() != null) {
				boolean pickMe = false;
				
				for(OneOfDTO oneOf : mapping.getOneOf()) {
					boolean isFiltered = true;
					if(oneOf.getFilter() != null) {
						Object r = doc.read(oneOf.getFilter().getPath());
						if(r != null) {
							if(r instanceof JSONArray) {
								// if none match set isFilter to false
								boolean matchFound = false;
								for(Object _obj : (JSONArray)r) {
									if(isFilteredResult(_obj, oneOf.getFilter())) {
										matchFound = true;
									}
								}
								if(!matchFound) {
									isFiltered = false;									
								}
							}
							else {
								if(!isFilteredResult(r, oneOf.getFilter())) {
									isFiltered = false;
								}
							}
						}
					}
					List<MappingDTO> _mappings = oneOf.getMappings();
					
					
					boolean hasResults2 = false;
					if(isFiltered) {
						for(int i = 0; i < _mappings.size(); i++) {						
							hasResults2 = handleMapping(c, _mappings.get(i), addedCollections, doc);
							if(hasResults2) {
								pickMe = true;
							}
						}
					}
					if(pickMe) {
						break;
					}					
				}

				

			}
			else {
				handleMapping(c, mapping, addedCollections, doc);
			}
			
		}
		
		addNamespaces(c, rootElement, namespaces);		
		return sortColumnMapKeys(c);

	}
	
	private int getPrevIndex(Map<String, Object> c, String path, int index) {
		String targetPrefix = path.substring(0, path.indexOf("[*") + 1);
		int oldIndexNumber = -1;
		if(targetPrefix != null ) {
			for(String cKey : c.keySet()) {
				if(cKey.indexOf(targetPrefix) == 0) {
					String oldIndex = cKey.substring(cKey.indexOf("[")+1, cKey.indexOf("]"));
					oldIndexNumber = Integer.parseInt(oldIndex);
					
					
				}
			}		
			
		}		
		return oldIndexNumber;
	}
	
	private void addTargetNodes(Map<String, Object> c, MappingDTO mapping, Object value, int valueIndex, Set<String> addedCollections) {
		List<NodeInfo> targets = mapping.getTarget();
		int prevIndex = 0;
		boolean prevFound = false;
		for(int i = 0; i < targets.size(); i++) {
			Object newValue = value;
			NodeInfo target = targets.get(i);
			String targetPath = target.getId();
			if(target.getProcessing() != null && value != null ) {
				newValue = doProcessing(target.getProcessing(), value);
			}
			if(newValue != null) {
				if(value.toString().equals("[\"##missing##\"]") || value.toString().equals("##missing##"))  {
					continue;
				}
				
				if(targetPath.contains("[*]")) {
					// check if collection path exists in the c and add index accordingly 
					if(!prevFound) {
						prevIndex = getPrevIndex(c, targetPath, valueIndex);	
						// add one if the source collection key has not yet been added 
						boolean addedIndex = false;
						for(NodeInfo source : mapping.getSource()) {
							String sourcePath = source.getId();
							if(sourcePath.indexOf("]") > 0) {
								if(addedCollections.contains(sourcePath.substring(0, sourcePath.indexOf(("]"))))) {
									addedIndex = true;
								}								
							}
							else {
								/*
								if(addedCollections.contains(sourcePath)) {
									addedIndex = true;
								}*/
								//for(NodeInfo target: mapping.getTarget()) {
								//	String targetPath = target.getId();
									if(targetPath.indexOf("[*") > 0) {
										addedCollections.add(sourcePath);		
									}
									
								//}								
								
							}
						}
						if(!addedIndex) {
							prevIndex = prevIndex + 1;
						}
						else {
							
							prevIndex = valueIndex;
						}
						prevFound = true;
						
					}
					c.put(targetPath.replace("*", ""+ prevIndex), newValue);					
				}
				else {
					c.put(targetPath, newValue);	
				}			
				
			}
			
		}
			
		
	}

}
