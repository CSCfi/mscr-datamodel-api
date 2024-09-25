package fi.vm.yti.datamodel.api.v2.transformation.functions;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.text.StringSubstitutor;

import com.fasterxml.jackson.databind.JsonNode;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import info.debatty.java.stringsimilarity.Levenshtein;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import static java.util.stream.Collectors.joining;

public class SimpleMappingFunctions {

	
	public static Map<String, Set<String>> vocabularies = new HashMap();
	
	static {
		vocabularies.put(
			"pid:voc1",
			Set.of(
					"Engineering Sciences#Computer Science, Electrical and System Engineering",
			    	"Computer Science, Electrical and System Engineering#Systems Engineering",
		    		"Systems Engineering#Automation, Control Systems, Robotics, Mechatronics",
		    		"Systems Engineering#Measurement Systems",
		    		"Systems Engineering#Microsystems",
		    		"Systems Engineering#Traffic and Transport Systems, Logistics",
		    		"Systems Engineering#Human Factors, Ergonomics, Human-Machine Systems",
			    	"Computer Science, Electrical and System Engineering#Electrical Engineering",
		    		"Electrical Engineering#Electronic Semiconductors, Components, Circuits, Systems",
		    		"Electrical Engineering#Communication, High-Frequency and Network Technology, Theoretical Electrical Engineering",
		    		"Electrical Engineering#Electrical Energy Generation, Distribution, Application","5.4.2.4#Electrical Engineering#Radio Science and Radar Technology",
			    	"Computer Science, Electrical and System Engineering#Computer Science",
		    		"Computer Science#Theoretical Computer Science",
		    		"Computer Science#Software Technology",
		    		"Computer Science#Operating, Communication, Library and Information Systems",
		    		"Computer Science#Artificial Intelligence, Image and Language Processing",
		    		"Computer Science#Computer Architecture, Computer Engineering and Embedded Systems",
		    		"Computer Science#Information Science",
		    		"Computer Science#Research Data Management"		
			)				
		);
		
	}
	public static String stringToString(String input) {
		return input;
	}
	
	public static int stringToInt(String input) throws NumberFormatException {
		return Integer.parseInt(input);
	}
	public static Double stringToDouble(String input) throws NumberFormatException {
		return Double.parseDouble(input);
	}

	public static String anyToString(Object input) {
		return ""+input;
	}

	public static Double celsiusToFahrenheit(Double input) {
		return input*(1.8) + 32;
	}
	
	public static String simpleCoordinateToComplex(String input) {
		String[] parts = input.split(",");
		return "<coordinate><lang>"+parts[0] + "</lang><long>"+ parts[1] + "</long></coordinate>";
	}
	
	public static String customCoordinateToString(Object input) {
		System.out.println(input);
		System.out.println(input.getClass().getCanonicalName());

		if(input instanceof JSONArray) {
			final List<String> r = new ArrayList<String>();
			JSONArray a = (JSONArray)input;
			
			a.forEach(_v -> {
				Map<String, Object> v = (Map<String, Object>)_v;
				r.add(v.get("lat") + "," + v.get("long"));
			});
			
			return  r.stream()				      
				      .collect(Collectors.joining("|"));
		}
		else {
			Map<String, Object> v = (Map<String, Object>)input;
			return v.get("lat") + "," + v.get("long");			
		}
		
		//JSONObject obj = new JSONObject(Map.of("test", "test"));
	}
	
	public static String configurableObjectToString(Object value, Map<String, Object> params) {
		Object propertiesList = params.get("propertiesList");
		String separator = (String)params.get("separator");
		Map<String, Object> v = (Map<String, Object>)value;
		List<String> props = (List<String>)propertiesList;
		List<String> values = new ArrayList();
		props.forEach(_p -> {
			if(v.containsKey(_p)) {
				values.add((String)v.get(_p));
			}
		});
		
		
		return values.stream()				      
			      .collect(Collectors.joining(separator));
	}
	
	public static String configurableObjectToSimple(Object value, Map<String, Object> params) {
		Object propertiesList = params.get("propertiesList");
		String separator = ",";		
		Map<String, Object> v = (Map<String, Object>)value;
		List<String> props = (List<String>)propertiesList;
		List<String> values = new ArrayList();
		props.forEach(_p -> {
			if(v.containsKey(_p)) {
				values.add((String)v.get(_p));
			}
		});
		
		
		return values.stream()				      
			      .collect(Collectors.joining(separator));
	}
	
	public static Map<String, Object> configurableStringToObject(Object value, Map<String, Object> params) {
		List<String> propertiesList = (List<String>) params.get("propertiesList");
		String separator = (String)params.get("separator");

		String[] parts = ((String)value).split(separator);
		int max = propertiesList.size() > parts.length ? parts.length : propertiesList.size();
		
		
		Map<String, Object> obj = new HashMap<String, Object>();
		for(int i = 0; i < max; i++) {
			obj.put(propertiesList.get(i), parts[i]);
		}		
		return obj; 
	}
	
	public static Object stringToXmlObject(Object node, Map<String, Object> params) {
		if(node instanceof JSONArray) {
			JSONArray a = new JSONArray();
			for(Object _node : (JSONArray)node) {
				if(_node instanceof String) {
					String prop = (String)params.get("property");
					Map<String, Object> obj = new HashMap<String, Object>();
					obj.put(prop, _node);
					a.add(obj);
				}
				else {
					a.add(_node);
				}
			}
			return a;
		}
		else {
			if(node instanceof String) {
				String prop = (String)params.get("property");
				Map<String, Object> obj = new HashMap<String, Object>();
				obj.put(prop, node);
				return obj;			
			}
			return node;
		}
	}
	
	public static Object configurableObjectToParams(Object value, Map<String, Object> params) {
		if(value instanceof JSONArray) {
			List<Object> r = new ArrayList<Object>();
			for(Object _value : (JSONArray)value) {
				Map<String, Object> props = (Map<String, Object>)_value;
				for(String sourceProperty : params.keySet()) {
					String targetParam = params.get(sourceProperty).toString();
					
					props.put(targetParam, props.get(sourceProperty));
				}
				r.add(props);
			}
			return r;
			
			
		}
		return "";
	}
	
	public static Object copyMap(Object value) {
		return (Map<String, Object>)value;
		
	}
	
	public static Object formatString(Object value) {
		if(value == null) {
			return "";
		}
		if(value instanceof JSONArray) {
			List<String> values = new ArrayList<String>();
			JSONArray a = (JSONArray)value;
			Iterator<Object> iter = a.iterator();
			while(iter.hasNext()) {
				values.add(iter.next().toString().trim());
			}
			return values;
		}
		else {
			return value.toString().trim();	
		}
				
	}
	
	public static String concatenateObject(Object value) {
		Map<String, Object> values = (Map<String, Object>)value;
		return values.values().stream().map(Object::toString).collect(joining("|"));
	}
	
	public static Object pickFirst(Object value) {
		if(value instanceof List) {
			return ((List)value).get(0);			
		}
		if(value instanceof JSONArray) {			
			return ((JSONArray)value).get(0);
		}
		return value;
		
	}
	
	public static Object simpleReplaceString(Object value, Map<String, Object> params) {
		if(value instanceof List) {	
			List<Object> list = (List<Object>)value;
			List<String> r = new ArrayList<String>();
			for(Object _value : list) {
				String whatToFind = (String) params.get("old");
				String replacement = (String) params.get("new");
				r.add( _value.toString().replace(whatToFind, replacement) );
				
			}
			return r;
		}
		else {
			String whatToFind = (String) params.get("old");
			String replacement = (String) params.get("new");
			return value.toString().replace(whatToFind, replacement);
			
		}
		
		
	}
	public static Object propertiesToArray(Object value, Map<String, Object> params) {		
		List<String> sourceProps = (List<String>) params.get("props");
		String targetProp = params.get("targetProp").toString();
		String sourcePropTarget = (String)params.get("sourcePropTarget");
		
		
		if(value instanceof JSONArray) {
			JSONArray values = (JSONArray)value;
			for(Object v : values) {
				JSONArray a = new JSONArray();
				for(String sourceProp : sourceProps) {
					Map<String, Object> newValue = new HashMap<String, Object>();
					Object _v = ((Map<String, Object>)v).get(sourceProp);
					if(_v != null) {
						newValue.put("#text", _v);
						if(sourcePropTarget != null) {
							newValue.put(sourcePropTarget, sourceProp);	
						}
						
						a.add(newValue);
						
					}
				}
				((Map<String, Object>)v).put(targetProp, a);
				
			}
		}
		else {
			JSONArray a = new JSONArray();
			for(String sourceProp : sourceProps) {
				
				Map<String, Object> newValue = new HashMap<String, Object>();
				Object _v = ((Map<String, Object>)value).get(sourceProp);
				if(_v != null) {
					newValue.put("#text", _v);
					if(sourcePropTarget != null) {
						newValue.put(sourcePropTarget, sourceProp);	
					}
					
					a.add(newValue);
					((Map<String, Object>)value).put(targetProp, newValue);
				}
			}	
			//((Map<String, Object>)value).put(targetProp, a);
			
		}
		return value;						
	}
	
	public static Object pickPropertiesToObject(Object value, Map<String, Object> params) {
		
		
		List<String> sourceProps = (List<String>) params.get("sourceProps");
		List<String> targetProps = (List<String>) params.get("targetProps");

		Map<String, Object> sourceObj = (Map<String, Object>) value;
		Map<String, Object> targetObj = new HashMap<String, Object>();
		
		int i = 0;
		for(String sourceProp : sourceProps) {
			targetObj.put(targetProps.get(i), sourceObj.get(sourceProp));
			
		}
		return targetObj;
	
	}
	
	public static Object pickProperty(Object value, Map<String, Object> params) {
		if(params.get("needle") instanceof List) {
			JSONArray list = new JSONArray();
			for(Object key : (List)params.get("needle")) {
				Object v = ((Map<String, Object>)value).get(key);
				if(v != null) {
					list.add(v);	
				}
			}
			return list;
		}
		String key = params.get("needle").toString();
		boolean flattenArray = true;
		if(params.containsKey("flattenArray")) {
			flattenArray = Boolean.parseBoolean(params.get("flattenArray").toString());
		}
		if(value instanceof List) {			
			List<Object> results = new ArrayList<Object>();
			List<Object> values = (List<Object>)value;
			for(Object _value : values) {
				Map<String, Object> _values = (Map<String, Object>)_value;
				if(_values.containsKey(key)) {
					results.add(_values.get(key));					
				}				
				else {
					results.add(null);
				}
				
			}
			if(flattenArray) {
				return results.stream().map(Object::toString).collect(joining(""));
			}
			return results;
		}
		else if(value instanceof JSONArray) {			
			List<Object> results = new ArrayList<Object>();
			JSONArray values = (JSONArray)value;
			for(Object _value : values) {
				Map<String, Object> _values = (Map<String, Object>)_value;
				if(_values.containsKey(key)) {
					results.add(_values.get(key));					
				}				
				else {
					results.add(null);
				}
				
			}
			if(flattenArray) {
				return results.stream().map(Object::toString).collect(joining(""));
			}
			
			return results;
		}
		else if(value instanceof String) {
			return value;
		}
		else {
			Map<String, Object> values = (Map<String, Object>)value;
			if(values.containsKey(key)) {
				return values.get(key);
			}
		}
		return null;
	}
	
	public static Object pickPropertyWithJSONPath(Object value, Map<String, Object> params) {
		String path = params.get("path").toString();
		DocumentContext doc = JsonPath.parse(value);
		Object _value = null;
		try {
			_value = doc.read(path);
		}catch(PathNotFoundException ex) {
			ex.printStackTrace();
		}
		return _value;
	}	
	
	private static String handleDataciteToB2FindCreator(Map<String, Object> obj) {
		String suffix = "";
		if(obj.containsKey("nameIdentifier")) {
			Map<String, Object> ni = (Map<String, Object>) obj.get("nameIdentifier");
			if(ni.containsKey("-nameIdentifierScheme") && ni.get("-nameIdentifierScheme").toString().equals("ORCID")) {
				suffix =  " (" + ni.get("#text") + ")";
				
			}
		}
		
		return ((Map<String, Object>)obj.get("creatorName")).get("#text").toString() + suffix;
	}
	public static Object dataciteCreatorToB2Find(Object value, Map<String, Object> params) {
		if(value instanceof JSONArray) {
			List<String> list = new ArrayList<String>();
			JSONArray a = (JSONArray)value;
			for(Object _obj : a) {
				list.add(handleDataciteToB2FindCreator((Map<String, Object>)_obj));
			}
			return list;
			
		}
		else {
			return handleDataciteToB2FindCreator((Map<String, Object>)value);
		}

	}
	
	
	public static Object concatLists(Object value, Map<String, Object> params) {
		Map<String, Object> obj = (Map<String, Object>)value;
		List<Object> list = new ArrayList<Object>();
		for(String key : params.keySet()) {
			Object values = obj.get(params.get(key));
			if(!(values instanceof List)) {
				list.add(values);
			}
			else {
				list.addAll((List<Object>)values);
			}
		}
		return list;
		

	}
	
	public static String concat(List<String> values, String delimiter) {
		return values.stream().map(Object::toString).collect(joining(delimiter));
	}
	
	public static List<String> split(List<String> values, String delimiter) {
		return Arrays.asList(concat(values, delimiter).split(delimiter));
	}
		
	
	public static Object formatStringWithSubstitutor(Object value, Map<String, Object> params) {

		if(value instanceof JSONArray) {
			List<Object> r = new ArrayList<Object>();
			for(Object _value : (JSONArray)value) {
				String pattern = (String)params.get("pattern");
				Map<String, Object> values = (Map<String, Object>)_value;
				StringSubstitutor sb = new StringSubstitutor(values);
				r.add(sb.replace(pattern));
			}
			return r;
			
			
		}
		if(value instanceof List) {
			List<Object> r = new ArrayList<Object>();
			for(Object _value : (List)value) {
				String pattern = (String)params.get("pattern");
				Map<String, Object> values = (Map<String, Object>)_value;
				StringSubstitutor sb = new StringSubstitutor(values);
				r.add(sb.replace(pattern));
			}
			return r;
			
			
		}		
		else {
			String pattern = (String)params.get("pattern");
			Map<String, Object> values = (Map<String, Object>)value;
			StringSubstitutor sb = new StringSubstitutor(values);
			return sb.replace(pattern);
			
		}
	}
	
	public static String prefixString(Object node, Map<String, Object> params) {
		String value = (String)params.get("prefix");
		return value + node;
	}	
	
	public static Object formatUrl(String value) {
		String v = (String)formatString(value);
		URL url = null;
		try {
			url = new URL(v);
		} catch (MalformedURLException e) {
		}
		if((url != null && url.getProtocol().equals("doi")) || v.startsWith("10.")) {
			return String.format("https://doi.org/%s", v);
		}
			
		return v;
		
	}
	
	public static Object similarityBasedValueMapping(Object node, Map<String, Object> params) {
		Double threshold = (Double)params.get("threshold");
		String vocabulary = (String)params.get("targetVocabulary");
		Levenshtein l = new Levenshtein();
		String _default = "Other";
		final Set<String> matches = new HashSet<String>();
		String[] tokens = (node.toString()).split(" ");
		for(int i = 0; i < tokens.length; i++) {
			String token = tokens[i];
			vocabularies.get(vocabulary).forEach(target -> {
				double ratio = l.distance(token.toLowerCase(), target.toLowerCase());
				if(ratio > threshold) {
					matches.add(target.toString());
				}
						
			});	
		}
		List<String> r = matches.stream().collect(Collectors.toList());
		if(r.isEmpty()) {
			r.add(_default);
		}
		return r.toArray(new String[0]);
	}
	
	public static Object mapVocabularies(Object node, Map<String, Object> params) {
		String targetVocabulary = (String)params.get("targetVocabulary");
		String sourceVocabulary = (String)params.get("sourceVocabulary");
		Map<String, VocabularyItem> crosswalk = getCrosswalk(sourceVocabulary, targetVocabulary);
		if(crosswalk.containsKey(node.toString())) {
			VocabularyItem i = crosswalk.get(node.toString());
			return Map.of(
					"uri", i.uri,
					"label", i.label
					);
		}
		return null;
	}	
	
	record VocabularyItem(String uri, String label) {};
	private static Map<String, VocabularyItem> getCrosswalk(String sourceVocabulary, String targetVocabulary) {
		var types = Map.of(
				"Collection", new VocabularyItem("http://purl.org/dc/dcmitype/Collection", "Collection"),
				"Dataset", new VocabularyItem("http://purl.org/dc/dcmitype/Dataset", "Dataset"),
				"Event", new VocabularyItem("http://purl.org/dc/dcmitype/Event", "Event"),
				"Image", new VocabularyItem("http://purl.org/dc/dcmitype/Image", "Image")				
				);
		
		var langs = Map.of(
				"en", new VocabularyItem("http://purl.org/lang/en", "English"),
				"fi", new VocabularyItem("http://purl.org/lang/fi", "Finnish"),
				"sv", new VocabularyItem("http://purl.org/lang/sv", "Swedish")
				);
		
		if(sourceVocabulary.equals("typesvoc")) {
			return types;
		}
		else {
			return langs;
		}
	}

	public static String formatDate(String input, Map<String, Object> params) {
		var inputFormat = params.get("inputFormat");
		var outputFormat = params.get("outputFormat");
		if(inputFormat.equals("timemillis") && outputFormat.equals("javaDate")) {
			return new SimpleDateFormat((String)params.get("outputFormatDetails")).format(new Date(Long.parseLong(input.trim())));
		}
		return input;
	}
	
	public static Object vocabularyMapper(Object input, Map<String, Object> params) {
		return input;
	}
	
	public static Object staticContent(Object input, Map<String, Object> params) {
		return params.get("value");
		
	}
	
	public static String constant(List<String> values, String  value) {
		return value;
	}
	
	/* CLARIN */
	public static String clarinToFullDate(Object node) {
		return "2010-05-01";
		
	}
}

