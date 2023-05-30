package fi.vm.yti.datamodel.api.v2.dto;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ModelConstants {

	
	private final String defaultNamespace;
	
	public ModelConstants(@Value("${defaultNamespace}") String defaultNamespace) {
		this.defaultNamespace = defaultNamespace;
	}
	
	public String getDefaultNamespace() {
		return this.defaultNamespace;
	}
	
    public static final String URN_UUID = "urn:uuid:";
    public static final String DEFAULT_LANGUAGE = "fi";
    public static final List<String> USED_LANGUAGES = List.of("fi", "sv", "en");
    public static final String ORGANIZATION_GRAPH = "urn:yti:organizations";
    public static final String SERVICE_CATEGORY_GRAPH = "urn:yti:servicecategories";
    public static final Map<String, String> PREFIXES = Map.of(
            "rdfs", "http://www.w3.org/2000/01/rdf-schema#",
            "rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#", //TODO this can be removed once migration for old rdf lists are done
            "dcterms", "http://purl.org/dc/terms/",
            "owl", "http://www.w3.org/2002/07/owl#",
            "dcap", "http://purl.org/ws-mmi-dc/terms/",
            "xsd", "http://www.w3.org/2001/XMLSchema#",
            "iow", "http://uri.suomi.fi/datamodel/ns/iow#",
            "skos", "http://www.w3.org/2004/02/skos/core#",
            "sh", "http://www.w3.org/ns/shacl#"
    );
}
