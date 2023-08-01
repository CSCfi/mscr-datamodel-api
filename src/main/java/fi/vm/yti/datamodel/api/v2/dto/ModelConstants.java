package fi.vm.yti.datamodel.api.v2.dto;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ModelConstants {

	
	private final String defaultNamespace;
	private final String defaultResolveBase;
	
	public ModelConstants(
			@Value("${defaultNamespace}") String defaultNamespace,
			@Value("${defaultResolveBase}") String defaultResolveBase) {
		this.defaultNamespace = defaultNamespace;
		this.defaultResolveBase = defaultResolveBase;
	}
	
	public String getDefaultNamespace() {
		return this.defaultNamespace;
	}
	
	public String getCodelistNamespace() {
		return defaultResolveBase + "/codelist/";
	}
	
	public String getTerminologyNamespace() {
		return defaultResolveBase + "/terminology/";
	}

    public static final String SUOMI_FI_NAMESPACE = "http://uri.suomi.fi/datamodel/ns/";
    public static final String CODELIST_NAMESPACE = "http://uri.suomi.fi/codelist/";
    public static final String TERMINOLOGY_NAMESPACE = "http://uri.suomi.fi/terminology/";
    public static final String MODEL_POSITIONS_NAMESPACE = "http://uri.suomi.fi/datamodel/positions/";

    public static final String RESOURCE_SEPARATOR = "/";
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
            "iow", "http://uri.suomi.fi/datamodel/ns/iow/",
            "skos", "http://www.w3.org/2004/02/skos/core#",
            "sh", "http://www.w3.org/ns/shacl#"
    );
    public static final List<String> SUPPORTED_DATA_TYPES = List.of(
            "owl:rational",
            "owl:real",
            "rdf:PlainLiteral",
            "rdf:XMLLiteral",
            "rdfs:Literal",
            "xsd:anyURI",
            "xsd:base64Binary",
            "xsd:boolean",
            "xsd:byte",
            "xsd:dateTime",
            "xsd:dateTimeStamp",
            "xsd:decimal",
            "xsd:double",
            "xsd:float",
            "xsd:hexBinary",
            "xsd:int",
            "xsd:integer",
            "xsd:language",
            "xsd:long",
            "xsd:Name",
            "xsd:NCName",
            "xsd:negativeInteger",
            "xsd:NMTOKEN",
            "xsd:nonNegativeInteger",
            "xsd:nonPositiveInteger",
            "xsd:normalizedString",
            "xsd:positiveInteger",
            "xsd:short",
            "xsd:string",
            "xsd:token",
            "xsd:unsignedByte",
            "xsd:unsignedInt",
            "xsd:unsignedLong",
            "xsd:unsignedShort"
    );
}
