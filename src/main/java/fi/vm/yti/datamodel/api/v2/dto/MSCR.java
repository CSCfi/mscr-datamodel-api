package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Map;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class MSCR {



	

	

	private MSCR(){
        //property class
    }
    
    public enum SOURCE_TYPE {
    	REFERENCED,
    	HOSTED
    }
    
    public static final String FUNCTIONS_GRAPH = "urn:mscr:functions";

    public static final String URI ="http://uri.suomi.fi/datamodel/ns/mscr#";
    public static final String FnO ="https://w3id.org/function/ontology#";

    public static final Property format = ResourceFactory.createProperty(URI, "format");
    public static final Property latestVersion = ResourceFactory.createProperty(URI, "latestVersion");
    public static final Property versions = ResourceFactory.createProperty(URI, "versions");
    public static final Resource CROSSWALK = ResourceFactory.createResource(URI + "Crosswalk");
    public static final Resource SCHEMA = ResourceFactory.createResource(URI + "Schema");

    public static final Resource MAPPINGSET = ResourceFactory.createResource(URI + "MappingSet");
    public static final Resource MAPPING = ResourceFactory.createResource(URI + "Mapping");
    public static final Resource FnO_FUNCTION = ResourceFactory.createResource(FnO + "Function");
    public static final Resource FnO_OUTPUT = ResourceFactory.createResource(FnO + "Output");
    public static final Resource FnO_PARAMETER = ResourceFactory.createResource(FnO + "Parameter");
    public static final Property FnO_name = ResourceFactory.createProperty(FnO + "name");
    public static final Property FnO_type = ResourceFactory.createProperty(FnO + "type");
    public static final Property FnO_required = ResourceFactory.createProperty(FnO + "required");
    public static final Property FnO_expects = ResourceFactory.createProperty(FnO + "expects");
    public static final Property FnO_returns = ResourceFactory.createProperty(FnO + "returns");
    


    // use it when generating rdf
    public static final Resource NULL = ResourceFactory.createResource(URI + "null");
    
    public static final Property localName = ResourceFactory.createProperty(URI, "localName");
    public static final Property sourceSchema = ResourceFactory.createProperty(URI, "sourceSchema");
    public static final Property targetSchema = ResourceFactory.createProperty(URI, "targetSchema");
    
    public static final Property aggregationKey = ResourceFactory.createProperty(URI, "aggregationKey");
    public static final Property namespace = ResourceFactory.createProperty(URI, "namespace");
    public static final Property versionLabel = ResourceFactory.createProperty(URI, "versionLabel");
    public static final Property revisions = ResourceFactory.createProperty(URI, "revisions");
    public static final Property variants = ResourceFactory.createProperty(URI, "variants");
    public static final Property hasRevision = ResourceFactory.createProperty(URI, "hasRevision");
    public static final Property numberOfRevisions = ResourceFactory.createProperty(URI, "numberOfRevisions");
    public static final Property state = ResourceFactory.createProperty(URI, "state");
    public static final Property visibility = ResourceFactory.createProperty(URI, "visibility");
    public static final Property owner = ResourceFactory.createProperty(URI, "owner");
    public static final Property sourceURL = ResourceFactory.createProperty(URI, "sourceURL");
    public static final Property customRoot = ResourceFactory.createProperty(URI, "customRoot");
    
    public static final Property PROV_wasRevisionOf = ResourceFactory.createProperty("http://www.w3.org/ns/prov#wasRevisionOf");

    public static final Property id = ResourceFactory.createProperty(URI, "id");
    public static final Property handle = ResourceFactory.createProperty(URI, "handle");
    public static final Property uri = ResourceFactory.createProperty(URI, "uri");
    public static final Property label = ResourceFactory.createProperty(URI, "label");
    public static final Property type = ResourceFactory.createProperty(URI, "type");
    public static final Property description = ResourceFactory.createProperty(URI, "description");
    public static final Property path = ResourceFactory.createProperty(URI, "path");
    public static final Property operator = ResourceFactory.createProperty(URI, "operator");
    public static final Property key = ResourceFactory.createProperty(URI, "key");
    public static final Property value = ResourceFactory.createProperty(URI, "value");
    public static final Property filter = ResourceFactory.createProperty(URI, "filter");
    public static final Property mappings = ResourceFactory.createProperty(URI, "mappings");
    public static final Property depth = ResourceFactory.createProperty(URI, "depth");
    public static final Property sourceType = ResourceFactory.createProperty(URI, "sourceType");
    public static final Resource sourceTypeAttribute = ResourceFactory.createResource(URI+ "sourceType/Attribute");
    
    public static final Property source = ResourceFactory.createProperty(URI, "source");
    public static final Property predicate = ResourceFactory.createProperty(URI, "predicate");
    public static final Property target = ResourceFactory.createProperty(URI, "target");

    public static final Property processing = ResourceFactory.createProperty(URI, "processing");
    public static final Property oneOf = ResourceFactory.createProperty(URI, "oneOf");
    public static final Property processingParams = ResourceFactory.createProperty(URI, "processingParams");
    public static final Property notes = ResourceFactory.createProperty(URI, "notes");
    
    public static final Property qname = ResourceFactory.createProperty(URI, "qname");

    public record Organization(String id,  Map<String, String> label) {}

}