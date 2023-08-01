package fi.vm.yti.datamodel.api.v2.dto;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class MSCR {

    private MSCR(){
        //property class
    }

    public static final String URI ="http://uri.suomi.fi/datamodel/ns/mscr#";

    public static final Property format = ResourceFactory.createProperty(URI, "format");
    public static final Property latestVersion = ResourceFactory.createProperty(URI, "latestVersion");
    public static final Property versions = ResourceFactory.createProperty(URI, "versions");
    public static final Resource CROSSWALK = ResourceFactory.createResource(URI + "Crosswalk");
    public static final Resource SCHEMA = ResourceFactory.createResource(URI + "Schema");

    // use it when generating rdf
    public static final Resource NULL = ResourceFactory.createResource(URI + "null");
    
    public static final Property localName = ResourceFactory.createProperty(URI, "localName");
    public static final Property sourceSchema = ResourceFactory.createProperty(URI, "sourceSchema");
    public static final Property targetSchema = ResourceFactory.createProperty(URI, "targetSchema");
    
}