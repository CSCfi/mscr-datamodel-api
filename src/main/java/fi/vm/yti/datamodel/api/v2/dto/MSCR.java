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
    public static final Resource SCHEMAGROUP = ResourceFactory.createResource(URI + "SchemaGroup");
    public static final Resource SCHEMA = ResourceFactory.createResource(URI + "Schema");

    public static final Property localName = ResourceFactory.createProperty(URI, "localName");
    
}