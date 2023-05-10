package fi.vm.yti.datamodel.api.v2.mapper;

import fi.vm.yti.datamodel.api.v2.dto.ModelType;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.shared.JenaException;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;

import java.util.*;

public class MapperUtils {

    private MapperUtils(){
        //Util class so we need to hide constructor
    }

    /**
     * Get UUID from urn
     * Will return null if urn cannot be parsed
     * @param urn URN string formatted as urn:uuid:{uuid}
     * @return UUID
     */
    public static UUID getUUID(String urn) {
        try {
            return UUID.fromString(urn.replace("urn:uuid:", ""));
        } catch (Exception e) {
            return null;
        }
    }

    public static String getModelIdFromNamespace(String namespace){
        return namespace.substring(namespace.lastIndexOf("/") + 1);
    }

    public static ModelType getModelTypeFromResource(Resource resource){
        var modelType = resource.getProperty(RDF.type).getResource();
        if(modelType.equals(OWL.Ontology)){
            return ModelType.LIBRARY;
        }else{
            return ModelType.PROFILE;
        }
    }

    /**
     * Localized property to Map of (language, value)
     * @param resource Resource to get property from
     * @param property Property type
     * @return Map of (language, value)
     */
    public static Map<String, String> localizedPropertyToMap(Resource resource, Property property){
        var map = new HashMap<String, String>();
        resource.listProperties(property).forEach(prop -> {
            var lang = prop.getLanguage();
            var value = prop.getString();
            map.put(lang, value);
        });
        return map;
    }

    /**
     * Add localized property to Jena model
     * @param data Map of (language, value)
     * @param resource Resource to add to
     * @param property Property to add
     * @param model Jena model to add to
     */
    public static void addLocalizedProperty(Set<String> languages,
                                            Map<String, String> data,
                                            Resource resource,
                                            Property property,
                                            Model model) {
        if (data == null || languages == null || languages.isEmpty()) {
            return;
        }

        data.forEach((lang, value) -> {
            if(!languages.contains(lang)){
                throw new MappingError("Model missing language for localized property {" + lang + "}");
            }
            resource.addProperty(property, model.createLiteral(value, lang));
        });
    }

    /**
     * Updates localized property
     * @param languages Languages of the datamodel, localized property has to be in language
     * @param data Data to add
     * @param resource Resource
     * @param property Property
     * @param model Model
     */
    public static void updateLocalizedProperty(Set<String> languages,
                                               Map<String, String> data,
                                               Resource resource,
                                               Property property,
                                               Model model) {
        if(data != null && languages != null && !languages.isEmpty()){
            resource.removeAll(property);
            addLocalizedProperty(languages, data, resource, property, model);
        }
    }

    /**
     * Convert array property to list of strings
     * @param resource Resource to get property from
     * @param property Property type
     * @return List of property values
     */
    public static List<String> arrayPropertyToList(Resource resource, Property property){
        var list = new ArrayList<String>();
        try{
            var statement = resource.getProperty(property);
            if (statement == null) {
                return list;
            }
            statement.getList()
                    .asJavaList()
                    .forEach(node -> list.add(node.toString()));
        }catch(JenaException ex){
            //if item could not be gotten as list it means it is multiple statements of the property
            resource.listProperties(property)
                    .forEach(val -> list.add(val.getObject().toString()));
        }
        return list;
    }

    /**
     * Convert array property to set of strings
     * @param resource Resource to get property from
     * @param property Property type
     * @return Set of property values, empty if property is not found
     */
    public static Set<String> arrayPropertyToSet(Resource resource, Property property){
        var list = new HashSet<String>();
        try{
            var statement = resource.getProperty(property);
            if (statement == null) {
                return list;
            }
            statement.getList()
                    .asJavaList()
                    .forEach(node -> list.add(node.toString()));
        }catch(JenaException ex){
            //if item could not be gotten as list it means it is multiple statements of the property
            resource.listProperties(property)
                    .forEach(val -> list.add(val.getObject().toString()));
        }
        return list;
    }

    /**
     * Convert property to String, with null checks to ensure no NullPointerException
     * @param resource Resource to get property from
     * @param property Property
     * @return String if property is found, null if not
     */
    public static String propertyToString(Resource resource, Property property){
        var prop = resource.getProperty(property);
        //null check for property
        if(prop == null){
            return null;
        }
        var object = prop.getObject();
        //null check for object
        return object == null ? null : object.toString();
    }

    /**
     * Update string property
     * If string is empty|blank value is removed
     * @param resource Resource
     * @param property Property
     * @param value Value
     */
    public static void updateStringProperty(Resource resource, Property property, String value){
        if(value != null){
            resource.removeAll(property);
            if(!value.isBlank()){
                resource.addProperty(property, value);
            }
        }
    }

    /**
     * Update Uri property
     * If string is empty|blank value is removed
     * @param resource Resource
     * @param property Property
     * @param value Value
     */
    public static void updateUriProperty(Resource resource, Property property, String value){
        if(value != null){
            resource.removeAll(property);
            if(!value.isBlank()){
                resource.addProperty(property, ResourceFactory.createResource(value));
            }
        }
    }

    /**
     * Adds an optional string property
     * This has a null check, so it does not need to be separately added
     * @param resource Resource
     * @param property Property
     * @param value Value
     */
    public static void addOptionalStringProperty(Resource resource, Property property, String value){
        if(value != null && !value.isBlank()){
            resource.addProperty(property, value);
        }
    }

    public static void addOptionalUriProperty(Resource resource, Property property, String value){
        if(value != null && !value.isBlank()){
            resource.addProperty(property, ResourceFactory.createResource(value));
        }
    }

    /**
     * Add resource relationship to resource.
     * Resource namespace needs to be in data model (owlImports or dcTermsRequires)
     * @param owlImports Owl imports
     * @param dcTermsRequires DcTerms requires
     * @param resource Resource
     * @param property Property
     * @param resourceUri Resource URI
     */
    public static void addResourceRelationship(Set<String> owlImports, Set<String> dcTermsRequires, Resource resource, Property property, String resourceUri){
        var namespace = NodeFactory.createURI(resourceUri).getNameSpace().replace("#", "");
        var ownNamespace = resource.getNameSpace().replace("#", "");
        if(!ownNamespace.equals(namespace) &&!owlImports.contains(namespace) && !dcTermsRequires.contains(namespace)){
            throw new MappingError("Resource namespace not in owl:imports or dcterms:requires");
        }
        resource.addProperty(property, ResourceFactory.createResource(resourceUri));
    }

    /**
     * Checks if the type property (RDF:type) of the resource is particular type
     * @param resource Resource to check
     * @param type Type to check
     * @return if resource has given type
     */
    public static boolean hasType(Resource resource, Resource... type) {
        if (!resource.hasProperty(RDF.type)) {
            return false;
        }
        return Arrays.stream(type)
                .anyMatch(t -> t.equals(resource.getProperty(RDF.type).getResource()));
    }
    
    public static String getMSCRPrefix(String PID) {
        try {
            return "pid" + PID.replace("urn:IAMNOTAPID:", "").replace("-", "");
        } catch (Exception e) {
            return null;
        }
    }
    
    public static Map<String, String> listsToMap(String[] languages, String[] values) {
    	var map = new HashMap<String, String>();
    	for(var index = 0; index < languages.length; index++) {
    		map.put(languages[index], values[index]);
    	}
    	return map;
    }

}
