package fi.vm.yti.datamodel.api.v2.mapper;

import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.security.YtiUser;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.shared.JenaException;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
        if(isApplicationProfile(resource)) {
            return ModelType.PROFILE;
        }else if(isLibrary(resource)) {
            return ModelType.LIBRARY;
        }
        return ModelType.PROFILE;
    }

    /**
     * Localized property to Map of (language, value). If no language specified for property
     * (e.g. external classes), handle that value as an english content
     * @param resource Resource to get property from
     * @param property Property type
     * @return Map of (language, value)
     */
    public static Map<String, String> localizedPropertyToMap(Resource resource, Property property){
        var map = new HashMap<String, String>();
        resource.listProperties(property).forEach(prop -> {
            var lang = prop.getLanguage();
            var value = prop.getString();
            if (lang == null || lang.trim().equals("")) {
                map.put("en", value);
            } else {
                map.put(lang, value);
            }
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

    public static <T> T getLiteral(Resource resource, Property property, Class<T> type) {
        var prop = resource.getProperty(property);
        if (prop == null){
            return null;
        }
        var literal = prop.getLiteral();

        if (type.equals(Integer.class)) {
            return type.cast(literal.getInt());
        } else if (type.equals(Boolean.class)) {
            return type.cast(literal.getBoolean());
        } else if (type.equals(Double.class)) {
            return type.cast(literal.getDouble());
        }
        return null;
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

    public static void addLiteral(Resource resource, Property property, Object value) {
        if (value != null) {
            resource.addLiteral(property, value);
        }
    }
    public static void updateLiteral(Resource resource, Property property, Object value){
        if (value != null) {
            resource.removeAll(property);
            resource.addLiteral(property, value);
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
        var namespace = NodeFactory.createURI(resourceUri).getNameSpace().replaceAll("/$", "");
        var ownNamespace = resource.getNameSpace().replaceAll("/$", "");
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
        var typeList = resource.listProperties(RDF.type).toList();
        return Arrays.stream(type)
                .anyMatch(t -> typeList.stream().anyMatch(r -> r.getResource().equals(t)));
    }

    public static boolean isApplicationProfile(Resource resource) {
        return hasType(resource, Iow.ApplicationProfile);
    }

    public static boolean isLibrary(Resource resource) {
        return hasType(resource, OWL.Ontology) && !hasType(resource, Iow.ApplicationProfile);
    }

    public static void addCreationMetadata(Resource resource, YtiUser user) {
        var creationDate = new XSDDateTime(Calendar.getInstance());
        resource.addProperty(DCTerms.modified, ResourceFactory.createTypedLiteral(creationDate))
                .addProperty(DCTerms.created, ResourceFactory.createTypedLiteral(creationDate))
                .addProperty(Iow.creator, user.getId().toString())
                .addProperty(Iow.modifier, user.getId().toString());
    }

    public static void addUpdateMetadata(Resource resource, YtiUser user) {
        var updateDate = new XSDDateTime(Calendar.getInstance());
        resource.removeAll(DCTerms.modified);
        resource.addProperty(DCTerms.modified, ResourceFactory.createTypedLiteral(updateDate));
        resource.removeAll(Iow.modifier);
        resource.addProperty(Iow.modifier, user.getId().toString());
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

    public static void mapCreationInfo(ResourceCommonDTO dto,
                                        Resource resource,
                                        Consumer<ResourceCommonDTO> userMapper) {
        var created = resource.getProperty(DCTerms.created).getLiteral().getString();
        var modified = resource.getProperty(DCTerms.modified).getLiteral().getString();
        dto.setCreated(created);
        dto.setModified(modified);
        dto.setCreator(new UserDTO(MapperUtils.propertyToString(resource, Iow.creator)));
        dto.setModifier(new UserDTO(MapperUtils.propertyToString(resource, Iow.modifier)));

        if (userMapper != null) {
            userMapper.accept(dto);
        }
    }

    public static UriDTO uriToURIDTO(String uri, Model model) {
        if (uri == null) {
            return null;
        }
        var u = NodeFactory.createURI(uri);
        var prefix = model.getNsURIPrefix(u.getNameSpace());
        if (prefix != null) {
            return new UriDTO(uri, String.format("%s:%s", prefix, u.getLocalName()));
        } else if (uri.startsWith(ModelConstants.SUOMI_FI_NAMESPACE)) {
            return new UriDTO(uri, uri
                    .replace(ModelConstants.SUOMI_FI_NAMESPACE, "")
                    .replace(ModelConstants.RESOURCE_SEPARATOR, ":")
            );
        } else {
            // use last element of the path as a prefix, if it could not determine
            var parts = u.getNameSpace().split("\\W");
            return new UriDTO(uri, String.format("%s:%s", parts[parts.length - 1], u.getLocalName()));
        }
    }

    public static Set<UriDTO> uriToURIDTOs(Collection<String> uris, Model model) {
        return uris.stream()
                .map(s -> uriToURIDTO(s, model))
                .collect(Collectors.toSet());
    }
}
