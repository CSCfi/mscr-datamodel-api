package fi.vm.yti.datamodel.api.v2.mapper;

import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexModel;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.service.JenaQueryException;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.security.YtiUser;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Service
public class ModelMapper {

    private final Logger log = LoggerFactory.getLogger(ModelMapper.class);

    private final JenaService jenaService;
    private final String defaultNamespace;
    
    public ModelMapper (JenaService jenaService,
    		@Value("${defaultNamespace}") String defaultNamespace){
        this.jenaService = jenaService;
        this.defaultNamespace = defaultNamespace;
    }

    /**
     * Map DataModelDTO to Jena model
     * @param modelDTO Data Model DTO
     * @return Model
     */
    public Model mapToJenaModel(DataModelDTO modelDTO, YtiUser user) {
        log.info("Mapping DatamodelDTO to Jena Model");
        var model = ModelFactory.createDefaultModel();
        var modelUri = defaultNamespace + modelDTO.getPrefix();
        // TODO: type of application profile?
        model.setNsPrefixes(ModelConstants.PREFIXES);
        Resource type = modelDTO.getType().equals(ModelType.LIBRARY)
                ? OWL.Ontology
                : DCAP.DCAP;

        var creationDate = new XSDDateTime(Calendar.getInstance());
        var modelResource = model.createResource(modelUri)
                .addProperty(RDF.type, type)
                .addProperty(OWL.versionInfo, modelDTO.getStatus().name())
                .addProperty(DCTerms.identifier, UUID.randomUUID().toString())
                .addProperty(DCTerms.modified, ResourceFactory.createTypedLiteral(creationDate))
                .addProperty(DCTerms.created, ResourceFactory.createTypedLiteral(creationDate))
                .addProperty(Iow.creator, user.getId().toString())
                .addProperty(Iow.modifier, user.getId().toString());

        modelDTO.getLanguages().forEach(lang -> modelResource.addProperty(DCTerms.language, lang));

        modelResource.addProperty(Iow.contentModified, ResourceFactory.createTypedLiteral(creationDate));
        MapperUtils.addOptionalStringProperty(modelResource, Iow.contact, modelDTO.getContact());

        modelResource.addProperty(DCAP.preferredXMLNamespacePrefix, modelDTO.getPrefix());
        modelResource.addProperty(DCAP.preferredXMLNamespace, modelUri);

        MapperUtils.addLocalizedProperty(modelDTO.getLanguages(), modelDTO.getLabel(), modelResource, RDFS.label, model);
        MapperUtils.addLocalizedProperty(modelDTO.getLanguages(), modelDTO.getDescription(), modelResource, RDFS.comment, model);

        var groupModel = jenaService.getServiceCategories();
        modelDTO.getGroups().forEach(group -> {
            var groups = groupModel.listResourcesWithProperty(SKOS.notation, group);
            if (groups.hasNext()) {
                modelResource.addProperty(DCTerms.isPartOf, groups.next());
            }
        });

        addOrgsToModel(modelDTO, modelResource);

        addInternalNamespaceToDatamodel(modelDTO, modelResource, model);
        addExternalNamespaceToDatamodel(modelDTO, model, modelResource);

        modelDTO.getTerminologies().forEach(terminology -> modelResource.addProperty(DCTerms.references, ResourceFactory.createResource(terminology)));

        model.setNsPrefix(modelDTO.getPrefix(), modelUri + "#");

        return model;
    }


    public Model mapToUpdateJenaModel(String prefix, DataModelDTO dataModelDTO, Model model, YtiUser user){
        var updateDate = new XSDDateTime(Calendar.getInstance());
        var modelResource = model.getResource(defaultNamespace + prefix);

        //update languages before getting and using the languages for localized properties
        if(dataModelDTO.getLanguages() != null){
            modelResource.removeAll(DCTerms.language);
            dataModelDTO.getLanguages().forEach(lang -> modelResource.addProperty(DCTerms.language, lang));
        }

        var langs = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.language);

        var status = dataModelDTO.getStatus();
        if (status != null) {
            MapperUtils.updateStringProperty(modelResource, OWL.versionInfo, status.name());
        }

        MapperUtils.updateLocalizedProperty(langs, dataModelDTO.getLabel(), modelResource, RDFS.label, model);
        MapperUtils.updateLocalizedProperty(langs, dataModelDTO.getDescription(), modelResource, RDFS.comment, model);
        MapperUtils.updateStringProperty(modelResource, Iow.contact, dataModelDTO.getContact());

        if(dataModelDTO.getGroups() != null){
            modelResource.removeAll(DCTerms.isPartOf);
            var groupModel = jenaService.getServiceCategories();
            dataModelDTO.getGroups().forEach(group -> {
                var groups = groupModel.listResourcesWithProperty(SKOS.notation, group);
                if (groups.hasNext()) {
                    modelResource.addProperty(DCTerms.isPartOf, groups.next());
                }
            });
        }

        if(dataModelDTO.getOrganizations() != null){
            modelResource.removeAll(DCTerms.contributor);
            addOrgsToModel(dataModelDTO, modelResource);
        }

        if(dataModelDTO.getInternalNamespaces() != null){
            removeNamespacesFromModel(model, modelResource.listProperties(DCTerms.requires), true);
            removeNamespacesFromModel(model, modelResource.listProperties(OWL.imports), true);
            addInternalNamespaceToDatamodel(dataModelDTO, modelResource, model);
        }

        if(dataModelDTO.getExternalNamespaces() != null){
            removeNamespacesFromModel(model, modelResource.listProperties(DCTerms.requires), false);
            removeNamespacesFromModel(model, modelResource.listProperties(OWL.imports), false);
            addExternalNamespaceToDatamodel(dataModelDTO, model, modelResource);
        }

        if (dataModelDTO.getTerminologies() != null) {
            modelResource.removeAll(DCTerms.references);
            dataModelDTO.getTerminologies().forEach(terminology ->
                    modelResource.addProperty(DCTerms.references, ResourceFactory.createResource(terminology))
            );
        }

        modelResource.removeAll(DCTerms.modified);
        modelResource.addProperty(DCTerms.modified, ResourceFactory.createTypedLiteral(updateDate));
        modelResource.removeAll(Iow.modifier);
        modelResource.addProperty(Iow.modifier, user.getId().toString());
        return model;
    }

    /**
     * Remove internal of external namespaces from model
     * @param statements Iterator of property values
     * @param internal if true remove internal namespaces, if false remove external namespaces
     */
    private void removeNamespacesFromModel(Model model, StmtIterator statements, boolean internal){
        while(statements.hasNext()){
            var next = statements.next();
            var namespace = next.getObject().toString();
            if(internal) {
                if(namespace.startsWith(defaultNamespace)){
                    statements.remove();
                    model.removeNsPrefix(model.getNsURIPrefix(namespace));
                }
            }else{
                if(!namespace.startsWith(defaultNamespace)){
                    statements.remove();
                    model.removeNsPrefix(model.getNsURIPrefix(namespace));
                }
            }
        }
    }

    /**
     * Map a Model to DataModelExpandDTO
     *
     * @param prefix model prefix
     * @param model  Model
     * @return Data Model DTO
     */
    public DataModelInfoDTO mapToDataModelDTO(String prefix, Model model, Consumer<ResourceInfoBaseDTO> userMapper) {

        var datamodelDTO = new DataModelInfoDTO();
        datamodelDTO.setPrefix(prefix);

        var modelResource = model.getResource(defaultNamespace + prefix);

        var types = modelResource.listProperties(RDF.type).mapWith(Statement::getResource).toList();
        if(types.contains(DCAP.DCAP) || types.contains(ResourceFactory.createProperty("http://www.w3.org/2002/07/dcap#DCAP"))){
            datamodelDTO.setType(ModelType.PROFILE);
        }else if(types.contains(OWL.Ontology)){
            datamodelDTO.setType(ModelType.LIBRARY);
        }else{
            throw new MappingError("RDF:type not supported for data model");
        }

        var status = Status.valueOf(MapperUtils.propertyToString(modelResource, OWL.versionInfo));
        datamodelDTO.setStatus(status);

        //Language
        datamodelDTO.setLanguages(MapperUtils.arrayPropertyToSet(modelResource, DCTerms.language));

        //Label
        datamodelDTO.setLabel(MapperUtils.localizedPropertyToMap(modelResource, RDFS.label));

        //Description
        datamodelDTO.setDescription(MapperUtils.localizedPropertyToMap(modelResource, RDFS.comment));

        var existingGroups = jenaService.getServiceCategories();
        var groups = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.isPartOf);
        datamodelDTO.setGroups(ServiceCategoryMapper.mapServiceCategoriesToDTO(groups, existingGroups));

        var organizations = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.contributor);
        datamodelDTO.setOrganizations(OrganizationMapper.mapOrganizationsToDTO(organizations, jenaService.getOrganizations()));

        var created = modelResource.getProperty(DCTerms.created).getLiteral().getString();
        var modified = modelResource.getProperty(DCTerms.modified).getLiteral().getString();
        datamodelDTO.setCreated(created);
        datamodelDTO.setModified(modified);
        datamodelDTO.setCreator(new UserDTO(MapperUtils.propertyToString(modelResource, Iow.creator)));
        datamodelDTO.setModifier(new UserDTO(MapperUtils.propertyToString(modelResource, Iow.modifier)));

        var internalNamespaces = new HashSet<String>();
        var externalNamespaces = new HashSet<ExternalNamespaceDTO>();
        addNamespacesToList(internalNamespaces, externalNamespaces, model, modelResource, OWL.imports);
        addNamespacesToList(internalNamespaces, externalNamespaces, model, modelResource, DCTerms.requires);
        datamodelDTO.setInternalNamespaces(internalNamespaces);
        datamodelDTO.setExternalNamespaces(externalNamespaces);

        Set<TerminologyDTO> terminologies = modelResource.listProperties(DCTerms.references).toList().stream().map(ref -> {
            var graph = ref.getObject().toString();
            var terminologyModel = jenaService.getTerminology(graph);
            if (terminologyModel == null) {
                return new TerminologyDTO(graph);
            }
            return TerminologyMapper.mapToTerminologyDTO(graph, terminologyModel);
        }).collect(Collectors.toSet());

        datamodelDTO.setTerminologies(terminologies);

        if (userMapper != null) {
            userMapper.accept(datamodelDTO);
        }

        datamodelDTO.setContact(MapperUtils.propertyToString(modelResource, Iow.contact));
        return datamodelDTO;
    }

    /**
     * Map a DataModel to a DataModelDocument
     * @param prefix Prefix of model
     * @param model Model
     * @return Index model
     */
    public IndexModel mapToIndexModel(String prefix, Model model){
        var resource = model.getResource(defaultNamespace + prefix);
        var indexModel = new IndexModel();
        indexModel.setId(defaultNamespace + prefix);
        indexModel.setStatus(Status.valueOf(resource.getProperty(OWL.versionInfo).getString()));
        indexModel.setModified(resource.getProperty(DCTerms.modified).getString());
        indexModel.setCreated(resource.getProperty(DCTerms.created).getString());
        var contentModified = resource.getProperty(Iow.contentModified);
        if(contentModified != null) {
            indexModel.setContentModified(contentModified.getString());
        }
        var types = resource.listProperties(RDF.type).mapWith(Statement::getResource).toList();
        if(types.contains(DCAP.DCAP) || types.contains(ResourceFactory.createProperty("http://www.w3.org/2002/07/dcap#DCAP"))){
            indexModel.setType(ModelType.PROFILE);
        }else if(types.contains(OWL.Ontology)){
            indexModel.setType(ModelType.LIBRARY);
        }else{
            throw new MappingError("RDF:type not supported for data model");
        }
        indexModel.setPrefix(prefix);
        indexModel.setLabel(MapperUtils.localizedPropertyToMap(resource, RDFS.label));
        indexModel.setComment(MapperUtils.localizedPropertyToMap(resource, RDFS.comment));
        var contributors = new ArrayList<UUID>();
        resource.listProperties(DCTerms.contributor).forEach(contributor -> {
            var value = contributor.getObject().toString();
            contributors.add(MapperUtils.getUUID(value));
        });
        indexModel.setContributor(contributors);
        var isPartOf = MapperUtils.arrayPropertyToList(resource, DCTerms.isPartOf);
        var serviceCategories = jenaService.getServiceCategories();
        var groups = isPartOf.stream().map(serviceCat -> MapperUtils.propertyToString(serviceCategories.getResource(serviceCat), SKOS.notation)).collect(Collectors.toList());
        indexModel.setIsPartOf(groups);
        indexModel.setLanguage(MapperUtils.arrayPropertyToList(resource, DCTerms.language));

        indexModel.setDocumentation(MapperUtils.localizedPropertyToMap(resource, Iow.documentation));
        return indexModel;
    }

    /**
     * Add organizations to a model
     * @param modelDTO Payload to get organizations from
     * @param modelResource Model resource to add orgs to
     */
    private void addOrgsToModel(DataModelDTO modelDTO, Resource modelResource) {
        var organizationsModel = jenaService.getOrganizations();
        modelDTO.getOrganizations().forEach(org -> {
            var orgUri = ModelConstants.URN_UUID + org;
            var queryRes = ResourceFactory.createResource(orgUri);
            if(organizationsModel.containsResource(queryRes)){
                modelResource.addProperty(DCTerms.contributor, organizationsModel.getResource(orgUri));
            }
        });
    }

    /**
     * Add namespaces from model to two sets
     * @param intNs Internal namespaces set - Defined by having configured default namespace
     * @param extNs External namespaces set - Everything not internal
     * @param model Model to get external namespace information from
     * @param resource Model resource where the given property lies
     * @param property Property to get namespace reference from
     */
    private void addNamespacesToList(Set<String> intNs, Set<ExternalNamespaceDTO> extNs, Model model, Resource resource, Property property){
        resource.listProperties(property).forEach(prop -> {
            var ns = prop.getObject().toString();
            if(ns.startsWith(defaultNamespace)){
                intNs.add(ns);
            }else {
                var extNsModel = model.getResource(ns);
                var extNsDTO = new ExternalNamespaceDTO();
                extNsDTO.setNamespace(ns);
                extNsDTO.setPrefix(MapperUtils.propertyToString(extNsModel, DCAP.preferredXMLNamespacePrefix));
                extNsDTO.setName(MapperUtils.propertyToString(extNsModel, RDFS.label));
                extNs.add(extNsDTO);
            }
        });
    }

    /**
     * Add internal namespace to data model, if model type cannot be resolved the namespace won't be added
     * @param modelDTO Model DTO to get internal namespaces from
     * @param resource Data model resource to add linking property (OWL.imports or DCTerms.requires)
     */
    private void addInternalNamespaceToDatamodel(DataModelDTO modelDTO, Resource resource, Model model){
        modelDTO.getInternalNamespaces().forEach(namespace -> {
            var ns = jenaService.getDataModel(namespace);
            if(ns != null){
                var nsRes = ns.getResource(namespace);
                var prefix = MapperUtils.propertyToString(nsRes, DCAP.preferredXMLNamespacePrefix);
                var nsType = nsRes.getProperty(RDF.type).getResource();
                if(nsType.equals(OWL.Ontology)){
                    resource.addProperty(OWL.imports, nsRes);
                }else{
                    resource.addProperty(DCTerms.requires, nsRes);
                }
                model.setNsPrefix(prefix, namespace);
            }
        });
    }

    /**
     * Add external namespaces to data model
     * @param modelDTO Model DTO to get external namespaces from
     * @param model Data model to add namespace resource to
     * @param resource Data model resource to add linking property (OWL.imports or DCTerms.requires)
     */
    private void addExternalNamespaceToDatamodel(DataModelDTO modelDTO, Model model, Resource resource){
            modelDTO.getExternalNamespaces().forEach(namespace -> {
                var nsUri = namespace.getNamespace();
                var resolvedNamespace = ModelFactory.createDefaultModel();
                try{
                    resolvedNamespace.read(nsUri);
                    var extRes = resolvedNamespace.getResource(nsUri);
                    var resType = extRes.getProperty(RDF.type).getResource();

                    var nsRes = model.createResource(nsUri);
                    nsRes.addProperty(RDFS.label, namespace.getName());
                    nsRes.addProperty(DCAP.preferredXMLNamespacePrefix, namespace.getPrefix());
                    nsRes.addProperty(DCTerms.type, resType);
                    if(resType.equals(OWL.Ontology)){
                        resource.addProperty(OWL.imports, extRes);
                    }else{
                        resource.addProperty(DCTerms.requires, extRes);
                    }
                    model.setNsPrefix(namespace.getPrefix(), nsUri);
                }catch (HttpException ex){
                    if (ex.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
                        log.warn("Model not found with prefix {}", nsUri);
                        throw new ResourceNotFoundException(nsUri);
                    } else {
                        throw new JenaQueryException("Error fetching external namespace");
                    }
                }
            });
    }
}
