package fi.vm.yti.datamodel.api.v2.mapper;

import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexModel;
import fi.vm.yti.datamodel.api.v2.repository.ConceptRepository;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import fi.vm.yti.datamodel.api.v2.repository.SchemesRepository;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.security.YtiUser;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
public class ModelMapper {

    private final Logger log = LoggerFactory.getLogger(ModelMapper.class);
    private final CoreRepository coreRepository;
    private final ConceptRepository conceptRepository;
    private final ImportsRepository importsRepository;
    private final SchemesRepository schemesRepository;

    private final JenaService jenaService;
    private final String defaultNamespace;
    private final ModelConstants modelConstants;
    
    public ModelMapper (JenaService jenaService,
    		@Value("${defaultNamespace}") String defaultNamespace,
    		ModelConstants modelConstants,
    		CoreRepository coreRepository,
            ConceptRepository conceptRepository,
            ImportsRepository importsRepository,
            SchemesRepository schemesRepository){
        this.jenaService = jenaService;
        this.defaultNamespace = defaultNamespace;
        this.modelConstants = modelConstants;
        this.coreRepository = coreRepository;
        this.conceptRepository = conceptRepository;
        this.importsRepository = importsRepository;
        this.schemesRepository = schemesRepository;
    }
        


    /**
     * Map DataModelDTO to Jena model
     * @param modelDTO Data Model DTO
     * @return Model
     */
    public Model mapToJenaModel(DataModelDTO modelDTO, ModelType modelType, YtiUser user) {
        log.info("Mapping DatamodelDTO to Jena Model");
        var model = ModelFactory.createDefaultModel();
        var modelUri = defaultNamespace + modelDTO.getPrefix();
        // TODO: type of application profile?
        model.setNsPrefixes(ModelConstants.PREFIXES);

        var creationDate = new XSDDateTime(Calendar.getInstance());
        var modelResource = model.createResource(modelUri)
                .addProperty(RDF.type, OWL.Ontology)
                .addProperty(OWL.versionInfo, modelDTO.getStatus().name())
                .addProperty(DCTerms.identifier, UUID.randomUUID().toString())
                .addProperty(Iow.contentModified, ResourceFactory.createTypedLiteral(creationDate))
                .addProperty(DCAP.preferredXMLNamespacePrefix, modelDTO.getPrefix())
                .addProperty(DCAP.preferredXMLNamespace, modelUri);

        MapperUtils.addCreationMetadata(modelResource, user);

        if (modelType.equals(ModelType.PROFILE)) {
            modelResource.addProperty(RDF.type, Iow.ApplicationProfile);
        }

        modelDTO.getLanguages().forEach(lang -> modelResource.addProperty(DCTerms.language, lang));

        MapperUtils.addOptionalStringProperty(modelResource, Iow.contact, modelDTO.getContact());
        MapperUtils.addLocalizedProperty(modelDTO.getLanguages(), modelDTO.getDocumentation(), modelResource, Iow.documentation, model);
        MapperUtils.addLocalizedProperty(modelDTO.getLanguages(), modelDTO.getLabel(), modelResource, RDFS.label, model);
        MapperUtils.addLocalizedProperty(modelDTO.getLanguages(), modelDTO.getDescription(), modelResource, RDFS.comment, model);

        var groupModel = coreRepository.getServiceCategories();
        modelDTO.getGroups().forEach(group -> {
            var groups = groupModel.listResourcesWithProperty(SKOS.notation, group);
            if (groups.hasNext()) {
                modelResource.addProperty(DCTerms.isPartOf, groups.next());
            }
        });

        addOrgsToModel(modelDTO, modelResource);

        addInternalNamespaceToDatamodel(modelDTO, modelResource, model);
        addExternalNamespaceToDatamodel(modelDTO, model, modelResource);

        modelDTO.getTerminologies().forEach(terminology -> MapperUtils.addOptionalUriProperty(modelResource, DCTerms.requires, terminology));

        if(modelType.equals(ModelType.PROFILE)) {
            modelDTO.getCodeLists().forEach(codeList -> MapperUtils.addOptionalUriProperty(modelResource, DCTerms.requires, codeList));
        }

        model.setNsPrefix(modelDTO.getPrefix(), modelUri + ModelConstants.RESOURCE_SEPARATOR);
        MapperUtils.addCreationMetadata(modelResource, user);

        return model;
    }


    public Model mapToUpdateJenaModel(String prefix, DataModelDTO dataModelDTO, Model model, YtiUser user){       
        var modelResource = model.getResource(defaultNamespace + prefix);
        var modelType = MapperUtils.getModelTypeFromResource(modelResource);

        //update languages before getting and using the languages for localized properties
        modelResource.removeAll(DCTerms.language);
        dataModelDTO.getLanguages().forEach(lang -> modelResource.addProperty(DCTerms.language, lang));

        var langs = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.language);

        MapperUtils.updateStringProperty(modelResource, OWL.versionInfo, dataModelDTO.getStatus().name());

        MapperUtils.updateLocalizedProperty(langs, dataModelDTO.getLabel(), modelResource, RDFS.label, model);
        MapperUtils.updateLocalizedProperty(langs, dataModelDTO.getDescription(), modelResource, RDFS.comment, model);
        MapperUtils.updateStringProperty(modelResource, Iow.contact, dataModelDTO.getContact());
        MapperUtils.updateLocalizedProperty(langs, dataModelDTO.getDocumentation(), modelResource, Iow.documentation, model);

        modelResource.removeAll(DCTerms.isPartOf);
        var groupModel = coreRepository.getServiceCategories();
        dataModelDTO.getGroups().forEach(group -> {
            var groups = groupModel.listResourcesWithProperty(SKOS.notation, group);
            if (groups.hasNext()) {
                modelResource.addProperty(DCTerms.isPartOf, groups.next());
            }
        });


        modelResource.removeAll(DCTerms.contributor);
        addOrgsToModel(dataModelDTO, modelResource);

        removeFromIterator(modelResource.listProperties(DCTerms.requires), val -> val.startsWith(modelConstants.getDefaultNamespace()));
        removeFromIterator(modelResource.listProperties(OWL.imports), val -> val.startsWith(modelConstants.getDefaultNamespace()));
        addInternalNamespaceToDatamodel(dataModelDTO, modelResource, model);

        removeFromIterator(modelResource.listProperties(DCTerms.requires), val -> !val.startsWith(modelConstants.getDefaultNamespace())
                && !val.startsWith(modelConstants.getCodelistNamespace()) && !val.startsWith(modelConstants.getTerminologyNamespace()));
        removeFromIterator(modelResource.listProperties(OWL.imports), val -> !val.startsWith(modelConstants.getDefaultNamespace())
                && !val.startsWith(modelConstants.getCodelistNamespace()) && !val.startsWith(modelConstants.getTerminologyNamespace()));
        addExternalNamespaceToDatamodel(dataModelDTO, model, modelResource);


        removeFromIterator(modelResource.listProperties(DCTerms.requires), val ->
                val.startsWith(modelConstants.getTerminologyNamespace()));
        dataModelDTO.getTerminologies().forEach(terminology -> MapperUtils.addOptionalUriProperty(modelResource, DCTerms.requires, terminology));

		if(MapperUtils.isApplicationProfile(modelResource)){
            removeFromIterator(modelResource.listProperties(DCTerms.requires), val -> val.startsWith(modelConstants.getCodelistNamespace()));
            dataModelDTO.getCodeLists().forEach(codeList -> MapperUtils.addOptionalUriProperty(modelResource, DCTerms.requires, codeList));
        }

        MapperUtils.addUpdateMetadata(modelResource, user);
        return model;
    }


    private void removeFromIterator(StmtIterator statements, Predicate<String> predicate){
        while(statements.hasNext()){
            var next = statements.next();
            var value = next.getObject().toString();
            if(predicate.test(value)){
                statements.remove();
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
    public DataModelInfoDTO mapToDataModelDTO(String prefix, Model model, Consumer<ResourceCommonDTO> userMapper) {

        var datamodelDTO = new DataModelInfoDTO();
        datamodelDTO.setPrefix(prefix);

        var modelResource = model.getResource(defaultNamespace + prefix);

        datamodelDTO.setType(MapperUtils.getModelTypeFromResource(modelResource));
        datamodelDTO.setStatus(Status.valueOf(MapperUtils.propertyToString(modelResource, OWL.versionInfo)));

        /*
        if(MapperUtils.isApplicationProfile(modelResource)){
            datamodelDTO.setType(ModelType.PROFILE);
        }else if(MapperUtils.isLibrary(modelResource)){
            datamodelDTO.setType(ModelType.LIBRARY);
        }else{
            throw new MappingError("RDF:type not supported for data model");
        }
        */

        //Language
        datamodelDTO.setLanguages(MapperUtils.arrayPropertyToSet(modelResource, DCTerms.language));
        datamodelDTO.setLabel(MapperUtils.localizedPropertyToMap(modelResource, RDFS.label));
        datamodelDTO.setDescription(MapperUtils.localizedPropertyToMap(modelResource, RDFS.comment));

        var groups = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.isPartOf);
        datamodelDTO.setGroups(ServiceCategoryMapper.mapServiceCategoriesToDTO(groups, coreRepository.getServiceCategories()));

        var organizations = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.contributor);
        datamodelDTO.setOrganizations(OrganizationMapper.mapOrganizationsToDTO(organizations, coreRepository.getOrganizations()));

        MapperUtils.mapCreationInfo(datamodelDTO, modelResource, userMapper);

        var internalNamespaces = new HashSet<String>();
        var externalNamespaces = new HashSet<ExternalNamespaceDTO>();
        addNamespacesToList(internalNamespaces, externalNamespaces, model, modelResource, OWL.imports);
        addNamespacesToList(internalNamespaces, externalNamespaces, model, modelResource, DCTerms.requires);
        datamodelDTO.setInternalNamespaces(internalNamespaces);
        datamodelDTO.setExternalNamespaces(externalNamespaces);

        var terminologies = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.requires)
                .stream().filter(val -> val.startsWith(modelConstants.getTerminologyNamespace()))
                .map(ref -> {
                    var terminologyModel = conceptRepository.fetch(ref);
                    if (terminologyModel == null) {
                        return new TerminologyDTO(ref);
                    }
                    return TerminologyMapper.mapToTerminologyDTO(ref, terminologyModel);
                }).collect(Collectors.toSet());
        datamodelDTO.setTerminologies(terminologies);

        var codeLists = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.requires)
                .stream().filter(val -> val.startsWith(modelConstants.getCodelistNamespace()))
                .map(codeList -> {
                    var codeListModel = schemesRepository.fetch(codeList);
                    if(codeListModel == null){
                        var codeListDTO = new CodeListDTO();
                        codeListDTO.setId(codeList);
                        return codeListDTO;
                    }
                    return CodeListMapper.mapToCodeListDTO(codeList, codeListModel);
                }).collect(Collectors.toSet());
        datamodelDTO.setCodeLists(codeLists);

        if (userMapper != null) {
            userMapper.accept(datamodelDTO);
        }

        datamodelDTO.setContact(MapperUtils.propertyToString(modelResource, Iow.contact));
        datamodelDTO.setDocumentation(MapperUtils.localizedPropertyToMap(modelResource, Iow.documentation));
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

        if(MapperUtils.isApplicationProfile(resource)){
            indexModel.setType(ModelType.PROFILE);
        }else if(MapperUtils.isLibrary(resource)){
            indexModel.setType(ModelType.LIBRARY);
        }else{
            throw new MappingError("RDF:type not supported for data model");
        }

        indexModel.setPrefix(prefix);
        indexModel.setLabel(MapperUtils.localizedPropertyToMap(resource, RDFS.label));
        indexModel.setComment(MapperUtils.localizedPropertyToMap(resource, RDFS.comment));
        var contributors = MapperUtils.arrayPropertyToList(resource, DCTerms.contributor)
                .stream().map(MapperUtils::getUUID).toList();
        indexModel.setContributor(contributors);
        var isPartOf = MapperUtils.arrayPropertyToList(resource, DCTerms.isPartOf);
        var serviceCategories = coreRepository.getServiceCategories();
        var groups = isPartOf.stream().map(serviceCat -> MapperUtils.propertyToString(serviceCategories.getResource(serviceCat), SKOS.notation)).toList();
        indexModel.setIsPartOf(groups);
        indexModel.setLanguage(MapperUtils.arrayPropertyToList(resource, DCTerms.language));

        return indexModel;
    }

    /**
     * Add organizations to a model
     * @param modelDTO Payload to get organizations from
     * @param modelResource Model resource to add orgs to
     */
    private void addOrgsToModel(DataModelDTO modelDTO, Resource modelResource) {
        var organizationsModel = coreRepository.getOrganizations();
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
        resource.listProperties(property)
                .filterDrop(prop -> {
                    var string = prop.getObject().toString();
                    return string.startsWith(modelConstants.getCodelistNamespace()) || string.startsWith(modelConstants.getTerminologyNamespace());
                })
                .forEach(prop -> {
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
            var ns = coreRepository.fetch(namespace);
            var nsRes = ns.getResource(namespace);
            var prefix = MapperUtils.propertyToString(nsRes, DCAP.preferredXMLNamespacePrefix);
            var nsType = MapperUtils.getModelTypeFromResource(nsRes);
            var modelType = MapperUtils.getModelTypeFromResource(resource);
            resource.addProperty(getNamespacePropertyFromType(modelType, nsType), nsRes);
            model.setNsPrefix(prefix, namespace);
        });
    }

    private Property getNamespacePropertyFromType(ModelType modelType, ModelType nsType){
        if(modelType.equals(ModelType.LIBRARY) && nsType.equals(ModelType.LIBRARY)){
            return OWL.imports;
        }else if(modelType.equals(ModelType.PROFILE) && nsType.equals(ModelType.LIBRARY)){
            return DCTerms.requires;
        }else if(modelType.equals(ModelType.PROFILE) && nsType.equals(ModelType.PROFILE)){
            return OWL.imports;
        }else{
            return DCTerms.requires;
        }
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
                var nsRes = model.createResource(nsUri);
                nsRes.addProperty(RDFS.label, namespace.getName());
                nsRes.addProperty(DCAP.preferredXMLNamespacePrefix, namespace.getPrefix());

                try{
                    var resolvedNs = importsRepository.fetch(nsUri);
                    var extRes = resolvedNs.getResource(nsUri);
                    var nsType = MapperUtils.getModelTypeFromResource(extRes);
                    var modelType = MapperUtils.getModelTypeFromResource(resource);
                    resource.addProperty(getNamespacePropertyFromType(modelType, nsType), nsRes);
                }catch(Exception e){
                    //If namespace wasn't resolved just add it as dcterms:requires
                    resource.addProperty(DCTerms.requires, nsRes);
                }
                model.setNsPrefix(namespace.getPrefix(), nsUri);
        });
    }
}
