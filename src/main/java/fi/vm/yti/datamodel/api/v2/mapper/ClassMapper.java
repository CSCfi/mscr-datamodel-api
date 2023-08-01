package fi.vm.yti.datamodel.api.v2.mapper;

import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.utils.DataModelUtils;
import fi.vm.yti.datamodel.api.v2.utils.SparqlUtils;
import fi.vm.yti.security.YtiUser;
import org.apache.jena.arq.querybuilder.ConstructBuilder;
import org.apache.jena.arq.querybuilder.WhereBuilder;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.topbraid.shacl.vocabulary.SH;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class ClassMapper {

    private ClassMapper(){
        //Static class
    }

    private static final Logger logger = LoggerFactory.getLogger(ClassMapper.class);

    private static Resource createResourceAndMapCommonInfo(Model model, String modelURI, BaseDTO dto) {
        var modelResource = model.getResource(modelURI);
        var classUri = modelURI + ModelConstants.RESOURCE_SEPARATOR + dto.getIdentifier();
        var resource = model.createResource(classUri)
                .addProperty(OWL.versionInfo, dto.getStatus().name())
                .addProperty(RDFS.isDefinedBy, modelResource)
                .addProperty(DCTerms.identifier, ResourceFactory.createTypedLiteral(dto.getIdentifier(), XSDDatatype.XSDNCName));

        var langs = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.language);
        //Labels
        MapperUtils.addLocalizedProperty(langs, dto.getLabel(), resource, RDFS.label, model);
        //Concept from terminology
        MapperUtils.addOptionalUriProperty(resource, DCTerms.subject, dto.getSubject());
        MapperUtils.addLocalizedProperty(langs, dto.getNote(), resource, RDFS.comment, model);
        MapperUtils.addOptionalStringProperty(resource, SKOS.editorialNote, dto.getEditorialNote());

        return resource;
    }

    private static void updateResourceAndMapCommon(Model model, String graph, Resource classResource, BaseDTO dto) {
        var modelResource = model.getResource(graph);
        var languages = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.language);

        MapperUtils.updateLocalizedProperty(languages, dto.getLabel(), classResource, RDFS.label, model);
        MapperUtils.updateLocalizedProperty(languages, dto.getNote(), classResource, RDFS.comment, model);
        MapperUtils.updateUriProperty(classResource, DCTerms.subject, dto.getSubject());
        MapperUtils.updateStringProperty(classResource, SKOS.editorialNote, dto.getEditorialNote());
        var status = dto.getStatus();
        if (status != null) {
            MapperUtils.updateStringProperty(classResource, OWL.versionInfo, status.name());
        }
    }

    public static String createOntologyClassAndMapToModel(String modelURI, Model model, ClassDTO dto, YtiUser user) {
        logger.info("Adding class to {}", modelURI);
        var modelResource = model.getResource(modelURI);

        var classResource = createResourceAndMapCommonInfo(model, modelURI, dto);
        MapperUtils.addCreationMetadata(classResource, user);

        classResource.addProperty(RDF.type, OWL.Class);

        var owlImports = MapperUtils.arrayPropertyToSet(modelResource, OWL.imports);
        var dcTermsRequires = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.requires);
        //Equivalent class
        if(dto.getEquivalentClass() != null){
            dto.getEquivalentClass().forEach(eq -> MapperUtils.addResourceRelationship(owlImports, dcTermsRequires, classResource, OWL.equivalentClass, eq));
        }
        //Sub Class
        if(dto.getSubClassOf() == null || dto.getSubClassOf().isEmpty()){
            classResource.addProperty(RDFS.subClassOf, OWL.Thing); //Add OWL:Thing if nothing else is specified
        }else{
            dto.getSubClassOf().forEach(sub -> MapperUtils.addResourceRelationship(owlImports, dcTermsRequires, classResource, RDFS.subClassOf, sub));
        }
        modelResource.addProperty(DCTerms.hasPart, classResource);
        return classResource.getURI();
    }

    public static String createNodeShapeAndMapToModel(String modelURI, Model model, NodeShapeDTO dto, YtiUser user) {
        logger.info("Adding node shape to {}", modelURI);

        var nodeShapeResource = createResourceAndMapCommonInfo(model, modelURI, dto);
        MapperUtils.addCreationMetadata(nodeShapeResource, user);

        nodeShapeResource.addProperty(RDF.type, SH.NodeShape);
        MapperUtils.addOptionalUriProperty(nodeShapeResource, SH.targetClass, dto.getTargetClass());
        MapperUtils.addOptionalUriProperty(nodeShapeResource, SH.node, dto.getTargetNode());

        return nodeShapeResource.getURI();
    }

    public static List<String> mapPlaceholderPropertyShapes(Model applicationProfileModel, String classURI,
                                                            Model propertiesModel, YtiUser user,
                                                            Predicate<String> checkFreeIdentifier) {
        var iterator = propertiesModel.listSubjects();
        var classResource = applicationProfileModel.getResource(classURI);
        var propertyResourceURIs = new ArrayList<String>();
        while (iterator.hasNext()) {
            var uri = iterator.next().getURI();
            var identifier = NodeFactory.createURI(uri).getLocalName();

            var currentIdentifier = identifier;
            var count = 0;
            while (checkFreeIdentifier.test(classResource.getNameSpace() + currentIdentifier)) {
                currentIdentifier = identifier + String.format("-%d", ++count);
            }
            var targetResource = propertiesModel.getResource(uri);
            var propertyShapeResource = applicationProfileModel.createResource(classResource.getNameSpace() + currentIdentifier);
            var label = targetResource.getProperty(RDFS.label);

            if (label != null) {
                // external class labels are defined often in only one language
                if (label.getLanguage().equals("")) {
                    MapperUtils.addLocalizedProperty(Set.of("en"),
                            Map.of("en", label.getObject().toString()),
                            propertyShapeResource,
                            RDFS.label,
                            applicationProfileModel);
                } else {
                    propertyShapeResource.addProperty(RDFS.label, label.getObject());
                }
            }

            propertyShapeResource.addProperty(SH.path, ResourceFactory.createResource(uri))
                    .addProperty(DCTerms.identifier, ResourceFactory.createTypedLiteral(currentIdentifier, XSDDatatype.XSDNCName))
                    .addProperty(RDF.type, SH.PropertyShape)
                    .addProperty(RDF.type, targetResource.getProperty(RDF.type).getObject())
                    .addProperty(RDFS.isDefinedBy, classResource.getProperty(RDFS.isDefinedBy).getObject())
                    .addProperty(OWL.versionInfo, Status.DRAFT.name());

            MapperUtils.addCreationMetadata(propertyShapeResource, user);

            classResource.addProperty(SH.property, propertyShapeResource);
            propertyResourceURIs.add(propertyShapeResource.getURI());
        }
        return propertyResourceURIs;
    }

    public static void mapNodeShapeProperties(Model model, String classURI, Set<String> propertyURIs) {
        var classRes = model.getResource(classURI);
        propertyURIs.forEach(
                (var uri) -> classRes.addProperty(SH.property, ResourceFactory.createResource(uri))
        );
    }

    public static void mapToUpdateOntologyClass(Model model, String graph, Resource classResource, ClassDTO classDTO, YtiUser user) {
        logger.info("Updating class in graph {}", graph);
        var modelResource = model.getResource(graph);

        updateResourceAndMapCommon(model, graph, classResource, classDTO);

        var owlImports = MapperUtils.arrayPropertyToSet(modelResource, OWL.imports);
        var dcTermsRequires = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.requires);

        var equivalentClasses = classDTO.getEquivalentClass();
        if(equivalentClasses != null){
            classResource.removeAll(OWL.equivalentClass);
            equivalentClasses.forEach(eq -> MapperUtils.addResourceRelationship(owlImports, dcTermsRequires, classResource, OWL.equivalentClass, eq));
        }

        var subClassOf = classDTO.getSubClassOf();
        if (subClassOf != null){
            classResource.removeAll(RDFS.subClassOf);
            if(subClassOf.isEmpty()){
                classResource.addProperty(RDFS.subClassOf, OWL.Thing); //Add OWL:Thing if no subClassOf is specified
            }else{
                subClassOf.forEach(sub -> MapperUtils.addResourceRelationship(owlImports, dcTermsRequires, classResource, RDFS.subClassOf, sub));
            }
        }
        MapperUtils.addUpdateMetadata(classResource, user);
    }

    public static void mapToUpdateNodeShape(Model model, String graph, Resource classResource, NodeShapeDTO nodeShapeDTO, Set<String> properties, YtiUser user) {
        logger.info("Updating node shape in graph {}", graph);

        updateResourceAndMapCommon(model, graph, classResource, nodeShapeDTO);
        if (nodeShapeDTO.getTargetClass() == null) {
            classResource.removeAll(SH.targetClass);
        } else {
            MapperUtils.updateUriProperty(classResource, SH.targetClass, nodeShapeDTO.getTargetClass());
        }

        if (nodeShapeDTO.getTargetNode() == null) {
            classResource.removeAll(SH.node);
        } else {
            MapperUtils.updateUriProperty(classResource, SH.node, nodeShapeDTO.getTargetNode());
        }

        classResource.removeAll(SH.property);
        mapNodeShapeProperties(model, classResource.getURI(), properties);

        MapperUtils.addUpdateMetadata(classResource, user);
    }

    private static void mapCommonInfoDTO(ResourceInfoBaseDTO dto,
                                            Resource resource,
                                            Resource modelResource,
                                            Model orgModel,
                                            boolean hasRightToModel) {

        var uriDTO = MapperUtils.uriToURIDTO(resource.getURI(), resource.getModel());
        dto.setUri(uriDTO.getUri());
        dto.setCurie(uriDTO.getCurie());

        dto.setLabel(MapperUtils.localizedPropertyToMap(resource, RDFS.label));
        dto.setStatus(Status.valueOf(MapperUtils.propertyToString(resource, OWL.versionInfo)));

        var subject = MapperUtils.propertyToString(resource, DCTerms.subject);
        if (subject != null) {
            var conceptDTO = new ConceptDTO();
            conceptDTO.setConceptURI(subject);
            dto.setSubject(conceptDTO);
        }
        dto.setIdentifier(resource.getLocalName());
        if (hasRightToModel) {
            dto.setEditorialNote(MapperUtils.propertyToString(resource, SKOS.editorialNote));
        }

        var contributors = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.contributor);
        dto.setContributor(OrganizationMapper.mapOrganizationsToDTO(contributors, orgModel));
        dto.setContact(MapperUtils.propertyToString(modelResource, Iow.contact));
        dto.setNote(MapperUtils.localizedPropertyToMap(resource, RDFS.comment));
    }

    /**
     * Map model with given prefix and class identifier
     *
     * @param model Model
     * @param modelUri Model uri
     * @param classIdentifier class identifier
     * @param orgModel Model of organisations
     * @param hasRightToModel does current user have right to model
     * @return Class DTO
     */
    public static ClassInfoDTO mapToClassDTO(Model model, String modelUri,
                                             String classIdentifier,
                                             Model orgModel,
                                             boolean hasRightToModel,
                                             Consumer<ResourceCommonDTO> userMapper){
        var dto = new ClassInfoDTO();
        var classUri = modelUri + ModelConstants.RESOURCE_SEPARATOR + classIdentifier;
        var classResource = model.getResource(classUri);
        var modelResource = model.getResource(modelUri);

        DataModelUtils.addPrefixesToModel(modelResource.getURI(), model);

        mapCommonInfoDTO(dto, classResource, modelResource, orgModel, hasRightToModel);
        MapperUtils.mapCreationInfo(dto, classResource, userMapper);

        var subClasses = MapperUtils.arrayPropertyToSet(classResource, RDFS.subClassOf);
        var equivalentClasses = MapperUtils.arrayPropertyToSet(classResource, OWL.equivalentClass);

        dto.setSubClassOf(MapperUtils.uriToURIDTOs(subClasses, model));
        dto.setEquivalentClass(MapperUtils.uriToURIDTOs(equivalentClasses, model));

        return dto;
    }

    public static NodeShapeInfoDTO mapToNodeShapeDTO(Model model, String modelUri,
                                                     String identifier,
                                                     Model orgModel,
                                                     boolean hasRightToModel,
                                                     Consumer<ResourceCommonDTO> userMapper) {
        var dto = new NodeShapeInfoDTO();
        var nodeShapeURI = modelUri + ModelConstants.RESOURCE_SEPARATOR + identifier;
        var nodeShapeResource = model.getResource(nodeShapeURI);
        var modelResource = model.getResource(modelUri);

        DataModelUtils.addPrefixesToModel(modelUri, model);

        mapCommonInfoDTO(dto, nodeShapeResource, modelResource, orgModel, hasRightToModel);
        MapperUtils.mapCreationInfo(dto, nodeShapeResource, userMapper);

        dto.setTargetClass(MapperUtils.uriToURIDTO(
                MapperUtils.propertyToString(nodeShapeResource, SH.targetClass), model));
        dto.setTargetNode(MapperUtils.uriToURIDTO(
                MapperUtils.propertyToString(nodeShapeResource, SH.node), model));

        return dto;
    }

    public static ExternalClassDTO mapExternalClassToDTO(Model model, String uri) {
        var resource = model.getResource(uri);

        var dto = new ExternalClassDTO();
        dto.setUri(uri);
        dto.setLabel(MapperUtils.localizedPropertyToMap(resource, RDFS.label));
        return dto;
    }

    public static Query getClassResourcesQuery(String classUri, boolean isExternal){
        var constructBuilder = new ConstructBuilder();
        var resourceName = "?resource";
        var uri = NodeFactory.createURI(classUri);
        SparqlUtils.addConstructProperty(resourceName, constructBuilder, RDF.type, "?type");
        SparqlUtils.addConstructProperty(resourceName, constructBuilder, RDFS.label, "?label");
        if (!isExternal) {
            SparqlUtils.addConstructProperty(resourceName, constructBuilder, DCTerms.identifier, "?identifier");
        }
        SparqlUtils.addConstructProperty(resourceName, constructBuilder, RDFS.isDefinedBy, "?isDefinedBy");
        var domainQuery = new WhereBuilder().addWhere(resourceName, RDFS.domain, uri)
                .addWhere(resourceName, RDFS.domain, "?domain");
        var rangeQuery = new WhereBuilder().addWhere(resourceName, RDFS.range, uri)
                .addWhere(resourceName, RDFS.range, "?range");
        constructBuilder.addWhere(domainQuery.addUnion(rangeQuery))
                .addConstruct(resourceName, RDFS.range, "?range")
                .addConstruct(resourceName, RDFS.domain, "?domain");
        return constructBuilder.build();
    }

    public static Query getNodeShapeResourcesQuery(String nodeShapeURI) {
        var constructBuilder = new ConstructBuilder();
        var resourceName = "?resource";
        var uri = NodeFactory.createURI(nodeShapeURI);
        SparqlUtils.addConstructProperty(resourceName, constructBuilder, RDF.type, "?type");
        SparqlUtils.addConstructProperty(resourceName, constructBuilder, RDFS.label, "?label");
        SparqlUtils.addConstructOptional(resourceName, constructBuilder, DCTerms.identifier, "?identifier");
        SparqlUtils.addConstructProperty(resourceName, constructBuilder, RDFS.isDefinedBy, "?isDefinedBy");
        constructBuilder.addWhere(uri, SH.property, resourceName);

        return constructBuilder.build();
    }

    public static void addClassResourcesToDTO(Model classResources, ClassInfoDTO dto){
        var associations = new ArrayList<SimpleResourceDTO>();
        var attributes = new ArrayList<SimpleResourceDTO>();
        classResources.listSubjects().forEach(res -> {
            var resDTO = new SimpleResourceDTO();
            resDTO.setUri(res.getURI());
            resDTO.setIdentifier(res.getProperty(DCTerms.identifier).getString());
            resDTO.setLabel(MapperUtils.localizedPropertyToMap(res, RDFS.label));
            var modelUri = MapperUtils.propertyToString(res, RDFS.isDefinedBy);
            if(modelUri == null){
                throw new MappingError("ModelUri null for resource");
            }
            resDTO.setModelId(MapperUtils.getModelIdFromNamespace(modelUri));
            var type = res.getProperty(RDF.type).getResource();
            if(type.equals(OWL.ObjectProperty)){
                associations.add(resDTO);
            }else if(type.equals(OWL.DatatypeProperty)){
                attributes.add(resDTO);
            }
            dto.setAssociation(associations);
            dto.setAttribute(attributes);
        });
    }

    public static void addNodeShapeResourcesToDTO(Model model, Model propertyShapeResources, NodeShapeInfoDTO nodeShapeDTO, Set<String> restrictedProperties) {
        var deactivatedURIs = model.listSubjectsWithProperty(SH.deactivated)
                .mapWith(Resource::getURI).toList();

        propertyShapeResources.listSubjects().forEach(resource -> {
            var dto = new SimplePropertyShapeDTO();

            var modelUri = MapperUtils.propertyToString(resource, RDFS.isDefinedBy);
            if (modelUri == null) {
                throw new MappingError("ModelUri null for resource");
            }

            if(restrictedProperties.contains(resource.getURI())){
                dto.setFromShNode(true);
            }

            dto.setUri(resource.getURI());
            dto.setModelId(MapperUtils.getModelIdFromNamespace(modelUri));
            dto.setLabel(MapperUtils.localizedPropertyToMap(resource, RDFS.label));
            dto.setIdentifier(resource.getLocalName());
            dto.setDeactivated(deactivatedURIs.contains(resource.getURI()));
            if (MapperUtils.hasType(resource, OWL.DatatypeProperty)) {
                nodeShapeDTO.getAttribute().add(dto);
            } else if (MapperUtils.hasType(resource, OWL.ObjectProperty)) {
                nodeShapeDTO.getAssociation().add(dto);
            }
        });
    }

    public static void addExternalClassResourcesToDTO(Model classResources, ExternalClassDTO dto) {
        var associations = new ArrayList<ExternalResourceDTO>();
        var attributes = new ArrayList<ExternalResourceDTO>();

        classResources.listSubjects().forEach(res -> {
            var resourceDTO = new ExternalResourceDTO();
            resourceDTO.setUri(res.getURI());
            resourceDTO.setLabel(MapperUtils.localizedPropertyToMap(res, RDFS.label));
            if (MapperUtils.hasType(res, OWL.ObjectProperty)) {
                associations.add(resourceDTO);
            } else if (MapperUtils.hasType(res, OWL.DatatypeProperty)) {
                attributes.add(resourceDTO);
            }
        });
        dto.setAttributes(attributes);
        dto.setAssociations(associations);
    }

    public static void toggleAndMapDeactivatedProperty(Model model, String propertyURI) {
        var resource = model.getResource(propertyURI);
        if (!MapperUtils.hasType(resource, SH.PropertyShape, SH.NodeShape)) {
            throw new MappingError("Resource must be NodeShape or PropertyShape");
        }
        if (resource.hasProperty(SH.deactivated)) {
            resource.removeAll(SH.deactivated);
        } else {
            resource.addLiteral(SH.deactivated, true);
        }
    }

    public static void mapAppendNodeShapeProperty(Resource classResource, String propertyURI,
                                                  Set<String> restrictedProperties) {
        if (restrictedProperties.contains(propertyURI) || !MapperUtils.hasType(classResource, SH.NodeShape)) {
            throw new MappingError("Resource is not sh:PropertyShape or property already exists");
        }
        classResource.addProperty(SH.property, ResourceFactory.createResource(propertyURI));
    }

    public static void mapRemoveNodeShapeProperty(Model model, Resource classResource, String propertyURI,
                                                  Set<String> restrictedProperties) {
        if (restrictedProperties.contains(propertyURI) || !MapperUtils.hasType(classResource, SH.NodeShape)) {
            throw new MappingError("Resource is not sh:PropertyShape or property is added from sh:node reference");
        }
        model.remove(ResourceFactory.createStatement(classResource, SH.property, ResourceFactory.createResource(propertyURI)));
    }
}
