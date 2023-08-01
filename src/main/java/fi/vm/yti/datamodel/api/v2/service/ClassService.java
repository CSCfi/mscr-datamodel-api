package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.ClassMapper;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.ResourceSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexResource;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexResourceInfo;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import fi.vm.yti.security.AuthenticatedUserProvider;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.SimpleSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static fi.vm.yti.security.AuthorizationException.check;

@Service
public class ClassService {

    private final Logger logger = LoggerFactory.getLogger(ClassService.class);

    private final ResourceService resourceService;
    private final CoreRepository coreRepository;
    private final ImportsRepository importsRepository;

    private final AuthorizationManager authorizationManager;
    private final AuthenticatedUserProvider userProvider;
    private final TerminologyService terminologyService;
    private final GroupManagementService groupManagementService;
    private final OpenSearchIndexer openSearchIndexer;
    private final SearchIndexService searchIndexService;

    @Autowired
    public ClassService(ResourceService resourceService,
                        CoreRepository coreRepository,
                        ImportsRepository importsRepository,
                        AuthorizationManager authorizationManager,
                        AuthenticatedUserProvider userProvider,
                        TerminologyService terminologyService,
                        GroupManagementService groupManagementService,
                        OpenSearchIndexer openSearchIndexer,
                        SearchIndexService searchIndexService) {
        this.resourceService = resourceService;
        this.coreRepository = coreRepository;
        this.importsRepository = importsRepository;
        this.authorizationManager = authorizationManager;
        this.userProvider = userProvider;
        this.terminologyService = terminologyService;
        this.groupManagementService = groupManagementService;
        this.openSearchIndexer = openSearchIndexer;
        this.searchIndexService = searchIndexService;
    }

    public ResourceInfoBaseDTO get(String prefix, String classIdentifier) {
        var modelURI = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        var classURI = modelURI + ModelConstants.RESOURCE_SEPARATOR + classIdentifier;
        if(!coreRepository.resourceExistsInGraph(modelURI , classURI)){
            throw new ResourceNotFoundException(classURI);
        }
        var model = coreRepository.fetch(modelURI);
        var hasRightToModel = authorizationManager.hasRightToModel(prefix, model);

        var orgModel = coreRepository.getOrganizations();
        var userMapper = hasRightToModel ? groupManagementService.mapUser() : null;

        ResourceInfoBaseDTO dto;
        if (MapperUtils.isLibrary(model.getResource(modelURI))) {
            var classResources = coreRepository.queryConstruct(ClassMapper.getClassResourcesQuery(classURI, false));
            dto = ClassMapper.mapToClassDTO(model, modelURI, classIdentifier, orgModel,
                    hasRightToModel, userMapper);
            ClassMapper.addClassResourcesToDTO(classResources, (ClassInfoDTO) dto);
        } else {
            dto = ClassMapper.mapToNodeShapeDTO(model, modelURI, classIdentifier, orgModel,
                    hasRightToModel, userMapper);
            var existingProperties = getTargetNodeProperties(MapperUtils.propertyToString(model.getResource(classURI), SH.node));
            var nodeShapeResources = coreRepository.queryConstruct(ClassMapper.getNodeShapeResourcesQuery(classURI));
            ClassMapper.addNodeShapeResourcesToDTO(model, nodeShapeResources, (NodeShapeInfoDTO) dto, existingProperties);
        }

        terminologyService.mapConcept().accept(dto);
        return dto;
    }

    public ExternalClassDTO getExternal(String uri) {
        var namespace = NodeFactory.createURI(uri).getNameSpace();
        var model = importsRepository.fetch(namespace);

        var dto = ClassMapper.mapExternalClassToDTO(model, uri);
        var resources = importsRepository.queryConstruct(ClassMapper.getClassResourcesQuery(uri, true));
        ClassMapper.addExternalClassResourcesToDTO(resources, dto);

        return dto;
    }

    public List<IndexResourceInfo> getNodeShapes(String targetClass) throws IOException {
        var request = new ResourceSearchRequest();
        request.setStatus(Set.of(Status.VALID, Status.DRAFT));
        request.setTargetClass(targetClass);
        return searchIndexService
                .searchInternalResourcesWithInfo(request, userProvider.getUser())
                .getResponseObjects();
    }

    public List<IndexResource> getNodeShapeProperties(String nodeURI) throws IOException {
        var propertyURIs = getTargetNodeProperties(nodeURI);
        return searchIndexService
                .findResourcesByURI(propertyURIs)
                .getResponseObjects();
    }

    public URI create(String prefix, BaseDTO dto, boolean applicationProfile) throws URISyntaxException {
        var modelUri = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        var classUri = modelUri + ModelConstants.RESOURCE_SEPARATOR + dto.getIdentifier();
        if(coreRepository.resourceExistsInGraph(modelUri, classUri)){
            throw new MappingError("Class already exists");
        }
        var model = coreRepository.fetch(modelUri);
        check(authorizationManager.hasRightToModel(prefix, model));
        checkDataModelType(model.getResource(modelUri), dto);
        terminologyService.resolveConcept(dto.getSubject());


        if(applicationProfile) {
            ClassMapper.createNodeShapeAndMapToModel(modelUri, model, (NodeShapeDTO) dto, userProvider.getUser());
            addNodeShapeProperties(model, classUri, (NodeShapeDTO) dto);
        }else {
            ClassMapper.createOntologyClassAndMapToModel(modelUri, model, (ClassDTO) dto, userProvider.getUser());
        }

        coreRepository.put(modelUri, model);
        openSearchIndexer.createResourceToIndex(ResourceMapper.mapToIndexResource(model, classUri));
        return new URI(classUri);
    }

    public void addNodeShapeProperties(Model model, String classUri, NodeShapeDTO dto) {
        var allProperties = new HashSet<String>();
        // Node shape based on an existing node shape (sh:node)
        allProperties.addAll(getTargetNodeProperties(dto.getTargetNode()));
        // User defined properties from target class reference
        allProperties.addAll(getNodeShapeTargetClassProperties(dto, model, classUri));

        ClassMapper.mapNodeShapeProperties(model, classUri, allProperties);
    }

    public void checkDataModelType(Resource modelResource, BaseDTO dto) {
        if (dto instanceof NodeShapeDTO && MapperUtils.isLibrary(modelResource)) {
            throw new MappingError("Cannot add node shape to ontology");
        } else if (dto instanceof ClassDTO && MapperUtils.isApplicationProfile(modelResource)) {
            throw new MappingError("Cannot add ontology class to application profile");
        }
    }

    public void update(String prefix, String classIdentifier, BaseDTO dto) {
        logger.info("Updating class {}", classIdentifier);

        var graph = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        var classURI = graph + ModelConstants.RESOURCE_SEPARATOR + classIdentifier;
        if(!coreRepository.resourceExistsInGraph(graph, classURI)){
            throw new ResourceNotFoundException(classIdentifier);
        }
        var model = coreRepository.fetch(graph);
        checkDataModelType(model.getResource(graph), dto);
        check(authorizationManager.hasRightToModel(prefix, model));
        terminologyService.resolveConcept(dto.getSubject());

        var classResource = model.getResource(classURI);
        if (MapperUtils.isLibrary(model.getResource(graph))) {
            ClassMapper.mapToUpdateOntologyClass(model, graph, classResource, (ClassDTO) dto, userProvider.getUser());
        } else {
            var nodeShape = (NodeShapeDTO) dto;

            var oldNode = MapperUtils.propertyToString(classResource, SH.node);
            var oldTarget = MapperUtils.propertyToString(classResource, SH.targetClass);

            var nodeShapeProperties = classResource.listProperties(SH.property)
                    .mapWith((var stmt) -> stmt.getResource().getURI())
                    .toSet();

            // TODO: how to handle existing properties from sh:node reference
            if (oldNode == null && nodeShape.getTargetNode() != null) {
                // add new sh:node, add properties from sh:node reference
                nodeShapeProperties.addAll(getTargetNodeProperties(nodeShape.getTargetNode()));
            } else if (oldNode != null && nodeShape.getTargetNode() != null && !oldNode.equals(nodeShape.getTargetNode())) {
                // replace sh:node, remove properties inherited from old sh:node (?) and add properties from new sh:node reference
                nodeShapeProperties.addAll(getTargetNodeProperties(nodeShape.getTargetNode()));
            } else if (oldNode != null && nodeShape.getTargetNode() == null) {
                // remove sh:node, remove properties old sh:node reference (?)
            }

            // add properties from new target class and create placeholders
            if (nodeShape.getTargetClass() != null && !nodeShape.getTargetClass().equals(oldTarget)) {
                nodeShapeProperties.addAll(getNodeShapeTargetClassProperties(nodeShape, model, classURI));
            }

            ClassMapper.mapToUpdateNodeShape(model, graph, classResource, (NodeShapeDTO) dto,
                    nodeShapeProperties, userProvider.getUser());
        }
        coreRepository.put(graph, model);

        var indexClass = ResourceMapper.mapToIndexResource(model, classURI);
        openSearchIndexer.updateResourceToIndex(indexClass);
    }

    /**
     * Delete a Class or Node shape
     * @param prefix Model prefix
     * @param identifier Resource identifier
     */
    public void delete(String prefix, String identifier) {
        var modelURI = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        var classURI = modelURI + ModelConstants.RESOURCE_SEPARATOR + identifier;
        if(!coreRepository.resourceExistsInGraph(modelURI , classURI)){
            throw new ResourceNotFoundException(classURI);
        }
        var model = coreRepository.fetch(modelURI);
        check(authorizationManager.hasRightToModel(prefix, model));
        coreRepository.deleteResource(classURI);
        openSearchIndexer.deleteResourceFromIndex(classURI);
    }

    private Set<String> getTargetNodeProperties(String targetNode) {
        var propertyShapes = new HashSet<String>();
        var handledNodeShapes = new HashSet<String>();

        // collect recursively all property shape uris from target node
        while (targetNode != null) {
            if (handledNodeShapes.contains(targetNode)) {
                throw new MappingError("Circular dependency, cannot add sh:node reference");
            }
            handledNodeShapes.add(targetNode);

            var nodeModel = resourceService.findResources(Set.of(targetNode));
            var nodeResource = nodeModel.getResource(targetNode);

            propertyShapes.addAll(nodeResource.listProperties(SH.property)
                    .mapWith((var stmt) -> stmt.getResource().getURI())
                    .toList());

            if (!nodeResource.hasProperty(SH.node)) {
                break;
            }
            targetNode = nodeResource.getProperty(SH.node).getObject().toString();
        }
        return propertyShapes;
    }

    private Set<String> getNodeShapeTargetClassProperties(NodeShapeDTO nodeShapeDTO, Model model, String classURI) {
        // skip creating new resource if there is already resource with sh:path reference to the property
        var existingProperties = nodeShapeDTO.getProperties().stream()
                .map(p -> {
                    var iter = model.listStatements(new SimpleSelector(null, SH.path, ResourceFactory.createResource(p)));
                    return iter.hasNext()
                            ? iter.next().getSubject().getURI()
                            : null;
                })
                .filter(Objects::nonNull)
                .toList();

        var newProperties = resourceService.findResources(nodeShapeDTO.getProperties().stream()
                .filter(p -> !existingProperties.contains(p))
                .collect(Collectors.toSet()));

        Predicate<String> checkFreeIdentifier =
                (var uri) -> coreRepository.resourceExistsInGraph(model.getResource(classURI).getNameSpace(), uri);

        // create new property shape resources to the model
        var createdProperties = ClassMapper.mapPlaceholderPropertyShapes(model, classURI, newProperties,
                userProvider.getUser(), checkFreeIdentifier);

        var allProperties = new HashSet<String>();
        allProperties.addAll(existingProperties);
        allProperties.addAll(createdProperties);

        // index new resources
        openSearchIndexer.bulkInsert(OpenSearchIndexer.OPEN_SEARCH_INDEX_RESOURCE,
                createdProperties.stream()
                        .map(p -> ResourceMapper.mapToIndexResource(model, p))
                        .toList());

        return allProperties;
    }

    public boolean exists(String prefix, String identifier) {
        // identifiers e.g. corner-abcd1234 are reserved for visualization
        if (identifier.startsWith("corner-")) {
            return true;
        }
        var graphUri = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        return coreRepository.resourceExistsInGraph(graphUri, graphUri + ModelConstants.RESOURCE_SEPARATOR + identifier);
    }

    //TODO should boolean delete be enum instead?
    public void handlePropertyShapeReference(String prefix, String nodeShapeIdentifier, String uri, boolean delete) {
        var modelURI = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        var model = coreRepository.fetch(modelURI);
        var classURI = modelURI + ModelConstants.RESOURCE_SEPARATOR + nodeShapeIdentifier;
        check(authorizationManager.hasRightToModel(prefix, model));

        var classResource = model.getResource(classURI);
        var existingProperties = getTargetNodeProperties(MapperUtils.propertyToString(classResource, SH.node));
        if(delete) {
            ClassMapper.mapRemoveNodeShapeProperty(model, classResource, uri, existingProperties);
        }else {
            ClassMapper.mapAppendNodeShapeProperty(classResource, uri, existingProperties);
        }
        coreRepository.put(modelURI, model);
    }

    public void togglePropertyShape(String prefix, String propertyUri) {
        var modelURI = ModelConstants.SUOMI_FI_NAMESPACE + prefix;

        if(!coreRepository.resourceExistsInGraph(modelURI, propertyUri)){
            throw new ResourceNotFoundException(propertyUri);
        }
        var model = coreRepository.fetch(modelURI);
        check(authorizationManager.hasRightToModel(prefix, model));
        ClassMapper.toggleAndMapDeactivatedProperty(model, propertyUri);
        coreRepository.put(modelURI, model);
    }
}
