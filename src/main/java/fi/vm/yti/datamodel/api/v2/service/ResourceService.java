package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import fi.vm.yti.security.AuthenticatedUserProvider;
import org.apache.jena.arq.querybuilder.AskBuilder;
import org.apache.jena.arq.querybuilder.ConstructBuilder;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.RDF;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;

import static fi.vm.yti.security.AuthorizationException.check;

@Service
public class ResourceService {

    private final CoreRepository coreRepository;
    private final ImportsRepository importsRepository;
    private final AuthorizationManager authorizationManager;
    private final GroupManagementService groupManagementService;
    private final TerminologyService terminologyService;
    private final AuthenticatedUserProvider userProvider;
    private final OpenSearchIndexer openSearchIndexer;

    @Autowired
    public ResourceService(CoreRepository coreRepository,
                           ImportsRepository importsRepository,
                           AuthorizationManager authorizationManager,
                           GroupManagementService groupManagementService,
                           TerminologyService terminologyService,
                           AuthenticatedUserProvider userProvider,
                           OpenSearchIndexer openSearchIndexer){
        this.coreRepository = coreRepository;
        this.importsRepository = importsRepository;
        this.authorizationManager = authorizationManager;
        this.groupManagementService = groupManagementService;
        this.terminologyService = terminologyService;
        this.userProvider = userProvider;
        this.openSearchIndexer = openSearchIndexer;
    }

    public ResourceInfoBaseDTO get(String prefix, String identifier) {
        var graphUri = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        if(!coreRepository.resourceExistsInGraph(graphUri,graphUri + ModelConstants.RESOURCE_SEPARATOR + identifier)){
            throw new ResourceNotFoundException(identifier);
        }

        var model = coreRepository.fetch(graphUri);
        var orgModel = coreRepository.getOrganizations();
        var hasRightToModel = authorizationManager.hasRightToModel(prefix, model);
        var modelResource = model.getResource(graphUri);

        ResourceInfoBaseDTO dto;
        if (MapperUtils.isLibrary(modelResource)) {
            dto = ResourceMapper.mapToResourceInfoDTO(model, graphUri, identifier, orgModel, hasRightToModel, groupManagementService.mapUser());
        } else if (MapperUtils.isApplicationProfile(modelResource)) {
            dto = ResourceMapper.mapToPropertyShapeInfoDTO(model, graphUri, identifier, orgModel, hasRightToModel, groupManagementService.mapUser());
        } else {
            throw new MappingError("Invalid model");
        }

        terminologyService.mapConcept().accept(dto);
        return dto;
    }

    public ExternalResourceDTO getExternal(String uri){
        var namespace = NodeFactory.createURI(uri).getNameSpace();
        var model = importsRepository.fetch(namespace);
        var resource = model.getResource(uri);
        if (!model.contains(resource, null, (RDFNode) null)) {
            throw new ResourceNotFoundException(uri);
        }
        return ResourceMapper.mapToExternalResource(resource);
    }

    public URI create(String prefix, BaseDTO dto, ResourceType resourceType, boolean applicationProfile) throws URISyntaxException {
        var graphUri = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        if (coreRepository.resourceExistsInGraph(graphUri, graphUri + prefix + ModelConstants.RESOURCE_SEPARATOR + dto.getIdentifier())){
            throw new MappingError("Already exists");
        }
        var model = coreRepository.fetch(graphUri);
        check(authorizationManager.hasRightToModel(prefix, model));
        checkDataModelType(model.getResource(graphUri), dto);

        terminologyService.resolveConcept(dto.getSubject());

        String resourceUri;
        if(applicationProfile){
            resourceUri = ResourceMapper.mapToPropertyShapeResource(graphUri, model, (PropertyShapeDTO) dto, userProvider.getUser());
        }else {
            resourceUri = ResourceMapper.mapToResource(graphUri, model, (ResourceDTO) dto, resourceType, userProvider.getUser());
        }

        coreRepository.put(graphUri, model);
        var indexClass = ResourceMapper.mapToIndexResource(model, resourceUri);
        openSearchIndexer.createResourceToIndex(indexClass);
        return new URI(resourceUri);
    }

    public void update(String prefix, String identifier, BaseDTO dto) {
        var graphUri = ModelConstants.SUOMI_FI_NAMESPACE + prefix;

        if(!coreRepository.resourceExistsInGraph(graphUri, graphUri + ModelConstants.RESOURCE_SEPARATOR + identifier)){
            throw new ResourceNotFoundException(identifier);
        }

        var model = coreRepository.fetch(graphUri);
        check(authorizationManager.hasRightToModel(prefix, model));
        checkDataModelType(model.getResource(graphUri), dto);

        if (dto instanceof ResourceDTO resourceDTO) {
            ResourceMapper.mapToUpdateResource(graphUri, model, identifier, resourceDTO, userProvider.getUser());
        } else if (dto instanceof PropertyShapeDTO propertyShapeDTO) {
            ResourceMapper.mapToUpdatePropertyShape(graphUri, model, identifier, propertyShapeDTO, userProvider.getUser());
        } else {
            throw new MappingError("Invalid content type for mapping resource");
        }
        terminologyService.resolveConcept(dto.getSubject());

        coreRepository.put(graphUri, model);
        var indexResource = ResourceMapper.mapToIndexResource(model, graphUri + ModelConstants.RESOURCE_SEPARATOR + identifier);
        openSearchIndexer.updateResourceToIndex(indexResource);
    }

    public void delete(String prefix, String resourceIdentifier) {
        var modelURI = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        var resourceUri  = modelURI + ModelConstants.RESOURCE_SEPARATOR + resourceIdentifier;
        if(!coreRepository.resourceExistsInGraph(modelURI , resourceUri)){
            throw new ResourceNotFoundException(resourceUri);
        }
        var model = coreRepository.fetch(modelURI);
        check(authorizationManager.hasRightToModel(prefix, model));

        coreRepository.deleteResource(resourceUri);
        openSearchIndexer.deleteResourceFromIndex(resourceUri);
    }

    public URI copyPropertyShape(String prefix, String propertyShapeIdentifier, String targetPrefix, String newIdentifier) throws URISyntaxException {
        var graphUri = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        var targetGraph = ModelConstants.SUOMI_FI_NAMESPACE + targetPrefix;
        var targetResource = targetGraph + ModelConstants.RESOURCE_SEPARATOR + newIdentifier;

        if(!coreRepository.resourceExistsInGraph(graphUri, graphUri + ModelConstants.RESOURCE_SEPARATOR + propertyShapeIdentifier)){
            throw new ResourceNotFoundException(propertyShapeIdentifier);
        }
        if(coreRepository.resourceExistsInGraph(targetGraph, targetResource)){
            throw new MappingError("Identifier in use");
        }

        var model = coreRepository.fetch(graphUri);
        var targetModel = coreRepository.fetch(targetGraph);

        if(!MapperUtils.isApplicationProfile(model.getResource(graphUri)) || !MapperUtils.isApplicationProfile(targetModel.getResource(targetGraph))){
            throw new MappingError("Both data models have to be application profiles");
        }
        check(authorizationManager.hasRightToModel(prefix, model));
        check(authorizationManager.hasRightToModel(targetPrefix, targetModel));

        ResourceMapper.mapToCopyToLocalPropertyShape(graphUri, model, propertyShapeIdentifier, targetModel, targetGraph, newIdentifier, userProvider.getUser());

        coreRepository.put(targetGraph, targetModel);
        var indexResource = ResourceMapper.mapToIndexResource(targetModel, targetGraph + ModelConstants.RESOURCE_SEPARATOR + newIdentifier);
        openSearchIndexer.createResourceToIndex(indexResource);
        return new URI(targetResource);
    }

    public boolean exists(String prefix, String identifier) {
        // identifiers e.g. corner-abcd1234 are reserved for visualization
        if (identifier.startsWith("corner-")) {
            return true;
        }
        var graphUri = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        return coreRepository.resourceExistsInGraph(graphUri, graphUri + ModelConstants.RESOURCE_SEPARATOR + identifier);
    }

    private void checkDataModelType(Resource modelResource, BaseDTO dto) {
        if (dto instanceof PropertyShapeDTO && MapperUtils.isLibrary(modelResource)) {
            throw new MappingError("Cannot add property shape to ontology");
        } else if (dto instanceof ResourceDTO && MapperUtils.isApplicationProfile(modelResource)) {
            throw new MappingError("Cannot add resource to application profile");
        }
    }

    /**
     * Check if resource uri is one of given types
     * @param resourceUri Resource uri
     * @param types List of types to check
     * @param checkImports Should imports be checked instead of core
     * @return true if resource is one of types
     */
    public boolean checkIfResourceIsOneOfTypes(String resourceUri, List<Resource> types, boolean checkImports) {
        var askBuilder  =new AskBuilder()
                .addWhere(NodeFactory.createURI(resourceUri), RDF.type, "?type")
                .addValueVar("?type", types.toArray());
        try{
            if(checkImports){
                return importsRepository.queryAsk(askBuilder.build());
            }else {
                return coreRepository.queryAsk(askBuilder.build());
            }
        }catch(HttpException ex){
            throw new JenaQueryException();
        }
    }

    public Model findResources(Set<String> resourceURIs) {
        if (resourceURIs == null || resourceURIs.isEmpty()) {
            return ModelFactory.createDefaultModel();
        }
        var coreBuilder = new ConstructBuilder()
                .addPrefixes(ModelConstants.PREFIXES);
        var importsBuilder = new ConstructBuilder()
                .addPrefixes(ModelConstants.PREFIXES);

        var iterator = resourceURIs.iterator();
        var count = 0;
        while (iterator.hasNext()) {
            var pred = "?p" + count;
            var obj = "?o" + count;
            String uri = iterator.next();
            var resource = ResourceFactory.createResource(uri);
            if (uri.startsWith(ModelConstants.SUOMI_FI_NAMESPACE)) {
                coreBuilder.addConstruct(resource, pred, obj);
                coreBuilder.addOptional(resource, pred, obj);
            } else {
                importsBuilder.addConstruct(resource, pred, obj);
                importsBuilder.addOptional(resource, pred, obj);
            }
            count++;
        }

        var resultModel = coreRepository.queryConstruct(coreBuilder.build());
        var importsModel = importsRepository.queryConstruct(importsBuilder.build());

        resultModel.add(importsModel);
        return resultModel;
    }
}
