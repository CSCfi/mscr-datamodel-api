package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.index.OpenSearchConnector;
import fi.vm.yti.datamodel.api.v2.dto.ModelType;
import fi.vm.yti.datamodel.api.v2.dto.ResourceType;
import fi.vm.yti.datamodel.api.v2.endpoint.error.OpenSearchException;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.CountSearchResponse;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.CrosswalkSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.ModelSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.ResourceSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.SearchResponseDTO;
import fi.vm.yti.datamodel.api.v2.opensearch.index.*;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.*;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexModel;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexResource;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexResourceInfo;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.opensearch.queries.CountQueryFactory;
import fi.vm.yti.datamodel.api.v2.opensearch.queries.CrosswalkQueryFactory;
import fi.vm.yti.datamodel.api.v2.opensearch.queries.ModelQueryFactory;
import fi.vm.yti.datamodel.api.v2.opensearch.queries.QueryFactoryUtils;
import fi.vm.yti.datamodel.api.v2.opensearch.queries.ResourceQueryFactory;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.security.YtiUser;
import org.apache.jena.arq.querybuilder.ExprFactory;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.arq.querybuilder.WhereBuilder;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.jetbrains.annotations.NotNull;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.search.Hit;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class SearchIndexService {

    private final OpenSearchClient client;
    private final GroupManagementService groupManagementService;
    private final TerminologyService terminologyService;
    private final CoreRepository coreRepository;

    public SearchIndexService(OpenSearchConnector openSearchConnector,
                              GroupManagementService groupManagementService,
                              TerminologyService terminologyService,
                              CoreRepository coreRepository) {
        this.client = openSearchConnector.getClient();
        this.groupManagementService = groupManagementService;
        this.terminologyService = terminologyService;
        this.coreRepository = coreRepository;
    }

    /**
     * List counts of data model grouped by different search results
     * @return response containing counts for data models
     */
    public CountSearchResponse getCounts(CountRequest searchRequest) {
        var query = CountQueryFactory.createModelQuery(searchRequest);
        try {
            var response = client.search(query, IndexModel.class);
            return CountQueryFactory.parseResponse(response);
        } catch (IOException e) {
            throw new OpenSearchException(e.getMessage(), OpenSearchIndexer.OPEN_SEARCH_INDEX_MODEL);
        }
    }

    public SearchResponseDTO<IndexModel> searchModels(ModelSearchRequest request, YtiUser user) {
        try {
            if (!user.isSuperuser()) {
                request.setIncludeIncompleteFrom(getOrganizationsForUser(user));
            }

            var build = ModelQueryFactory.createModelQuery(request);
            var response = client.search(build, IndexModel.class);

            var modelSearchResponse = new SearchResponseDTO<IndexModel>();
            modelSearchResponse.setResponseObjects(response.hits().hits().stream()
                    .map(Hit::source)
                    .toList()
            );
            modelSearchResponse.setTotalHitCount(response.hits().total().value());
            modelSearchResponse.setPageFrom(request.getPageFrom());
            modelSearchResponse.setPageSize(request.getPageSize());

            return modelSearchResponse;
        } catch (IOException e) {
            throw new OpenSearchException(e.getMessage(), OpenSearchIndexer.OPEN_SEARCH_INDEX_MODEL);
        }
    }

    public SearchResponseDTO<IndexResource> searchInternalResources(ResourceSearchRequest request, YtiUser user) throws IOException {
        Set<String> allowedDatamodels = new HashSet<>();
        if(!user.isSuperuser()) {
                var organizations = getOrganizationsForUser(user);
                var modelRequest = new ModelSearchRequest();
                modelRequest.setPageSize(QueryFactoryUtils.INTERNAL_SEARCH_PAGE_SIZE);
                modelRequest.setOrganizations(organizations);
                modelRequest.setIncludeIncompleteFrom(organizations);
                var build = ModelQueryFactory.createModelQuery(modelRequest);
                var response = client.search(build, IndexModel.class);
                allowedDatamodels = response.hits().hits().stream()
                        .filter(hit -> hit.source() != null)
                        .map(hit -> hit.source().getId()).collect(Collectors.toSet());
        }

        // Add resources from other models to the search request. Used in attribute / association lists
        if (request.getLimitToDataModel() != null
                && !request.isFromAddedNamespaces()
                && request.getResourceTypes() != null
                && !request.getResourceTypes().contains(ResourceType.CLASS)) {
            ExprFactory exprFactory = new ExprFactory();
            var graph = request.getLimitToDataModel();

            Resource resourceType = null;
            if (request.getResourceTypes().contains(ResourceType.ATTRIBUTE)) {
                resourceType = OWL.DatatypeProperty;
            } else if (request.getResourceTypes().contains(ResourceType.ASSOCIATION)) {
                resourceType = OWL.ObjectProperty;
            }
            var propertyVar = "?property";
            var select = new SelectBuilder()
                    .addVar(propertyVar)
                    .addWhere(new WhereBuilder()
                            .addGraph(NodeFactory.createURI(graph), new WhereBuilder()
                                    .addWhere("?subj", SH.property, propertyVar))
                            .addFilter(exprFactory
                                    .not(exprFactory.strstarts(exprFactory.str(propertyVar), graph))));

            if (resourceType != null) {
                select.addWhere(new WhereBuilder()
                        .addWhere(propertyVar, "a", resourceType));
            }
            var externalResources = new HashSet<String>();
            coreRepository.querySelect(select.build(), (var row) -> externalResources.add(row.get("property").toString()));
            request.setAdditionalResources(externalResources);
        }
        return searchInternalResources(request, allowedDatamodels);
    }

    public SearchResponseDTO<IndexResourceInfo> searchInternalResourcesWithInfo(ResourceSearchRequest request, YtiUser user) throws IOException {
        var dto = searchInternalResources(request, user);
        var dataModels = new HashMap<String, IndexModel>();

        var modelRequest = new ModelSearchRequest();
        modelRequest.setPageSize(1000);

        searchModels(modelRequest, user).getResponseObjects()
                .forEach(o -> dataModels.put(o.getId(), o));
        var concepts = terminologyService.getAllConcepts();

        var response = new SearchResponseDTO<IndexResourceInfo>();
        response.setTotalHitCount(dto.getTotalHitCount());
        response.setPageSize(dto.getPageSize());
        response.setPageFrom(dto.getPageFrom());
        response.setResponseObjects(dto.getResponseObjects().stream()
                .map(obj -> ResourceMapper.mapIndexResourceInfo(obj, dataModels, concepts))
                .toList());
        return response;
    }

    /**
     * Get model specific restrictions for a resource search
     * @param modelType Model type
     * @param groups Service category groups
     * @return List of DataModel URIs
     */
    private List<String> getModelSpecificRestrictions(ModelType modelType, Set<String> groups){
        if(modelType == null && (groups == null || groups.isEmpty())){
            //Skip the search all together if no extra filtering needs to be done
            return Collections.emptyList();
        }
        var modelRequest = new ModelSearchRequest();
        modelRequest.setPageSize(QueryFactoryUtils.INTERNAL_SEARCH_PAGE_SIZE);
        if(modelType != null){
            modelRequest.setType(Set.of(modelType));
        }
        if(groups != null && !groups.isEmpty()){
            modelRequest.setGroups(groups);
        }
        var build = ModelQueryFactory.createModelQuery(modelRequest);
        try {
            var response = client.search(build, IndexModel.class);
            return response.hits().hits().stream()
                    .filter(hit -> hit.source() != null)
                    .map(hit -> hit.source().getId()).toList();
        } catch (IOException e) {
            throw new OpenSearchException(e.getMessage(), OpenSearchIndexer.OPEN_SEARCH_INDEX_MODEL);
        }
    }

   public SearchResponseDTO<IndexResource> searchInternalResources(ResourceSearchRequest request, Set<String> allowedDatamodels) throws IOException {
        var namespaces = new ArrayList<String>();
        if(request.getLimitToDataModel() != null && !request.getLimitToDataModel().isBlank()){
            namespaces.add(request.getLimitToDataModel());
        }

        if(request.isFromAddedNamespaces()){
            if(request.getLimitToDataModel() == null){
                throw new OpenSearchException("limitToDataModel cannot be empty if getting from added namespace", OpenSearchIndexer.OPEN_SEARCH_INDEX_RESOURCE);
            }
            getNamespacesFromModel(request.getLimitToDataModel(), namespaces);
        }

        var restrictedDataModels = getModelSpecificRestrictions(request.getLimitToModelType(), request.getGroups());
        var build = ResourceQueryFactory.createInternalResourceQuery(request, namespaces, restrictedDataModels, allowedDatamodels);
        var response = client.search(build, IndexResource.class);

        var result = new SearchResponseDTO<IndexResource>();
        result.setResponseObjects(response.hits().hits().stream()
                .map(Hit::source)
                .toList()
        );
        result.setTotalHitCount(response.hits().total().value());
        result.setPageFrom(request.getPageFrom());
        result.setPageSize(request.getPageSize());

        return result;
    }

    public SearchResponseDTO<IndexResource> findResourcesByURI(Set<String> resourceURIs) throws IOException {
        var response = client.search(ResourceQueryFactory.createFindResourcesByURIQuery(resourceURIs), IndexResource.class);
        var result = new SearchResponseDTO<IndexResource>();
        result.setResponseObjects(response.hits().hits().stream()
                .map(Hit::source)
                .toList()
        );
        result.setTotalHitCount(response.hits().total().value());
        return result;
    }

    private void getNamespacesFromModel(String modelUri, List<String> namespaces){
        var model = coreRepository.fetch(modelUri);
        var resource = model.getResource(modelUri);
        namespaces.addAll(MapperUtils.arrayPropertyToList(resource, OWL.imports));
        namespaces.addAll(MapperUtils.arrayPropertyToList(resource, DCTerms.requires));
    }

    private Set<UUID> getOrganizationsForUser(YtiUser user){
        final var rolesInOrganizations = user.getRolesInOrganizations();

        var orgIds = new HashSet<>(rolesInOrganizations.keySet());

        // show child organization's incomplete content for main organization users
        var childOrganizationIds = orgIds.stream()
                .map(groupManagementService::getChildOrganizations)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        orgIds.addAll(childOrganizationIds);
        return orgIds;
    }

	public SearchResponseDTO<IndexCrosswalk> searchCrosswalks(CrosswalkSearchRequest request, @NotNull YtiUser user) {
        try {
            if (!user.isSuperuser()) {
                request.setIncludeIncompleteFrom(getOrganizationsForUser(user));
            }

            var build = CrosswalkQueryFactory.createCrosswalkQuery(request);            
            var response = client.search(build, IndexCrosswalk.class);

            var searchResponse = new SearchResponseDTO<IndexCrosswalk>();
            searchResponse.setResponseObjects(response.hits().hits().stream()
                    .map(Hit::source)
                    .toList()
            );
            searchResponse.setTotalHitCount(response.hits().total().value());
            searchResponse.setPageFrom(request.getPageFrom());
            searchResponse.setPageSize(request.getPageSize());

            return searchResponse;
        } catch (IOException e) {
            throw new OpenSearchException(e.getMessage(), OpenSearchIndexer.OPEN_SEARCH_INDEX_CROSSWALK);
        }
	}

	public SearchResponseDTO<IndexSchema> searchSchemas(ModelSearchRequest request, @NotNull YtiUser user) {
	     try {
	            if (!user.isSuperuser()) {
	                request.setIncludeIncompleteFrom(getOrganizationsForUser(user));
	            }

	            var build = ModelQueryFactory.createSchemaQuery(request);	            
	            var response = client.search(build, IndexSchema.class);

	            var modelSearchResponse = new SearchResponseDTO<IndexSchema>();
	            modelSearchResponse.setResponseObjects(response.hits().hits().stream()
	                    .map(Hit::source)
	                    .toList()
	            );
	            modelSearchResponse.setTotalHitCount(response.hits().total().value());
	            modelSearchResponse.setPageFrom(request.getPageFrom());
	            modelSearchResponse.setPageSize(request.getPageSize());

	            return modelSearchResponse;
	        } catch (IOException e) {
	            throw new OpenSearchException(e.getMessage(), OpenSearchIndexer.OPEN_SEARCH_INDEX_MODEL);
	        }
	}
}
