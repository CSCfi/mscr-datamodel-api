package fi.vm.yti.datamodel.api.v2.opensearch.queries;

import fi.vm.yti.datamodel.api.v2.dto.ResourceType;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.ResourceSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import org.opensearch.client.opensearch._types.SortOptions;
import org.opensearch.client.opensearch._types.SortOptionsBuilders;
import org.opensearch.client.opensearch._types.SortOrder;
import org.opensearch.client.opensearch._types.mapping.FieldType;
import org.opensearch.client.opensearch._types.query_dsl.BoolQuery;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.QueryBuilders;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.springframework.beans.factory.annotation.Value;

import java.util.*;

import static fi.vm.yti.datamodel.api.v2.opensearch.OpenSearchUtil.logPayload;

public class ResourceQueryFactory {

	
	private static String defaultNamespace = "http://uri.suomi.fi/datamodel/ns/";
	
	
    private ResourceQueryFactory(){
    	
    }
    public static SearchRequest createInternalResourceQuery(ResourceSearchRequest request, List<String> fromNamespaces, List<String> restrictedDataModels, Set<String> allowedDatamodels) {
        List<Query> must = new ArrayList<>();
        List<Query> should = new ArrayList<>();

        if(allowedDatamodels != null && !allowedDatamodels.isEmpty()){
            should.add(QueryFactoryUtils.termsQuery("isDefinedBy", allowedDatamodels));
        }

        should.add(QueryFactoryUtils.hideIncompleteStatusQuery());

        var query = request.getQuery();
        if(query != null && !query.isBlank()){
            must.add(QueryFactoryUtils.labelQuery(query));
        }

        var statuses = request.getStatus();
        if(statuses != null && !statuses.isEmpty()){
            must.add(QueryFactoryUtils.termsQuery("status", statuses.stream().map(Status::name).toList()));
        }

        // intersect allowed data model lists and include possible additional resources (references from other models)
        var isDefinedByCondition = getIsDefinedByCondition(fromNamespaces, restrictedDataModels, request.getLimitToDataModel());
        if (!isDefinedByCondition.isEmpty()) {
            must.add(new BoolQuery.Builder()
                    .should(QueryFactoryUtils.termsQuery("isDefinedBy", isDefinedByCondition))
                    .should(QueryFactoryUtils.termsQuery("id", request.getAdditionalResources() != null
                            ? request.getAdditionalResources()
                            : Collections.emptySet()))
                    .build().
                    _toQuery()
            );
        }

        var types = request.getResourceTypes();
        if(types != null && !types.isEmpty()){
            must.add(QueryFactoryUtils.termsQuery("resourceType", types.stream().map(ResourceType::name).toList()));
        }

        if (request.getTargetClass() != null) {
            must.add(QueryFactoryUtils.termQuery("targetClass", request.getTargetClass()));
        }

        var finalQuery = QueryBuilders.bool()
                .must(must)
                .should(should)
                .build()
                ._toQuery();

        var sortLang = request.getSortLang() != null ? request.getSortLang() : QueryFactoryUtils.DEFAULT_SORT_LANG;
        var sort = SortOptionsBuilders.field()
                .field("label." + sortLang + ".keyword")
                .order(SortOrder.Asc)
                .unmappedType(FieldType.Keyword)
                .build();

        var indices = new ArrayList<String>();
        var hasExternalNamespaces = isDefinedByCondition.stream()
                .anyMatch(ns -> !ns.startsWith(defaultNamespace));
        indices.add(OpenSearchIndexer.OPEN_SEARCH_INDEX_RESOURCE);
        if (hasExternalNamespaces) {
            indices.add(OpenSearchIndexer.OPEN_SEARCH_INDEX_EXTERNAL);
        }

        SearchRequest sr = new SearchRequest.Builder()
                .from(QueryFactoryUtils.pageFrom(request.getPageFrom()))
                .size(QueryFactoryUtils.pageSize(request.getPageSize()))
                .index(indices)
                .query(finalQuery)
                .sort(SortOptions.of(sortOptions -> sortOptions.field(sort)))
                .build();
        logPayload(sr);
        return sr;
    }

    public static SearchRequest createFindResourcesByURIQuery(Set<String> resourceURIs) {
        return new SearchRequest.Builder()
                .index(OpenSearchIndexer.OPEN_SEARCH_INDEX_RESOURCE)
                .query(QueryBuilders
                        .bool()
                        .must(QueryFactoryUtils.termsQuery("id", resourceURIs))
                        .build()
                        ._toQuery())
                .build();
    }

    /**
     * Intersect allowed data model lists. Only checks models from Interoperability platform,
     * external namespaces are always included.
     * @param fromNamespaces namespaces added to current model
     * @param restrictedDataModels models to include based on query by model type, status, group etc
     * @return intersection of restricted models
     */
    private static List<String> getIsDefinedByCondition(List<String> fromNamespaces, List<String> restrictedDataModels, String limitToDataModel) {
        List<String> isDefinedByCondition = new ArrayList<>();
        if(fromNamespaces != null && !fromNamespaces.isEmpty()) {
            if (restrictedDataModels.isEmpty()) {
                isDefinedByCondition.addAll(fromNamespaces);
            } else {
                isDefinedByCondition.addAll(
                        fromNamespaces.stream()
                            .filter(ns -> !ns.startsWith(defaultNamespace)
                                    || restrictedDataModels.contains(ns))
                            .toList()
                );
            }        	
        	
        } else {
            isDefinedByCondition.addAll(restrictedDataModels);
        }

        if (limitToDataModel != null) {
            isDefinedByCondition.add(limitToDataModel);
        }
        return isDefinedByCondition;
    }

}
