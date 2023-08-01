package fi.vm.yti.datamodel.api.v2.opensearch.queries;

import fi.vm.yti.datamodel.api.v2.dto.ModelType;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.CountDTO;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.CountRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.CountSearchResponse;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import org.opensearch.client.opensearch._types.aggregations.Aggregation;
import org.opensearch.client.opensearch._types.aggregations.AggregationBuilders;
import org.opensearch.client.opensearch._types.aggregations.StringTermsBucket;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch._types.query_dsl.QueryBuilders;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static fi.vm.yti.datamodel.api.v2.opensearch.OpenSearchUtil.logPayload;
public class CountQueryFactory {

    private CountQueryFactory(){
        //Static class
    }
    public static SearchRequest createModelQuery(CountRequest request) {
        var must = new ArrayList<Query>();
        var should = new ArrayList<Query>();

        var incompleteFrom = request.getIncludeIncompleteFrom();
        if(incompleteFrom != null && !incompleteFrom.isEmpty()){
            var incompleteFromQuery = QueryFactoryUtils.termsQuery("contributor", incompleteFrom.stream().map(UUID::toString).toList());
            should.add(incompleteFromQuery);
        }

        should.add(QueryFactoryUtils.hideIncompleteStatusQuery());

        var queryString = request.getQuery();
        if(queryString != null && !queryString.isBlank()){
            must.add(QueryFactoryUtils.labelQuery(queryString));
        }

        var modelType = request.getType();
        if(modelType != null && !modelType.isEmpty()){
            var modelTypeQuery = QueryFactoryUtils.termsQuery("type", modelType.stream().map(ModelType::name).toList());
            must.add(modelTypeQuery);
        }

        var groups = request.getGroups();
        if(groups != null && !groups.isEmpty()){
            var groupsQuery = QueryFactoryUtils.termsQuery("isPartOf", groups);
            must.add(groupsQuery);
        }

        var organizations = request.getOrganizations();
        if(organizations != null && !organizations.isEmpty()){
            var orgsQuery = QueryFactoryUtils.termsQuery("contributor", organizations.stream().map(UUID::toString).toList());
            must.add(orgsQuery);
        }

        var language = request.getLanguage();
        if(language != null && !language.isBlank()) {
            var languageQuery = QueryFactoryUtils.termQuery("language", language);
            must.add(languageQuery);
        }

        var status = request.getStatus();
        if(status != null && !status.isEmpty()){
            var statusQuery = QueryFactoryUtils.termsQuery("status", status.stream().map(Status::name).toList());
            must.add(statusQuery);
        }

        var finalQuery = QueryBuilders.bool()
                .must(must)
                .minimumShouldMatch("1")
                .should(should)
                .build()
                ._toQuery();

        var sr = new SearchRequest.Builder()
                .index(OpenSearchIndexer.OPEN_SEARCH_INDEX_MODEL)
                .size(0)
                .query(finalQuery)
                .aggregations("statuses", getAggregation("status"))
                .aggregations("types", getAggregation("type"))
                .aggregations("languages", getAggregation("language"))
                .aggregations("groups", getAggregation("isPartOf"))
                .build();

        logPayload(sr);
        return sr;
    }

    private static Aggregation getAggregation(String fieldName) {
        return AggregationBuilders.terms().field(fieldName).build()._toAggregation();
    }

    public static CountSearchResponse parseResponse(SearchResponse<?> response) {
        var ret = new CountSearchResponse();
        var counts = new CountDTO(
                getBucketValues(response, "statuses"),
                getBucketValues(response, "languages"),
                getBucketValues(response, "types"),
                getBucketValues(response, "groups")
        );
        ret.setTotalHitCount(response.hits().total().value());
        ret.setCounts(counts);
        return ret;
    }

    private static Map<String, Long> getBucketValues(SearchResponse<?> response, String aggregateName) {
        var aggregation = response.aggregations().get(aggregateName);

        if (aggregation == null) {
            return Collections.emptyMap();
        }
        return aggregation
                .sterms()
                .buckets()
                .array()
                .stream()
                .collect(Collectors.toMap(StringTermsBucket::key, StringTermsBucket::docCount));
    }

}
