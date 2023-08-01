package fi.vm.yti.datamodel.api.v2.opensearch;

import fi.vm.yti.datamodel.api.index.OpenSearchUtils;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.CountRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.CountSearchResponse;
import fi.vm.yti.datamodel.api.v2.opensearch.queries.CountQueryFactory;
import org.junit.jupiter.api.Test;
import org.opensearch.client.opensearch._types.aggregations.Aggregate;
import org.opensearch.client.opensearch._types.aggregations.Buckets;
import org.opensearch.client.opensearch._types.aggregations.StringTermsAggregate;
import org.opensearch.client.opensearch._types.aggregations.StringTermsBucket;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.search.HitsMetadata;
import org.opensearch.client.opensearch.core.search.TotalHits;
import org.opensearch.client.opensearch.core.search.TotalHitsRelation;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CountQueryFactoryTest {


    @Test
    void testModelCounts() throws Exception {
        String expected = OpenSearchUtils.getJsonString("/es/models_count_request.json");

        SearchRequest request = CountQueryFactory.createModelQuery(new CountRequest());

        JSONAssert.assertEquals(expected, OpenSearchUtils.getPayload(request), JSONCompareMode.LENIENT);
    }

    @Test
    void testParseModelCountResponse() {
        SearchResponse.Builder<Object> response = OpenSearchUtils.getBaseResponse();

        response
                .hits(new HitsMetadata.Builder<>()
                        .hits(List.of())
                        .total(new TotalHits.Builder()
                                .value(8)
                                .relation(TotalHitsRelation.Eq)
                                .build())
                        .build())
                .aggregations(getAggregation("statuses", Map.of(
                        Status.DRAFT.name(), 7L,
                        Status.VALID.name(), 1L
                )))
                .aggregations(getAggregation("groups", Map.of(
                        "P13", 2L,
                        "P11", 1L,
                        "P21", 1L
                )));

        CountSearchResponse countSearchResponse = CountQueryFactory.parseResponse(response.build());

        assertEquals(8, countSearchResponse.getTotalHitCount());
        Map<String, Long> groups = countSearchResponse.getCounts().getGroups();
        Map<String, Long> statuses = countSearchResponse.getCounts().getStatuses();

        assertEquals(3, groups.keySet().size());
        assertEquals(2L, groups.get("P13"));
        assertEquals(1L, groups.get("P11"));
        assertEquals(1L, groups.get("P21"));

        assertEquals(2, statuses.keySet().size());
        assertEquals(7, statuses.get(Status.DRAFT.name()));
        assertEquals(1, statuses.get(Status.VALID.name()));
    }

    private static Map<String, Aggregate> getAggregation(String key, Map<String, Long> data) {

        List<StringTermsBucket> buckets = data.entrySet().stream().map(d ->
                        new StringTermsBucket.Builder()
                                .key(d.getKey())
                                .docCount(d.getValue())
                                .build())
                .collect(Collectors.toList());

        return Map.of(key, new Aggregate.Builder()
                .sterms(new StringTermsAggregate.Builder()
                        .sumOtherDocCount(1)
                        .buckets(new Buckets.Builder<StringTermsBucket>()
                                .array(buckets)
                                .build())
                        .build())
                .build());
    }
}
