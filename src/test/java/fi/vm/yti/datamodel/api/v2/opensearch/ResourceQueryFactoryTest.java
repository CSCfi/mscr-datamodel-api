package fi.vm.yti.datamodel.api.v2.opensearch;

import fi.vm.yti.datamodel.api.index.OpenSearchUtils;
import fi.vm.yti.datamodel.api.v2.dto.ResourceType;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.ResourceSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.queries.QueryFactoryUtils;
import fi.vm.yti.datamodel.api.v2.opensearch.queries.ResourceQueryFactory;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.util.*;

import static org.springframework.test.util.AssertionErrors.assertEquals;


class ResourceQueryFactoryTest {

    @Test
    void createInternalClassQueryValues() throws Exception {
        var request = new ResourceSearchRequest();
        request.setQuery("test query");
        request.setGroups(Set.of("P11", "P1"));
        request.setLimitToDataModel("http://uri.suomi.fi/datamodel/ns/test");
        request.setFromAddedNamespaces(true);
        request.setPageFrom(1);
        request.setPageSize(100);
        request.setStatus(Set.of(Status.DRAFT, Status.VALID));
        request.setSortLang("en");
        request.setTargetClass("http://uri.suomi.fi/datamodel/ns/test/TestClass");
        request.setResourceTypes(Set.of(ResourceType.ATTRIBUTE, ResourceType.ASSOCIATION));

        var groupNamespaces = List.of("http://uri.suomi.fi/datamodel/ns/groupNs", "http://uri.suomi.fi/datamodel/ns/addedNs");
        var addedNamespaces = List.of("http://uri.suomi.fi/datamodel/ns/addedNs", "http://external-data.com/test");

        var allowedIncompleteDatamodels = Set.of("http://uri.suomi.fi/datamodel/ns/test");

        var classQuery = ResourceQueryFactory.createInternalResourceQuery(request, addedNamespaces, groupNamespaces, allowedIncompleteDatamodels);

        String expected = OpenSearchUtils.getJsonString("/es/classRequest.json");
        JSONAssert.assertEquals(expected, OpenSearchUtils.getPayload(classQuery), JSONCompareMode.LENIENT);

        assertEquals("Page from value not matching", 1, classQuery.from());
        assertEquals("Page size value not matching", 100, classQuery.size());
    }

    @Test
    void listAttributesInModelWithAdditionalResources() throws Exception {
        var request = new ResourceSearchRequest();

        request.setPageFrom(1);
        request.setPageSize(100);
        request.setLimitToDataModel("http://uri.suomi.fi/datamodel/ns/test");
        request.setResourceTypes(Set.of(ResourceType.ATTRIBUTE));
        request.setAdditionalResources(Set.of("http://uri.suomi.fi/datamodel/ns/ext/some-property"));

        var attributeListQuery = ResourceQueryFactory.createInternalResourceQuery(request, new ArrayList<>(),
                new ArrayList<>(), new HashSet<>());
        String expected = OpenSearchUtils.getJsonString("/es/attributeListRequest.json");
        JSONAssert.assertEquals(expected, OpenSearchUtils.getPayload(attributeListQuery), JSONCompareMode.LENIENT);
    }

    @Test
    void createInternalClassQueryDefaults() {
        var request = new ResourceSearchRequest();

        var classQuery = ResourceQueryFactory.createInternalResourceQuery(request, Collections.emptyList(), Collections.emptyList(), Collections.emptySet());

        assertEquals("Page from value not matching", QueryFactoryUtils.DEFAULT_PAGE_FROM, classQuery.from());
        assertEquals("Page size value not matching", QueryFactoryUtils.DEFAULT_PAGE_SIZE, classQuery.size());
        assertEquals("Label should be sorted in finnish by default", "label.fi.keyword", classQuery.sort().get(0).field().field());
    }
}
