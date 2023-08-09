package fi.vm.yti.datamodel.api.v2.opensearch;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import fi.vm.yti.datamodel.api.index.OpenSearchUtils;
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.MSCRSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.queries.MSCRQueryFactory;

public class MSCRQueryFactoryTest {

	
	@Test
	void createMSCRQueryOperatorTest() throws Exception {
		var r = new MSCRSearchRequest();
		
		r.setStatus(Set.of(Status.DRAFT, Status.VALID));
		r.setType(Set.of(MSCRType.CROSSWALK));
		
		r.setIncludeFacets(true);
		var query = MSCRQueryFactory.createMSCRQuery(r);
		
        String expected = OpenSearchUtils.getJsonString("/es/mscrSearchRequest.json");
        JSONAssert.assertEquals(expected, OpenSearchUtils.getPayload(query), JSONCompareMode.LENIENT);

	}
}
