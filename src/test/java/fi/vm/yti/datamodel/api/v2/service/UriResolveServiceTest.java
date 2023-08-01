package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.mapper.MapperTestUtils;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@Import({
        UriResolveService.class
})
class UriResolveServiceTest {

    @MockBean
    private CoreRepository coreRepository;
    @Autowired
    private UriResolveService service;

    @BeforeEach
    void init() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        RequestContextHolder.setRequestAttributes(new ServletRequestAttributes(request));
    }
    @Test
    void testRedirectSiteModel() {
        var accept = "text/html";
        var response = service.resolve("http://uri.suomi.fi/datamodel/ns/test", accept);
        assertTrue(response.getStatusCode().is3xxRedirection());
        assertNotNull(response.getHeaders().getLocation());
        assertTrue(response.getHeaders().getLocation().toString().endsWith("/model/test"));
    }

    @Test
    void testRedirectSiteResource() {
        var model = MapperTestUtils.getModelFromFile("/models/test_datamodel_library_with_resources.ttl");
        when(coreRepository.fetch(anyString())).thenReturn(model);


        var pathMap = Map.of(
                "http://uri.suomi.fi/datamodel/ns/test/TestClass", "/model/test/class/TestClass",
                "http://uri.suomi.fi/datamodel/ns/test/TestAttribute", "/model/test/attribute/TestAttribute",
                "http://uri.suomi.fi/datamodel/ns/test/TestAssociation", "/model/test/association/TestAssociation"
        );
        var accept = "text/html";
        for (var key : pathMap.keySet()) {
            var response = service.resolve(key, accept);
            assertTrue(response.getStatusCode().is3xxRedirection());
            assertNotNull(response.getHeaders().getLocation());
            assertTrue(response.getHeaders().getLocation().toString().endsWith(pathMap.get(key)));
        }
    }

    @Test
    void testRedirectSerializedResource() {
        var model = MapperTestUtils.getModelFromFile("/models/test_datamodel_library_with_resources.ttl");
        when(coreRepository.fetch(anyString())).thenReturn(model);

        var accept = "text/turtle";
        var response = service.resolve("http://uri.suomi.fi/datamodel/ns/test/TestClass", accept);
        assertTrue(response.getStatusCode().is3xxRedirection());
        assertNotNull(response.getHeaders().getLocation());
        assertTrue(response.getHeaders().getLocation().toString().endsWith("/datamodel-api/v2/export/test/TestClass"));
    }

    @Test
    void testInvalidIRI() {
        var accept = "text/turtle";
        var response = service.resolve("http://invalid.com", accept);
        assertEquals("400 BAD_REQUEST", response.getStatusCode().toString());
    }
}
