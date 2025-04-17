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
    void testRedirectSchema() {
        var accept = "text/html";
        var response = service.resolve("mscr:schema:12312312321", accept);
        assertTrue(response.getStatusCode().is3xxRedirection());
        assertNotNull(response.getHeaders().getLocation());
        assertTrue(response.getHeaders().getLocation().toString().endsWith("/schema/mscr:schema:12312312321"));
    }
    
    @Test
    void testRedirectCrosswalk() {
        var accept = "text/html";
        var response = service.resolve("mscr:crosswalk:12312312321", accept);
        assertTrue(response.getStatusCode().is3xxRedirection());
        assertNotNull(response.getHeaders().getLocation());
        assertTrue(response.getHeaders().getLocation().toString().endsWith("/crosswalk/mscr:crosswalk:12312312321"));
    }  

    @Test
    void testRedirectSchemaJSON() {
        var accept = "application/json";
        var response = service.resolve("mscr:schema:12312312321", accept);
        assertTrue(response.getStatusCode().is3xxRedirection());
        assertNotNull(response.getHeaders().getLocation());
        assertTrue(response.getHeaders().getLocation().toString().endsWith("/datamodel-api/v2/schema/mscr:schema:12312312321"));
    }
    
    @Test
    void testRedirectCrosswalkJSON() {
        var accept = "application/json";
        var response = service.resolve("mscr:crosswalk:12312312321", accept);
        System.out.println(response.getHeaders().getLocation().toString());
        assertTrue(response.getStatusCode().is3xxRedirection());
        assertNotNull(response.getHeaders().getLocation());
        assertTrue(response.getHeaders().getLocation().toString().endsWith("/datamodel-api/v2/crosswalk/mscr:crosswalk:12312312321"));
    }   
    
    @Test
    void testRedirectMappingJSON() {
        var accept = "application/json";
        var response = service.resolve("mscr:crosswalk:12312312321@mapping=test", accept);
        System.out.println(response.getHeaders().getLocation().toString());
        assertTrue(response.getStatusCode().is3xxRedirection());
        assertNotNull(response.getHeaders().getLocation());
        assertTrue(response.getHeaders().getLocation().toString().endsWith("/crosswalk/mscr:crosswalk:12312312321/mapping"));
    }     
    
    @Test
    void testRedirectRandom() {
        var accept = "text/html";
        var response = service.resolve("random_string", accept);
        assertTrue(response.getStatusCode().is4xxClientError());
    }

    @Test
    void testRedirectError() {
        var accept = "text/html";
        var response = service.resolve("test:test:test", accept);
        assertTrue(response.getStatusCode().is4xxClientError());
    }
    
 

}
