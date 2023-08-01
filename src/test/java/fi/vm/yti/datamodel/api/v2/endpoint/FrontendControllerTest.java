package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.opensearch.dto.CountRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.ModelSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.ResourceSearchRequest;
import fi.vm.yti.datamodel.api.v2.service.FrontendService;
import fi.vm.yti.datamodel.api.v2.service.NamespaceService;
import fi.vm.yti.datamodel.api.v2.service.SearchIndexService;
import fi.vm.yti.datamodel.api.v2.validator.ExceptionHandlerAdvice;
import fi.vm.yti.security.AuthenticatedUserProvider;
import fi.vm.yti.security.YtiUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@TestPropertySource(properties = {
        "spring.cloud.config.import-check.enabled=false"
})
@WebMvcTest(controllers = FrontendController.class)
@ActiveProfiles("junit")
class FrontendControllerTest {

    @Autowired
    private MockMvc mvc;

    @Autowired
    FrontendController frontendController;

    @MockBean
    SearchIndexService searchIndexService;
    @MockBean
    FrontendService frontendService;
    @MockBean
    AuthenticatedUserProvider userProvider;
    @MockBean
    NamespaceService namespaceService;

    @BeforeEach
    void init () {
        this.mvc = MockMvcBuilders
                .standaloneSetup(this.frontendController)
                .setControllerAdvice(new ExceptionHandlerAdvice())
                .build();

        when(userProvider.getUser()).thenReturn(EndpointUtils.mockUser);
    }

    @Test
    void getCounts() throws Exception {
        this.mvc.perform(get("/v2/frontend/counts")
                        .contentType("application/json"))
                .andExpect(status().isOk());
        verify(searchIndexService).getCounts(any(CountRequest.class));
    }

    @Test
    void searchInternalResourcesTest() throws Exception {
        this.mvc.perform(get("/v2/frontend/search-internal-resources")
                            .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(new ResourceSearchRequest())))
                        .andExpect(status().isOk());
        verify(searchIndexService).searchInternalResources(any(ResourceSearchRequest.class), any(YtiUser.class));

    }

    @Test
    void searchInternalResourcesInfoTest() throws Exception {
        this.mvc.perform(get("/v2/frontend/search-internal-resources-info")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(new ResourceSearchRequest())))
                .andExpect(status().isOk());
        verify(searchIndexService).searchInternalResourcesWithInfo(any(ResourceSearchRequest.class), any(YtiUser.class));
    }


    @Test
    void searchModelsTest() throws Exception {
        this.mvc.perform(get("/v2/frontend/search-models")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(new ModelSearchRequest())))
                .andExpect(status().isOk());
        verify(searchIndexService).searchModels(any(ModelSearchRequest.class), any(YtiUser.class));
    }

    @Test
    void getServiceCategories() throws Exception {
        this.mvc.perform(get("/v2/frontend/service-categories")
                        .contentType("application/json"))
                .andExpect(status().isOk());
        verify(frontendService).getServiceCategories("fi");

        this.mvc.perform(get("/v2/frontend/service-categories")
                        .param("sortLang", "en")
                        .contentType("application/json"))
                .andExpect(status().isOk());
        verify(frontendService).getServiceCategories("en");
    }

    @Test
    void getOrganizations() throws Exception {
        this.mvc.perform(get("/v2/frontend/organizations")
                        .contentType("application/json"))
                .andExpect(status().isOk());
        verify(frontendService).getOrganizations("fi", false);

        this.mvc.perform(get("/v2/frontend/organizations")
                        .param("sortLang", "en")
                        .contentType("application/json"))
                .andExpect(status().isOk());
        verify(frontendService).getOrganizations("en", false);

        this.mvc.perform(get("/v2/frontend/organizations")
                        .param("includeChildOrganizations", "true")
                        .contentType("application/json"))
                .andExpect(status().isOk());
        verify(frontendService).getOrganizations(anyString(), eq(true));
    }

    @Test
    void getNamespacesTest() throws Exception {
        this.mvc.perform(get("/v2/frontend/namespaces")
                        .contentType("application/json"))
                .andExpect(status().isOk());
        verify(namespaceService).getResolvedNamespaces();
    }

}
