package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.service.NamespaceResolver;
import fi.vm.yti.datamodel.api.v2.validator.ExceptionHandlerAdvice;
import fi.vm.yti.security.AuthenticatedUserProvider;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@TestPropertySource(properties = {
        "spring.cloud.config.import-check.enabled=false"
})
@WebMvcTest(controllers = NamespaceResolveController.class)
@ActiveProfiles("junit")
class NamespaceResolveControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private NamespaceResolver namespaceResolver;

    @MockBean
    private AuthenticatedUserProvider authenticatedUserProvider;

    @Autowired
    private NamespaceResolveController namespaceResolveController;

    @BeforeEach
    public void setup() {
        this.mvc = MockMvcBuilders
                .standaloneSetup(this.namespaceResolveController)
                .setControllerAdvice(new ExceptionHandlerAdvice())
                .build();

        when(authenticatedUserProvider.getUser()).thenReturn(EndpointUtils.mockUser);
    }

    @Test
    void shouldCallNamespaceResolver() throws Exception {
        this.mvc
                .perform(put("/v2/namespace")
                        .param("namespace", "notrealaddress"))
                .andExpect(status().isOk());
        verify(namespaceResolver).resolve(anyString(), eq(false));
    }
}
