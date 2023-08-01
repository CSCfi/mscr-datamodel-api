package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.service.UriResolveService;
import fi.vm.yti.datamodel.api.v2.validator.ExceptionHandlerAdvice;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@TestPropertySource(properties = {
        "spring.cloud.config.import-check.enabled=false"
})
@WebMvcTest(controllers = ResolveController.class)
@ActiveProfiles("junit")
class ResolveControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private UriResolveService uriResolveService;

    @Autowired
    private ResolveController resolveController;

    @BeforeEach
    public void setup() {
        this.mvc = MockMvcBuilders
                .standaloneSetup(this.resolveController)
                .setControllerAdvice(new ExceptionHandlerAdvice())
                .build();
    }

    @Test
    void testShouldCallResolveService() throws Exception {
        when(uriResolveService.resolve(anyString(), anyString())).thenReturn(ResponseEntity.status(HttpStatus.SEE_OTHER).build());

        var accept = "text/html";
        mvc.perform(get("/v2/resolve")
                        .param("iri", "http://uri.suomi.fi/datamodel/ns/test")
                        .header(HttpHeaders.ACCEPT, accept))
                .andExpect(status().is3xxRedirection());

        verify(uriResolveService).resolve(anyString(), anyString());
        verifyNoMoreInteractions(uriResolveService);
    }
}
