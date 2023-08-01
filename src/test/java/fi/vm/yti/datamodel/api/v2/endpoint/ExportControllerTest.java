package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.service.DataModelService;
import fi.vm.yti.datamodel.api.v2.validator.ExceptionHandlerAdvice;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@TestPropertySource(properties = {
        "spring.cloud.config.import-check.enabled=false"
})
@WebMvcTest(controllers = ExportController.class)
@ActiveProfiles("junit")
class ExportControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private DataModelService dataModelService;

    @Autowired
    private ExportController exportController;

    @BeforeEach
    public void setup() {
        this.mvc = MockMvcBuilders
                .standaloneSetup(this.exportController)
                .setControllerAdvice(new ExceptionHandlerAdvice())
                .build();
    }

    @Test
    void shouldCallDataModelService() throws Exception {
        mvc.perform(get("/v2/export/test")
                .header("Accept", "application/ld+json"))
                .andExpect(status().isOk());
        verify(dataModelService).export("test", null, "application/ld+json");
    }

    @Test
    void shouldGetModelAsFileNotSupportedType() throws Exception {
        mvc.perform(get("/v2/export/test")
                        .header("Accept", "application/pdf"))
                .andExpect(status().isNotAcceptable());
    }
}
