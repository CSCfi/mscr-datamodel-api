package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.dto.ClassDTO;
import fi.vm.yti.datamodel.api.v2.dto.NodeShapeDTO;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import fi.vm.yti.datamodel.api.v2.service.ClassService;
import fi.vm.yti.datamodel.api.v2.service.ResourceService;
import fi.vm.yti.datamodel.api.v2.validator.ExceptionHandlerAdvice;
import fi.vm.yti.datamodel.api.v2.validator.ValidationConstants;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@TestPropertySource(properties = {
        "spring.cloud.config.import-check.enabled=false"
})
@WebMvcTest(ClassController.class)
@ActiveProfiles("junit")
class ClassControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private ClassService classService;
    @MockBean
    ResourceService resourceService;
    @MockBean
    private ImportsRepository importsRepository;

    @Autowired
    private ClassController classController;

    @BeforeEach
    public void setup() {
        this.mvc = MockMvcBuilders
                .standaloneSetup(this.classController)
                .setControllerAdvice(new ExceptionHandlerAdvice())
                .build();
    }

    @Test
    void shouldValidateAndCreateClass() throws Exception {
        var classDTO = createClassDTO(false);

        this.mvc
            .perform(post("/v2/class/library/test")
                    .contentType("application/json")
                    .content(EndpointUtils.convertObjectToJsonString(classDTO)))
            .andExpect(status().isCreated());

        //Check that functions are called
        verify(classService).create(anyString(), any(ClassDTO.class), eq(false));

    }

    @Test
    void shouldValidateAndCreateMinimalClass() throws Exception {
        var classDTO = new ClassDTO();
        classDTO.setIdentifier("Identifier");
        classDTO.setStatus(Status.DRAFT);
        classDTO.setLabel(Map.of("fi", "test"));

       this.mvc
                .perform(post("/v2/class/library/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(classDTO)))
                .andExpect(status().isCreated());

        //Check that functions are called
        verify(classService).create(anyString(), any(ClassDTO.class), eq(false));
    }

    @ParameterizedTest
    @MethodSource("provideCreateClassDTOInvalidData")
    void shouldInvalidate(ClassDTO classDTO) throws Exception {
        this.mvc
                .perform(post("/v2/class/library/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(classDTO)))
                .andExpect(status().isBadRequest());
    }

    private static Stream<Arguments> provideCreateClassDTOInvalidData() {
        var args = new ArrayList<ClassDTO>();
        var textAreaMaxPlus = ValidationConstants.TEXT_AREA_MAX_LENGTH + 20;

        var classDTO = createClassDTO(false);
        classDTO.setStatus(null);
        args.add(classDTO);

        classDTO = createClassDTO(false);
        classDTO.setLabel(Map.of("fi", RandomStringUtils.random(textAreaMaxPlus)));
        args.add(classDTO);

        classDTO = createClassDTO(false);
        classDTO.setLabel(Map.of("fi", " "));
        args.add(classDTO);

        classDTO = createClassDTO(false);
        classDTO.setEditorialNote(RandomStringUtils.random(textAreaMaxPlus));
        args.add(classDTO);


        classDTO = createClassDTO(false);
        classDTO.setNote(Map.of("fi", RandomStringUtils.random(textAreaMaxPlus)));
        args.add(classDTO);

        classDTO = createClassDTO(false);
        classDTO.setIdentifier(null);
        args.add(classDTO);

        return args.stream().map(Arguments::of);
    }

    @Test
    void shouldValidateAndUpdate() throws Exception {
        var classDTO = createClassDTO(true);

        this.mvc
                .perform(put("/v2/class/library/test/class")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(classDTO)))
                .andExpect(status().isNoContent());

        //Check that functions are called
        verify(classService).update(anyString(), anyString(), any(ClassDTO.class));
    }

    @ParameterizedTest
    @MethodSource("provideUpdateClassDTOInvalidData")
    void shouldInvalidateUpdate(ClassDTO classDTO) throws Exception{
        this.mvc
                .perform(put("/v2/class/library/test/class")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(classDTO)))
                .andExpect(status().isBadRequest());
    }

    private static Stream<Arguments> provideUpdateClassDTOInvalidData() {
        var args = new ArrayList<ClassDTO>();

        //this has identifier so it should fail automatically
        var classDTO = createClassDTO(false);
        args.add(classDTO);

        return args.stream().map(Arguments::of);
    }

    @Test
    void shouldGetClass() throws Exception {
        mvc.perform(get("/v2/class/library/test/TestClass"))
                .andExpect(status().isOk());
    }

    @Test
    void shouldDeleteClass() throws Exception {
        mvc.perform(delete("/v2/class/library/test/class"))
                .andExpect(status().isOk());

        verify(classService).delete(anyString(), anyString());
    }

    @Test
    void shouldValidateAndCreateNodeShape() throws Exception {
        var nodeShapeDTO = createNodeShapeDTO();
        nodeShapeDTO.setProperties(Set.of("test"));

        this.mvc
                .perform(post("/v2/class/profile/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(nodeShapeDTO)))
                .andExpect(status().isCreated());

        verify(classService).create(anyString(), any(NodeShapeDTO.class), eq(true));
    }

    @Test
    void shouldCheckFreeIdentifierWhenExists() throws Exception {
        when(classService.exists(anyString(), anyString())).thenReturn(true);

        this.mvc
                .perform(get("/v2/class/test/Resource/exists")
                        .contentType("application/json"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("true")));
    }

    @Test
    void shouldCheckFreeIdentifierWhenNotExist() throws Exception {
        this.mvc
                .perform(get("/v2/class/test/Resource/exists")
                        .contentType("application/json"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("false")));
    }


    private static ClassDTO createClassDTO(boolean update){
        var dto = new ClassDTO();
        dto.setEditorialNote("test comment");
        if(!update){
            dto.setIdentifier("Identifier");
        }
        dto.setStatus(Status.DRAFT);
        dto.setSubject("http://uri.suomi.fi/terminology/notrealurl");
        dto.setLabel(Map.of("fi", "test label"));
        dto.setEquivalentClass(Set.of("http://uri.suomi.fi/datamodel/ns/notrealns/FakeClass"));
        dto.setSubClassOf(Set.of("http://uri.suomi.fi/datamodel/ns/notrealns/FakeClass"));
        dto.setNote(Map.of("fi", "test note"));
        return dto;
    }

    private static NodeShapeDTO createNodeShapeDTO() {
        var dto = new NodeShapeDTO();
        dto.setLabel(Map.of("fi", "node label"));
        dto.setIdentifier("node-shape-1");
        dto.setStatus(Status.DRAFT);
        dto.setSubject("http://uri.suomi.fi/terminology/concept-123");

        return dto;
    }
}
