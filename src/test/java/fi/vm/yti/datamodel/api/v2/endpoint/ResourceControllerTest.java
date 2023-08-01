package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.dto.PropertyShapeDTO;
import fi.vm.yti.datamodel.api.v2.dto.ResourceDTO;
import fi.vm.yti.datamodel.api.v2.dto.ResourceType;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import fi.vm.yti.datamodel.api.v2.service.ResourceService;
import fi.vm.yti.datamodel.api.v2.validator.ExceptionHandlerAdvice;
import fi.vm.yti.datamodel.api.v2.validator.ValidationConstants;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@TestPropertySource(properties = {
        "spring.cloud.config.import-check.enabled=false"
})
@WebMvcTest(ResourceController.class)
@ActiveProfiles("junit")
class ResourceControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private ResourceService resourceService;

    @MockBean
    ImportsRepository importsRepository;

    @Autowired
    private ResourceController resourceController;

    
    @Value("${defaultNamespace}")
    private String defaultNamespace;

    @BeforeEach
    public void setup() {
        this.mvc = MockMvcBuilders
                .standaloneSetup(this.resourceController)
                .setControllerAdvice(new ExceptionHandlerAdvice())
                .build();
    }

    @ParameterizedTest
    @CsvSource({"attribute", "association"})
    void shouldValidateAndCreate(String resourceType) throws Exception {
        var resourceDTO = createResourceDTO(false, resourceType);
        when(resourceService.checkIfResourceIsOneOfTypes(eq("http://uri.suomi.fi/datamodel/ns/int/FakeClass"), anyList(), anyBoolean())).thenReturn(true);

        this.mvc
                .perform(post("/v2/resource/library/test/{resourceType}", resourceType)
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andDo(print())
                .andExpect(status().isCreated());
        //validator
        if(resourceType.equals("attribute")){
            verify(resourceService, times(1)).checkIfResourceIsOneOfTypes(anyString(), anyList(), anyBoolean());
        }else{
            verify(resourceService, times(2)).checkIfResourceIsOneOfTypes(anyString(), anyList(), anyBoolean());
        }
        //controller
        verify(resourceService).create(anyString(), any(ResourceDTO.class), any(ResourceType.class), eq(false));
        verifyNoMoreInteractions(resourceService);
    }

    @ParameterizedTest
    @CsvSource({"attribute", "association"})
    void shouldValidateAndCreateMinimalResource(String resourceType) throws Exception {
        var resourceDTO = new ResourceDTO();
        resourceDTO.setIdentifier("Identifier");
        resourceDTO.setStatus(Status.DRAFT);
        resourceDTO.setLabel(Map.of("fi", "test"));

        this.mvc
                .perform(post("/v2/resource/library/test/{resourceType}", resourceType)
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andExpect(status().isCreated());

        verify(resourceService).create(anyString(), any(ResourceDTO.class), any(ResourceType.class), eq(false));
        verifyNoMoreInteractions(resourceService);
    }

    @ParameterizedTest
    @MethodSource("provideCreateResourceDTOInvalidData")
    void shouldInvalidate(String resourceType, ResourceDTO resourceDTO) throws Exception {
        this.mvc
                .perform(post("/v2/resource/library/test/{resourceType}", resourceType)
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andExpect(status().isBadRequest());
    }

    private static Stream<Arguments> provideCreateResourceDTOInvalidData() {
        var args = new ArrayList<Arguments>();
        var textAreaMaxPlus = ValidationConstants.TEXT_AREA_MAX_LENGTH + 20;

        var testType = "association";
        var resourceDTO = createResourceDTO(false, testType);
        resourceDTO.setStatus(null);
        args.add(Arguments.of(testType, resourceDTO));

        resourceDTO = createResourceDTO(false, testType);
        resourceDTO.setLabel(Map.of("fi", RandomStringUtils.random(textAreaMaxPlus)));
        args.add(Arguments.of(testType, resourceDTO));

        resourceDTO = createResourceDTO(false, testType);
        resourceDTO.setLabel(Map.of("fi", " "));
        args.add(Arguments.of(testType, resourceDTO));

        resourceDTO = createResourceDTO(false, testType);
        resourceDTO.setEditorialNote(RandomStringUtils.random(textAreaMaxPlus));
        args.add(Arguments.of(testType, resourceDTO));

        resourceDTO = createResourceDTO(false, testType);
        resourceDTO.setNote(Map.of("fi", RandomStringUtils.random(textAreaMaxPlus)));
        args.add(Arguments.of(testType, resourceDTO));

        resourceDTO = createResourceDTO(false, testType);
        resourceDTO.setIdentifier(null);
        args.add(Arguments.of(testType, resourceDTO));

        resourceDTO = createResourceDTO(false, testType);
        resourceDTO.setDomain("http://uri.suomi.fi/datamodel/ns/int/InvalidClass");
        args.add(Arguments.of(testType, resourceDTO));

        resourceDTO = createResourceDTO(false, "attribute");
        resourceDTO.setRange("notreal:type");
        args.add(Arguments.of("attribute", resourceDTO));

        resourceDTO = createResourceDTO(false, "association");
        resourceDTO.setRange("http://uri.suomi.fi/datamodel/ns/int/InvalidClass");
        args.add(Arguments.of("association", resourceDTO));

        return args.stream();
    }

    @ParameterizedTest
    @CsvSource({"attribute", "association"})
    void shouldValidateAndUpdate(String resourceType) throws Exception {
        var resourceDTO = createResourceDTO(true, resourceType);
        when(resourceService.checkIfResourceIsOneOfTypes(eq("http://uri.suomi.fi/datamodel/ns/int/FakeClass"), anyList(), anyBoolean())).thenReturn(true);

        this.mvc
                .perform(put("/v2/resource/library/test/{resourceType}/TestA{resourceType}", resourceType, resourceType.substring(1))
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andExpect(status().isNoContent());

        //validator
        if(resourceType.equals("attribute")){
            verify(resourceService, times(1)).checkIfResourceIsOneOfTypes(anyString(), anyList(), anyBoolean());
        }else{
            verify(resourceService, times(2)).checkIfResourceIsOneOfTypes(anyString(), anyList(), anyBoolean());
        }
        verify(resourceService).update(anyString(), anyString(), any(ResourceDTO.class));
        verifyNoMoreInteractions(resourceService);
    }

    @ParameterizedTest
    @MethodSource("provideUpdateResourceDTOInvalidData")
    void shouldInvalidateUpdate(ResourceDTO resourceDTO) throws Exception{
        this.mvc
                .perform(put("/v2/resource/library/test/association/resource")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldGetResource() throws Exception {
        mvc.perform(get("/v2/resource/library/test/TestAttribute"))
                .andExpect(status().isOk());
        verify(resourceService).get(anyString(), anyString());
        verifyNoMoreInteractions(resourceService);
    }


    @Test
    void shouldDeleteResource() throws Exception {
        mvc.perform(delete("/v2/resource/library/test/resource"))
                .andExpect(status().isOk());

        verify(resourceService).delete(anyString(), anyString());
    }



    @Test
    void shouldValidateAndCreatePropertyShape() throws Exception {
        var dto = createPropertyShapeDTO();

        when(resourceService.checkIfResourceIsOneOfTypes(eq("http://uri.suomi.fi/datamodel/ns/int/FakeClass"), anyList(),
                anyBoolean())).thenReturn(true);

        this.mvc
                .perform(post("/v2/resource/profile/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(dto)))
                .andExpect(status().isCreated());

        verify(resourceService, times(2)).checkIfResourceIsOneOfTypes(anyString(), anyList(), anyBoolean());
        verify(resourceService).create(anyString(), any(PropertyShapeDTO.class), eq(null), eq(true));
        verifyNoMoreInteractions(resourceService);
    }

    @Test
    void shouldCopy() throws Exception {
            this.mvc
                    .perform(post("/v2/resource/profile/test/PropertyShape")
                            .contentType("application/json")
                            .param("targetPrefix", "newtest")
                            .param("newIdentifier", "newid"))
                    .andExpect(status().isCreated());

            verify(resourceService).copyPropertyShape(anyString(), anyString(), anyString(), anyString());
    }

    @ParameterizedTest
    @MethodSource("provideCreatePropertyShapeDTOInvalidData")
    void shouldInvalidatePropertyShape(PropertyShapeDTO dto) throws Exception {
        this.mvc
                .perform(post("/v2/resource/profile/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(dto)))
                .andExpect(status().isBadRequest());
    }


    @Test
    void shouldCheckFreeIdentifierWhenExists() throws Exception {
        when(resourceService.exists("test", "Resource")).thenReturn(true);

        this.mvc
                .perform(get("/v2/resource/test/Resource/exists")
                        .contentType("application/json"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("true")));
    }

    @Test
    void shouldCheckFreeIdentifierWhenNotExist() throws Exception {
        this.mvc
                .perform(get("/v2/resource/test/Resource/exists")
                        .contentType("application/json"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("false")));
    }


    private static Stream<Arguments> provideCreatePropertyShapeDTOInvalidData() {
        var args = new ArrayList<PropertyShapeDTO>();
        var length = ValidationConstants.TEXT_FIELD_MAX_LENGTH;

        var dto = createPropertyShapeDTO();
        dto.setPath("http://uri.suomi.fi/invalid");
        args.add(dto);

        dto = createPropertyShapeDTO();
        dto.setDataType("invalid");
        args.add(dto);

        dto = createPropertyShapeDTO();
        dto.setDefaultValue(RandomStringUtils.random(length + 1));
        args.add(dto);

        dto = createPropertyShapeDTO();
        dto.setHasValue(RandomStringUtils.random(length + 1));
        args.add(dto);

        dto = createPropertyShapeDTO();
        dto.setAllowedValues(List.of(RandomStringUtils.random(length + 1)));
        args.add(dto);

        dto = createPropertyShapeDTO();
        dto.setCodeList("http://uri.suomi.fi/invalid");
        args.add(dto);

        return args.stream().map(Arguments::of);
    }
    private static Stream<Arguments> provideUpdateResourceDTOInvalidData() {
        var args = new ArrayList<ResourceDTO>();

        //this has identifier so it should fail automatically
        var resourceDTO = createResourceDTO(false, "association");
        args.add(resourceDTO);

        return args.stream().map(Arguments::of);
    }

    private static ResourceDTO createResourceDTO(boolean update, String resourceType){
        var dto = new ResourceDTO();
        dto.setEditorialNote("test comment");
        if(!update){
            dto.setIdentifier("Identifier");
        }
        dto.setStatus(Status.DRAFT);
        dto.setSubject("sanastot.suomi.fi/notrealurl");
        dto.setLabel(Map.of("fi", "test label"));
        dto.setEquivalentResource(Set.of("http://uri.suomi.fi/datamodel/ns/int/FakeResource"));
        dto.setSubResourceOf(Set.of("http://uri.suomi.fi/datamodel/ns/int/FakeResource"));
        dto.setDomain("http://uri.suomi.fi/datamodel/ns/int/FakeClass");
        if(resourceType.equals("association")){
            dto.setRange("http://uri.suomi.fi/datamodel/ns/int/FakeClass");
        }else{
            dto.setRange("owl:real");
        }
        dto.setNote(Map.of("fi", "test note"));
        return dto;
    }

    private static PropertyShapeDTO createPropertyShapeDTO() {
        var dto = new PropertyShapeDTO();
        dto.setLabel(Map.of("fi", "test label"));
        dto.setIdentifier("Identifier");
        dto.setType(ResourceType.ASSOCIATION);
        dto.setStatus(Status.DRAFT);
        dto.setSubject("sanastot.suomi.fi/notrealurl");
        dto.setPath("http://uri.suomi.fi/datamodel/ns/int/FakeClass");
        dto.setClassType("http://uri.suomi.fi/datamodel/ns/int/FakeClass");
        return dto;
    }
}
