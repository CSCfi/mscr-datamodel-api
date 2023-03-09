package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.validator.ExceptionHandlerAdvice;
import fi.vm.yti.datamodel.api.v2.validator.ValidationConstants;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
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

import java.util.*;
import java.util.stream.Stream;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
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
    private JenaService jenaService;

    @MockBean
    private AuthorizationManager authorizationManager;

    @MockBean
    private OpenSearchIndexer openSearchIndexer;

    @MockBean
    private ResourceMapper mapper;

    @Autowired
    private ResourceController resourceController;


    @BeforeEach
    public void setup() {
        this.mvc = MockMvcBuilders
                .standaloneSetup(this.resourceController)
                .setControllerAdvice(new ExceptionHandlerAdvice())
                .build();

        when(authorizationManager.hasRightToAnyOrganization(anyCollection())).thenReturn(true);
        when(authorizationManager.hasRightToModel(any(), any())).thenReturn(true);
    }

    @Test
    void shouldValidateAndCreate() throws Exception {
        var resourceDTO = createResourceDTO(false);
        var mockModel = mock(Model.class);

        when(jenaService.getDataModel(anyString())).thenReturn(mockModel);
        when(mapper.mapToResource(anyString(), any(Model.class), any(ResourceDTO.class))).thenReturn("test");

        this.mvc
                .perform(put("/v2/resource/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andExpect(status().isOk());

        //Check that functions are called
        verify(this.jenaService, times(2)).doesResolvedNamespaceExist(anyString());

        verify(this.jenaService).doesResourceExistInGraph(anyString(), anyString());
        verify(this.jenaService).getDataModel(anyString());
        verify(this.mapper)
                .mapToResource(anyString(), any(Model.class), any(ResourceDTO.class));
        verify(this.jenaService).putDataModelToCore(anyString(), any(Model.class));
        verify(this.mapper).mapToIndexResource(any(Model.class), anyString());
        verifyNoMoreInteractions(this.mapper);
        verifyNoMoreInteractions(this.jenaService);
    }

    @Test
    void shouldValidateAndCreateMinimalClass() throws Exception {
        var resourceDTO = new ResourceDTO();
        resourceDTO.setIdentifier("Identifier");
        resourceDTO.setStatus(Status.DRAFT);
        resourceDTO.setLabel(Map.of("fi", "test"));
        resourceDTO.setType(ResourceType.ASSOCIATION);
        var mockModel = mock(Model.class);

        when(jenaService.getDataModel(anyString())).thenReturn(mockModel);
        when(mapper.mapToResource(anyString(), any(Model.class), any(ResourceDTO.class))).thenReturn("test");

        this.mvc
                .perform(put("/v2/resource/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andExpect(status().isOk());

        //Check that functions are called
        verify(this.jenaService).doesResourceExistInGraph(anyString(), anyString());
        verify(this.jenaService).getDataModel(anyString());
        verify(this.mapper)
                .mapToResource(anyString(), any(Model.class), any(ResourceDTO.class));
        verify(this.jenaService).putDataModelToCore(anyString(), any(Model.class));
        verifyNoMoreInteractions(this.jenaService);
        verify(this.mapper).mapToIndexResource(any(Model.class), anyString());
        verifyNoMoreInteractions(this.mapper);
    }

    @Test
    void shouldNotFindModel() throws Exception {
        var resourceDTO = createResourceDTO(false);

        //finding models from jena is not mocked so it should return null and return 404 not found
        this.mvc
                .perform(put("/v2/resource/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andExpect(status().isNotFound());
    }

    @Test
    void resourceShouldAlreadyExist() throws Exception {
        when(jenaService.doesResourceExistInGraph(anyString(), anyString())).thenReturn(true);
        var resourceDTO = createResourceDTO(false);

        //finding models from jena is not mocked so it should return null and return 404 not found
        this.mvc
                .perform(put("/v2/resource/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andExpect(status().isBadRequest())
                .andExpect(content().json("{\"status\":\"BAD_REQUEST\",\"message\":\"Error during mapping: Already exists\"}"));
    }

    @ParameterizedTest
    @MethodSource("provideCreateResourceDTOInvalidData")
    void shouldInvalidate(ResourceDTO resourceDTO) throws Exception {
        this.mvc
                .perform(put("/v2/resource/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andExpect(status().isBadRequest());
    }

    private static Stream<Arguments> provideCreateResourceDTOInvalidData() {
        var args = new ArrayList<ResourceDTO>();
        var textAreaMaxPlus = ValidationConstants.TEXT_AREA_MAX_LENGTH + 20;

        var resourceDTO = createResourceDTO(false);
        resourceDTO.setStatus(null);
        args.add(resourceDTO);

        resourceDTO = createResourceDTO(false);
        resourceDTO.setLabel(Map.of("fi", RandomStringUtils.random(textAreaMaxPlus)));
        args.add(resourceDTO);

        resourceDTO = createResourceDTO(false);
        resourceDTO.setLabel(Map.of("fi", " "));
        args.add(resourceDTO);

        resourceDTO = createResourceDTO(false);
        resourceDTO.setEditorialNote(RandomStringUtils.random(textAreaMaxPlus));
        args.add(resourceDTO);


        resourceDTO = createResourceDTO(false);
        resourceDTO.setNote(Map.of("fi", RandomStringUtils.random(textAreaMaxPlus)));
        args.add(resourceDTO);

        resourceDTO = createResourceDTO(false);
        resourceDTO.setIdentifier(null);
        args.add(resourceDTO);

        resourceDTO = createResourceDTO(false);
        resourceDTO.setType(null);
        args.add(resourceDTO);

        return args.stream().map(Arguments::of);
    }

    @Test
    void shouldValidateAndUpdate() throws Exception {
        var resourceDTO = createResourceDTO(true);
        var mockModel = mock(Model.class);

        when(jenaService.getDataModel(anyString())).thenReturn(mockModel);
        when(mockModel.getResource(anyString())).thenReturn(mock(Resource.class));
        when(jenaService.doesResourceExistInGraph(anyString(), anyString())).thenReturn(true);

        this.mvc
                .perform(put("/v2/resource/test/resource")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andExpect(status().isOk());

        //TODO add update and validate checks
    }

    @Test
    void shouldNotFindResource() throws Exception {
        var resourceDTO = createResourceDTO(true);
        when(jenaService.doesResourceExistInGraph(anyString(), anyString())).thenReturn(false);

        this.mvc
                .perform(put("/v2/resource/test/resource")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andExpect(status().isNotFound());
    }

    @ParameterizedTest
    @MethodSource("provideUpdateResourceDTOInvalidData")
    void shouldInvalidateUpdate(ResourceDTO resourceDTO) throws Exception{
        this.mvc
                .perform(put("/v2/resource/test/resource")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(resourceDTO)))
                .andExpect(status().isBadRequest());
    }

    private static Stream<Arguments> provideUpdateResourceDTOInvalidData() {
        var args = new ArrayList<ResourceDTO>();

        //this has identifier so it should fail automatically
        var resourceDTO = createResourceDTO(false);
        args.add(resourceDTO);

        return args.stream().map(Arguments::of);
    }

    private static ResourceDTO createResourceDTO(boolean update){
        var dto = new ResourceDTO();
        dto.setEditorialNote("test comment");
        if(!update){
            dto.setIdentifier("Identifier");
        }
        dto.setStatus(Status.DRAFT);
        dto.setSubject("sanastot.suomi.fi/notrealurl");
        dto.setLabel(Map.of("fi", "test label"));
        dto.setEquivalentResource(Set.of("tietomallit.suomi.fi/ns/notrealns/FakeResource"));
        dto.setSubResourceOf(Set.of("tietomallit.suomi.fi/ns/notrealns/FakeResource"));
        dto.setNote(Map.of("fi", "test note"));
        dto.setType(ResourceType.ASSOCIATION);
        return dto;
    }


}
