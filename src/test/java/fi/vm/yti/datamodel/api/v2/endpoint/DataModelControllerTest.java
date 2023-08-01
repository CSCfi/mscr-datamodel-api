package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.service.DataModelService;
import fi.vm.yti.datamodel.api.v2.validator.ExceptionHandlerAdvice;
import fi.vm.yti.datamodel.api.v2.validator.ValidationConstants;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.vocabulary.SKOS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.*;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@TestPropertySource(properties = {
        "spring.cloud.config.import-check.enabled=false"
})
@WebMvcTest(controllers = DataModelController.class)
@ActiveProfiles("junit")
class DataModelControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private DataModelService dataModelService;

    @MockBean
    private CoreRepository coreRepository;

    @Autowired
    private DataModelController dataModelController;

    private static final UUID RANDOM_ORG = UUID.randomUUID();

    @BeforeEach
    public void setup() {
        this.mvc = MockMvcBuilders
                .standaloneSetup(this.dataModelController)
                .setControllerAdvice(new ExceptionHandlerAdvice())
                .build();
    }

    @Test
    void shouldValidateAndCreate() throws Exception {
        var dataModelDTO = createDatamodelDTO(false);
        //Mock validation stuff
        var mockModel = ModelFactory.createDefaultModel();
        var res = mockModel.createResource(ModelConstants.URN_UUID + RANDOM_ORG);
        res.addProperty(SKOS.notation, "P11");
        when(coreRepository.getOrganizations()).thenReturn(mockModel);
        when(coreRepository.getServiceCategories()).thenReturn(mockModel);

        this.mvc
                .perform(post("/v2/model/library")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(dataModelDTO)))
                .andExpect(status().isCreated());

        //Check that functions are called
        verify(dataModelService).create(any(DataModelDTO.class), any(ModelType.class));
        verifyNoMoreInteractions(this.dataModelService);
    }

    @Test
    void shouldValidateAndUpdate() throws Exception {
        var dataModelDTO = createDatamodelDTO(true);

        //Mock validation stuff
        var mockModel = ModelFactory.createDefaultModel();
        var res = mockModel.createResource(ModelConstants.URN_UUID + RANDOM_ORG);
        res.addProperty(SKOS.notation, "P11");
        when(coreRepository.getOrganizations()).thenReturn(mockModel);
        when(coreRepository.getServiceCategories()).thenReturn(mockModel);

        this.mvc
                .perform(put("/v2/model/library/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(dataModelDTO)))
                .andExpect(status().isNoContent());

        //Check that functions are called
        verify(dataModelService).update(anyString(), any(DataModelDTO.class));
        verifyNoMoreInteractions(dataModelService);
    }

    @Test
    void shouldReturnModel() throws Exception {
        when(dataModelService.get(anyString())).thenReturn(new DataModelInfoDTO());
        this.mvc
                .perform(get("/v2/model/test")
                        .accept(MediaType.APPLICATION_JSON_VALUE)
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE));

        verify(dataModelService).get(anyString());
        verifyNoMoreInteractions(dataModelService);
    }

    @ParameterizedTest
    @MethodSource("provideDataModelInvalidData")
    void shouldInValidate(DataModelDTO dataModelDTO) throws Exception {
        //Mock validation stuff
        var mockModel = ModelFactory.createDefaultModel();
        var res = mockModel.createResource(ModelConstants.URN_UUID + RANDOM_ORG);
        res.addProperty(SKOS.notation, "P11");
        when(coreRepository.getOrganizations()).thenReturn(mockModel);
        when(coreRepository.getServiceCategories()).thenReturn(mockModel);

        this.mvc
                .perform(post("/v2/model/profile")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(dataModelDTO)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldInValidateLibrary() throws Exception {
        var mockModel = ModelFactory.createDefaultModel();
        var res = mockModel.createResource(ModelConstants.URN_UUID + RANDOM_ORG);
        res.addProperty(SKOS.notation, "P11");
        when(coreRepository.getOrganizations()).thenReturn(mockModel);
        when(coreRepository.getServiceCategories()).thenReturn(mockModel);

        DataModelDTO dataModelDTO = createDatamodelDTO(false);

        // code lists are not allowed in libraries
        dataModelDTO.setCodeLists(Set.of("http://uri.suomi.fi/codelist/test"));
        this.mvc
                .perform(post("/v2/model/library")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(dataModelDTO)))
                .andExpect(status().isBadRequest());
    }

    @ParameterizedTest
    @MethodSource("provideDataModelUpdateInvalidData")
    void shouldInValidateUpdate(DataModelDTO dataModelDTO) throws Exception {
        //Mock validation stuff
        var mockModel = ModelFactory.createDefaultModel();
        var res = mockModel.createResource(ModelConstants.URN_UUID + RANDOM_ORG);
        res.addProperty(SKOS.notation, "P11");
        when(coreRepository.getOrganizations()).thenReturn(mockModel);
        when(coreRepository.getServiceCategories()).thenReturn(mockModel);

        this.mvc
                .perform(put("/v2/model/library/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(dataModelDTO)))
                .andExpect(status().isBadRequest());
    }

    @ParameterizedTest
    @CsvSource({"http","test"})
    void shouldCheckFreePrefixWhenExists(String prefix) throws Exception {
        when(dataModelService.exists(anyString())).thenReturn(true);

        this.mvc
                .perform(get("/v2/model/{prefix}/exists", prefix)
                    .contentType("application/json"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("true")));
    }

    @Test
    void shouldCheckFreePrefixWhenNotExist() throws Exception {
        this.mvc
                .perform(get("/v2/model/xyz/exists")
                        .contentType("application/json"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("false")));
    }

    /**
     * Create Datamodel DTO for testing
     * @param updateModel true if model will be used for updating instead of creating
     * @return DataModelDTO
     */
    private static DataModelDTO createDatamodelDTO(boolean updateModel){
        DataModelDTO dataModelDTO = new DataModelDTO();
        dataModelDTO.setDescription(Map.of("fi", "test description"));
        dataModelDTO.setLabel(Map.of("fi", "test label"));
        dataModelDTO.setGroups(Set.of("P11"));
        dataModelDTO.setLanguages(Set.of("fi"));
        dataModelDTO.setOrganizations(Set.of(RANDOM_ORG));
        dataModelDTO.setInternalNamespaces(Set.of("http://uri.suomi.fi/datamodel/ns/test"));
        var extNs = new ExternalNamespaceDTO();
        extNs.setName("test external namespace");
        extNs.setPrefix("testprefix");
        extNs.setNamespace("http://example.com/ns/test");
        dataModelDTO.setExternalNamespaces(Set.of(extNs));
        if(!updateModel){
            dataModelDTO.setPrefix("test");
        }
        dataModelDTO.setStatus(Status.DRAFT);
        dataModelDTO.setTerminologies(Set.of("http://uri.suomi.fi/terminology/test"));
        return dataModelDTO;
    }

    private static Stream<Arguments> provideDataModelInvalidData() {
        var textAreaMaxPlus = ValidationConstants.TEXT_AREA_MAX_LENGTH + 20;
        var emailAreaMaxPlus = ValidationConstants.EMAIL_FIELD_MAX_LENGTH + 20;

        var args = new ArrayList<DataModelDTO>();

        var dataModelDTO = createDatamodelDTO(false);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setStatus(null);
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setPrefix("123");
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setPrefix("asd123asd1232asd123");
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setPrefix(null);
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setOrganizations(Collections.emptySet());
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setOrganizations(Set.of(UUID.randomUUID()));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setGroups(Collections.emptySet());
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setGroups(Set.of("Not real group"));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setLanguages(Set.of("Not real lang"));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setLabel(Map.of("Not real lang", "label"));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setLabel(Collections.emptyMap());
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setLabel(Map.of("fi", RandomStringUtils.random(textAreaMaxPlus)));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setDescription(Map.of("Not real lang", "desc"));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setDescription(Map.of("fi", RandomStringUtils.random(textAreaMaxPlus)));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setInternalNamespaces(Set.of("http://example.com/ns/test"));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        var invalidExtRes = new ExternalNamespaceDTO();
        invalidExtRes.setName(null); // no null name
        invalidExtRes.setPrefix("test");
        invalidExtRes.setNamespace("http:://example.com/ns/test");
        dataModelDTO.setExternalNamespaces(Set.of(invalidExtRes));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        invalidExtRes = new ExternalNamespaceDTO();
        invalidExtRes.setName("test name");
        invalidExtRes.setPrefix(null); //no null prefix
        invalidExtRes.setNamespace("http:://example.com/ns/test");
        dataModelDTO.setExternalNamespaces(Set.of(invalidExtRes));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        invalidExtRes = new ExternalNamespaceDTO();
        invalidExtRes.setName("test name");
        invalidExtRes.setPrefix("test");
        invalidExtRes.setNamespace(null); //no null namespace
        dataModelDTO.setExternalNamespaces(Set.of(invalidExtRes));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        invalidExtRes = new ExternalNamespaceDTO();
        invalidExtRes.setName("this is invalid");
        //Reserved namespace, see ValidationConstants.RESERVED_NAMESPACES
        invalidExtRes.setPrefix("dcterms");
        invalidExtRes.setNamespace("http:://example.com/ns/test");
        dataModelDTO.setExternalNamespaces(Set.of(invalidExtRes));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        invalidExtRes = new ExternalNamespaceDTO();
        invalidExtRes.setName("this is invalid");
        //Reserved word, see ValidationConstants.RESERVED_WORDS
        invalidExtRes.setPrefix("rootResource");
        invalidExtRes.setNamespace("http:://example.com/ns/test");
        dataModelDTO.setExternalNamespaces(Set.of(invalidExtRes));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        invalidExtRes = new ExternalNamespaceDTO();
        invalidExtRes.setName("this is invalid");
        //uri.suomi.fi cannot be set as external namespace
        invalidExtRes.setPrefix("test");
        invalidExtRes.setNamespace("http://uri.suomi.fi/datamodel/ns/test");
        dataModelDTO.setExternalNamespaces(Set.of(invalidExtRes));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setTerminologies(Set.of("http://invalid.url"));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setCodeLists(Set.of("http://invalid.url"));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setContact(RandomStringUtils.random(emailAreaMaxPlus));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setDocumentation(Map.of("en", RandomStringUtils.random(textAreaMaxPlus)));
        args.add(dataModelDTO);

        return args.stream().map(Arguments::of);
    }

    private static Stream<Arguments> provideDataModelUpdateInvalidData() {
        var args = new ArrayList<DataModelDTO>();

        //prefix should not be allowed
        var dataModelDTO = createDatamodelDTO(true);
        dataModelDTO.setPrefix("test");
        args.add(dataModelDTO);

        return args.stream().map(Arguments::of);
    }

    @Test
    void shouldDeleteDataModel() throws Exception {
        mvc.perform(delete("/v2/model/test/"))
                .andExpect(status().isOk());

        verify(dataModelService).delete(anyString());
        verifyNoMoreInteractions(dataModelService);
    }

}
