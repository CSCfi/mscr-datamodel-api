package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexModel;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.ModelMapper;
import fi.vm.yti.datamodel.api.v2.service.GroupManagementService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.TerminologyService;
import fi.vm.yti.datamodel.api.v2.validator.ExceptionHandlerAdvice;
import fi.vm.yti.datamodel.api.v2.validator.ValidationConstants;
import fi.vm.yti.security.AuthenticatedUserProvider;
import fi.vm.yti.security.YtiUser;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.vocabulary.SKOS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@TestPropertySource(properties = {
        "spring.cloud.config.import-check.enabled=false",
        "defaultNamespace=http://custom.resolver.fi/datamodel/ns/"
})
@WebMvcTest(controllers =Datamodel.class)
@ActiveProfiles("junit")
class DatamodelTest {

	public static String defaultNamespace = "http://custom.resolver.fi/datamodel/ns/";
	
    @Autowired
    private MockMvc mvc;

    @MockBean
    private JenaService jenaService;

    @MockBean
    private AuthorizationManager authorizationManager;

    @MockBean
    private OpenSearchIndexer openSearchIndexer;

    @MockBean
    private ModelMapper modelMapper;

    @MockBean
    private TerminologyService terminologyService;

    @MockBean
    private GroupManagementService groupManagementService;

    @MockBean
    private AuthenticatedUserProvider userProvider;

    @Mock
    Consumer<ResourceInfoBaseDTO> consumer;

    @Autowired
    private Datamodel datamodel;

    private static final UUID RANDOM_ORG = UUID.randomUUID();
    private static final YtiUser USER = EndpointUtils.mockUser;

    @BeforeEach
    public void setup() {
    	this.mvc = MockMvcBuilders
                .standaloneSetup(this.datamodel)
                .setControllerAdvice(new ExceptionHandlerAdvice())
                .build();

        when(authorizationManager.hasRightToAnyOrganization(anyCollection())).thenReturn(true);
        when(authorizationManager.hasRightToModel(any(), any())).thenReturn(true);
        when(userProvider.getUser()).thenReturn(USER);
        ReflectionTestUtils.setField(modelMapper, "defaultNamespace", defaultNamespace);
        

    }

    @Test
    void shouldValidateAndCreate() throws Exception {
        var dataModelDTO = createDatamodelDTO(false);
        //Mock validation stuff
        var mockModel = ModelFactory.createDefaultModel();
        var res = mockModel.createResource(ModelConstants.URN_UUID + RANDOM_ORG);
        res.addProperty(SKOS.notation, "P11");
        when(jenaService.getOrganizations()).thenReturn(mockModel);
        when(jenaService.getServiceCategories()).thenReturn(mockModel);

        //Mock mapping
        var model = mock(Model.class);
        var indexmodel = mock(IndexModel.class);
        when(modelMapper.mapToJenaModel(any(DataModelDTO.class), any(YtiUser.class))).thenReturn(model);
        when(modelMapper.mapToIndexModel("test", model)).thenReturn(indexmodel);

        this.mvc
                .perform(put("/v2/model")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(dataModelDTO)))
                .andExpect(status().isOk());

        //Check that functions are called
        verify(this.modelMapper)
                .mapToJenaModel(any(DataModelDTO.class), any(YtiUser.class));
        verify(this.modelMapper)
                .mapToIndexModel(anyString(), any(Model.class));
        verifyNoMoreInteractions(this.modelMapper);
        verify(this.openSearchIndexer)
                .createModelToIndex(any(IndexModel.class));
        verifyNoMoreInteractions(this.openSearchIndexer);
    }

    @Test
    void shouldValidateAndUpdate() throws Exception {
        var dataModelDTO = createDatamodelDTO(true);

        //Mock validation stuff
        var mockModel = ModelFactory.createDefaultModel();
        var res = mockModel.createResource(ModelConstants.URN_UUID + RANDOM_ORG);
        res.addProperty(SKOS.notation, "P11");
        when(jenaService.getOrganizations()).thenReturn(mockModel);
        when(jenaService.getServiceCategories()).thenReturn(mockModel);

        //Mock mapping
        var model = mock(Model.class);
        var indexmodel = mock(IndexModel.class);
        when(jenaService.getDataModel(anyString())).thenReturn(mock(Model.class));
        when(modelMapper.mapToUpdateJenaModel(anyString(), any(DataModelDTO.class), any(Model.class), any(YtiUser.class))).thenReturn(model);
        when(modelMapper.mapToIndexModel("test", model)).thenReturn(indexmodel);

        this.mvc
                .perform(post("/v2/model/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(dataModelDTO)))
                .andExpect(status().isOk());

        //Check that functions are called
        verify(this.modelMapper)
                .mapToUpdateJenaModel(anyString(), any(DataModelDTO.class), any(Model.class), any(YtiUser.class));
        verify(this.modelMapper)
                .mapToIndexModel(anyString(), any(Model.class));
        verifyNoMoreInteractions(this.modelMapper);
        verify(this.openSearchIndexer)
                .updateModelToIndex(any(IndexModel.class));
        verifyNoMoreInteractions(this.openSearchIndexer);
    }

    @Test
    void shouldReturnModel() throws Exception {
        var mockModel = mock(Model.class);
        when(groupManagementService.mapUser()).thenReturn(consumer);
        when(jenaService.getDataModel(anyString())).thenReturn(mockModel);
        when(modelMapper.mapToDataModelDTO(anyString(), any(Model.class), eq(consumer))).thenReturn(new DataModelInfoDTO());

        this.mvc
                .perform(get("/v2/model/test")
                        .accept(MediaType.APPLICATION_JSON_VALUE)
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE));

        //verify(this.jenaService)
        //        .getDataModel(ModelConstants.DEFAULT_NAMESPACE + "test");
        //verifyNoMoreInteractions(this.jenaService);
        verify(modelMapper)
                .mapToDataModelDTO(eq("test"), any(Model.class), eq(consumer));
        verifyNoMoreInteractions(this.modelMapper);
    }

    @Test
    void shouldReturnNotFound() throws Exception {
        when(jenaService.getDataModel(anyString())).thenThrow(ResourceNotFoundException.class);

        this.mvc
                .perform(get("/v2/model/not-found")
                        .accept(MediaType.APPLICATION_JSON_VALUE)
                        .contentType(MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().isNotFound())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE));
    }

    @ParameterizedTest
    @MethodSource("provideDataModelInvalidData")
    void shouldInValidate(DataModelDTO dataModelDTO) throws Exception {
        //Mock validation stuff
        var mockModel = ModelFactory.createDefaultModel();
        var res = mockModel.createResource(ModelConstants.URN_UUID + RANDOM_ORG);
        res.addProperty(SKOS.notation, "P11");
        when(jenaService.getOrganizations()).thenReturn(mockModel);
        when(jenaService.getServiceCategories()).thenReturn(mockModel);

        this.mvc
                .perform(put("/v2/model")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(dataModelDTO)))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldInvalidateSamePrefix() throws Exception {
        //Mock validation stuff
        var mockModel = ModelFactory.createDefaultModel();
        var res = mockModel.createResource(ModelConstants.URN_UUID + RANDOM_ORG);
        res.addProperty(SKOS.notation, "P11");
        when(jenaService.getOrganizations()).thenReturn(mockModel);
        when(jenaService.getServiceCategories()).thenReturn(mockModel);

        this.mvc
                .perform(put("/v2/model")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(createDatamodelDTO(false))))
                .andExpect(status().isOk());

        when(jenaService.doesDataModelExist(anyString())).thenReturn(true);

        this.mvc
                .perform(put("/v2/model")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(createDatamodelDTO(false))))
                .andExpect(status().isBadRequest())
                .andExpect(content().string(containsString("prefix-in-use")));
    }

    @ParameterizedTest
    @MethodSource("provideDataModelUpdateInvalidData")
    void shouldInValidateUpdate(DataModelDTO dataModelDTO) throws Exception {
        //Mock validation stuff
        var mockModel = ModelFactory.createDefaultModel();
        var res = mockModel.createResource(ModelConstants.URN_UUID + RANDOM_ORG);
        res.addProperty(SKOS.notation, "P11");
        when(jenaService.getOrganizations()).thenReturn(mockModel);
        when(jenaService.getServiceCategories()).thenReturn(mockModel);

        this.mvc
                .perform(post("/v2/model/test")
                        .contentType("application/json")
                        .content(EndpointUtils.convertObjectToJsonString(dataModelDTO)))
                .andExpect(status().isBadRequest());
    }

    @ParameterizedTest
    @CsvSource({"http","test"})
    void shouldCheckFreePrefixWhenExists(String prefix) throws Exception {
        when(jenaService.doesDataModelExist(defaultNamespace + prefix)).thenReturn(true);

        this.mvc
                .perform(get("/v2/model/freePrefix/" + prefix)
                    .contentType("application/json"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("false")));
    }

    @Test
    void shouldCheckFreePrefixWhenNotExist() throws Exception {
        when(jenaService.doesDataModelExist(anyString())).thenReturn(false);

        this.mvc
                .perform(get("/v2/model/freePrefix/xyz")
                        .contentType("application/json"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("true")));
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
        dataModelDTO.setInternalNamespaces(Set.of(defaultNamespace + "test"));
        var extNs = new ExternalNamespaceDTO();
        extNs.setName("test external namespace");
        extNs.setPrefix("testprefix");
        extNs.setNamespace("http://example.com/ns/test");
        dataModelDTO.setExternalNamespaces(Set.of(extNs));
        if(!updateModel){
            dataModelDTO.setPrefix("test");
            dataModelDTO.setType(ModelType.LIBRARY);
        }
        dataModelDTO.setStatus(Status.DRAFT);
        dataModelDTO.setTerminologies(Set.of("http://uri.suomi.fi/terminology/test"));
        dataModelDTO.setContact("test@test.com");
        return dataModelDTO;
    }

    private static Stream<Arguments> provideDataModelInvalidData() {
        var textAreaMaxPlus = ValidationConstants.TEXT_AREA_MAX_LENGTH + 20;
        var emailAreaMaxPlus = ValidationConstants.EMAIL_FIELD_MAX_LENGTH + 20;

        var args = new ArrayList<DataModelDTO>();

        var dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setType(null);
        args.add(dataModelDTO);

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
        invalidExtRes.setNamespace(defaultNamespace + "test");
        dataModelDTO.setExternalNamespaces(Set.of(invalidExtRes));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setTerminologies(Set.of("http://invalid.url"));
        args.add(dataModelDTO);

        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setContact(RandomStringUtils.random(emailAreaMaxPlus));
        args.add(dataModelDTO);

        return args.stream().map(Arguments::of);
    }

    private static Stream<Arguments> provideDataModelUpdateInvalidData() {
        var args = new ArrayList<DataModelDTO>();

        //prefix should not be allowed
        var dataModelDTO = createDatamodelDTO(true);
        dataModelDTO.setPrefix("test");
        args.add(dataModelDTO);

        //changing type should not be allowed
        dataModelDTO = createDatamodelDTO(false);
        dataModelDTO.setType(ModelType.PROFILE);
        args.add(dataModelDTO);

        return args.stream().map(Arguments::of);
    }

    @Test
    void shouldDeleteDataModel() throws Exception {
        when(jenaService.doesDataModelExist(anyString())).thenReturn(true);
        when(jenaService.getDataModel(anyString())).thenReturn(mock(Model.class));
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);


        mvc.perform(delete("/v2/model/test/"))
                .andExpect(status().isOk());

        verify(jenaService).doesDataModelExist(anyString());
        verify(jenaService).getDataModel(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(jenaService).deleteDataModel(anyString());
        verify(openSearchIndexer).deleteModelFromIndex(anyString());
    }

    @Test
    void shouldFailToFindDataModelDelete() throws Exception {
        mvc.perform(delete("/v2/model/test"))
                .andExpect(status().isNotFound());

        verify(jenaService).doesDataModelExist(anyString());
        verifyNoMoreInteractions(jenaService);
        verifyNoInteractions(authorizationManager, openSearchIndexer);
    }

    @Test
    void shouldFailAuthorisationDelete() throws Exception {
        when(jenaService.doesDataModelExist(anyString())).thenReturn(true);
        when(jenaService.getDataModel(anyString())).thenReturn(mock(Model.class));
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(false);

        mvc.perform(delete("/v2/model/test"))
                .andExpect(status().isUnauthorized());

        verify(jenaService).doesDataModelExist(anyString());
        verify(jenaService).getDataModel(anyString());
        verifyNoMoreInteractions(jenaService);
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verifyNoInteractions(openSearchIndexer);
    }

}
