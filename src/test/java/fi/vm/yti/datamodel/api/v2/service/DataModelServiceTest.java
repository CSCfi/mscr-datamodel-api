package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.mapper.MapperTestUtils;
import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.DataModelDTO;
import fi.vm.yti.datamodel.api.v2.dto.ExternalNamespaceDTO;
import fi.vm.yti.datamodel.api.v2.dto.ModelType;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.endpoint.EndpointUtils;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.ModelMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexModel;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.security.AuthenticatedUserProvider;
import fi.vm.yti.security.YtiUser;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@Import({
        DataModelService.class
})
class DataModelServiceTest {

    @MockBean
    CoreRepository coreRepository;

    @MockBean
    AuthorizationManager authorizationManager;

    @MockBean
    GroupManagementService groupManagementService;

    @MockBean
    ModelMapper modelMapper;

    @MockBean
    TerminologyService terminologyService;

    @MockBean
    CodeListService codeListService;

    @MockBean
    OpenSearchIndexer openSearchIndexer;

    @MockBean
    AuthenticatedUserProvider userProvider;

    @Autowired
    DataModelService dataModelService;

    private static final UUID RANDOM_ORG = UUID.randomUUID();

    @Test
    void get() {
        when(coreRepository.fetch(anyString())).thenReturn(ModelFactory.createDefaultModel());

        dataModelService.get("test");

        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(modelMapper).mapToDataModelDTO(anyString(), any(Model.class), eq(null));
    }

    @Test
    void create() throws URISyntaxException {
        when(authorizationManager.hasRightToAnyOrganization(anyCollection())).thenReturn(true);
        when(userProvider.getUser()).thenReturn(EndpointUtils.mockUser);
        when(modelMapper.mapToJenaModel(any(DataModelDTO.class), any(ModelType.class), any(YtiUser.class))).thenReturn(ModelFactory.createDefaultModel());
        when(modelMapper.mapToIndexModel(anyString(), any(Model.class))).thenReturn(new IndexModel());
        var dto = createDatamodelDTO(false);
        var uri = dataModelService.create(dto, ModelType.LIBRARY);
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", uri.toString());

        verify(authorizationManager).hasRightToAnyOrganization(anyCollection());
        verify(terminologyService).resolveTerminology(anySet());
        verify(codeListService).resolveCodelistScheme(anySet());
        verify(modelMapper).mapToJenaModel(any(DataModelDTO.class), any(ModelType.class), any(YtiUser.class));
        verify(coreRepository).put(anyString(), any(Model.class));
        verify(modelMapper).mapToIndexModel(anyString(), any(Model.class));
        verify(openSearchIndexer).createModelToIndex(any(IndexModel.class));
    }

    @Test
    void update() {
        when(coreRepository.fetch(anyString())).thenReturn(ModelFactory.createDefaultModel());
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);
        when(userProvider.getUser()).thenReturn(EndpointUtils.mockUser);
        when(modelMapper.mapToUpdateJenaModel(anyString(), any(DataModelDTO.class), any(Model.class), any(YtiUser.class))).thenReturn(ModelFactory.createDefaultModel());
        when(modelMapper.mapToIndexModel(anyString(), any(Model.class))).thenReturn(new IndexModel());
        var dto = createDatamodelDTO(true);
        dataModelService.update("test", dto);


        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(terminologyService).resolveTerminology(anySet());
        verify(codeListService).resolveCodelistScheme(anySet());
        verify(modelMapper).mapToUpdateJenaModel(anyString(), any(DataModelDTO.class), any(Model.class), any(YtiUser.class));
        verify(coreRepository).put(anyString(), any(Model.class));
        verify(modelMapper).mapToIndexModel(anyString(), any(Model.class));
        verify(openSearchIndexer).updateModelToIndex(any(IndexModel.class));
    }

    @Test
    void delete() {
        when(coreRepository.graphExists(anyString())).thenReturn(true);
        when(coreRepository.fetch(anyString())).thenReturn(ModelFactory.createDefaultModel());
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);
        dataModelService.delete("test");

        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(coreRepository).delete(anyString());
        verify(openSearchIndexer).deleteModelFromIndex(anyString());
    }

    @Test
    void deleteNotExists() {
        assertThrows(ResourceNotFoundException.class, () -> dataModelService.delete("test"));
    }

    @Test
    void exists() {
        var response = dataModelService.exists("test");
        assertFalse(response);

        //reserved
        response = dataModelService.exists("http");
        assertTrue(response);

        //exists
        when(coreRepository.graphExists(anyString())).thenReturn(true);
        response = dataModelService.exists("test");
        assertTrue(response);
    }

    @Test
    void shouldExport() {
        var model = MapperTestUtils.getModelFromFile("/models/test_datamodel_library_with_resources.ttl");
        when(coreRepository.fetch(anyString())).thenReturn(model);
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);

        var response = dataModelService.export("test", null, "text/turtle");
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("test:TestClass"));
        assertTrue(response.getBody().contains("test:TestAttribute"));
        assertTrue(response.getBody().contains("test:TestAssociation"));

        //resource
        response = dataModelService.export("test", "TestClass", "text/turtle");
        assertNotNull(response.getBody());
        assertTrue(response.getBody().contains("test:TestClass"));
        assertFalse(response.getBody().contains("test:TestAttribute"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"application/ld+json", "text/turtle", "application/rdf+xml"})
    void shouldGetModelWithAcceptHeader(String accept) {
        when(coreRepository.fetch(anyString())).thenReturn(ModelFactory.createDefaultModel());

        var response = dataModelService.export("test", null, accept);
        assertTrue(response.getStatusCode().is2xxSuccessful());
        //TODO can this be fixed
    }

    @Test
    void shouldRemoveTriplesHiddenFromUnauthenticatedUser() {
        var model = MapperTestUtils.getModelFromFile("/models/test_datamodel_library_with_resources.ttl");
        when(coreRepository.fetch(anyString())).thenReturn(model);
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(false);
        var response = dataModelService.export("test", null, "text/turtle");
        assertTrue(response.getStatusCode().is2xxSuccessful());
        assertNotNull(response.getBody());
        assertFalse(response.getBody().contains("skos:editorialNote"));
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


}
