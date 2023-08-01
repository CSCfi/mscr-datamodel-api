package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.endpoint.EndpointUtils;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexResource;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import fi.vm.yti.security.AuthenticatedUserProvider;
import fi.vm.yti.security.YtiUser;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Import({
        ResourceService.class
})
class ResourceServiceTest {

    @MockBean
    CoreRepository coreRepository;
    @MockBean
    ImportsRepository importsRepository;
    @MockBean
    AuthorizationManager authorizationManager;
    @MockBean
    GroupManagementService groupManagementService;
    @MockBean
    TerminologyService terminologyService;
    @MockBean
    AuthenticatedUserProvider userProvider;
    @MockBean
    OpenSearchIndexer openSearchIndexer;

    @Autowired
    ResourceService resourceService;

    private static final YtiUser USER = EndpointUtils.mockUser;
    private final Consumer<ResourceCommonDTO> userMapper = (var dto) -> {};
    private final Consumer<ResourceInfoBaseDTO> conceptMapper = (var dto) -> {};
    @BeforeEach
    public void setup() {
        when(authorizationManager.hasRightToAnyOrganization(anyCollection())).thenReturn(true);
        when(authorizationManager.hasRightToModel(any(), any())).thenReturn(true);
        when(userProvider.getUser()).thenReturn(USER);
        when(groupManagementService.mapUser()).thenReturn(userMapper);
        when(terminologyService.mapConcept()).thenReturn(conceptMapper);
    }

    @Test
    void get() {
        when(coreRepository.resourceExistsInGraph(anyString(), anyString())).thenReturn(true);
        when(coreRepository.fetch(anyString())).thenReturn(EndpointUtils.getMockModel(OWL.Ontology));
        when(terminologyService.mapConcept()).thenReturn(mock(Consumer.class));

        try(var mapper = mockStatic(ResourceMapper.class)) {
            resourceService.get("test", "TestClass");
        }

        verify(coreRepository).resourceExistsInGraph(anyString(), anyString());
        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(coreRepository).getOrganizations();
    }

    @ParameterizedTest
    @EnumSource(value = ResourceType.class, names = {"ATTRIBUTE", "ASSOCIATION"})
    void create(ResourceType resourceType) throws URISyntaxException {
        var model = EndpointUtils.getMockModel(OWL.Ontology);
        when(coreRepository.fetch(anyString())).thenReturn(model);

        var dto = createResourceDTO(false, resourceType);
        try(var mapper = mockStatic(ResourceMapper.class)) {
            mapper.when(() -> ResourceMapper.mapToResource(anyString(), any(Model.class), any(ResourceDTO.class), any(ResourceType.class), any(YtiUser.class)))
                            .thenReturn("http://uri.suomi.fi/datamodel/ns/test/Identifier");
            mapper.when(() -> ResourceMapper.mapToIndexResource(any(Model.class), anyString())).thenReturn(new IndexResource());
            var uri = resourceService.create("test", dto, resourceType, false);
            assertEquals("http://uri.suomi.fi/datamodel/ns/test/Identifier", uri.toString());
        }

        verify(coreRepository).resourceExistsInGraph(anyString(), anyString());
        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(terminologyService).resolveConcept(anyString());
        verify(terminologyService).resolveConcept(anyString());
        verify(coreRepository).put(anyString(), any(Model.class));
        verify(openSearchIndexer).createResourceToIndex(any(IndexResource.class));
    }

    @Test
    void createPropertyShape() throws URISyntaxException {
        var model = EndpointUtils.getMockModel(DCAP.DCAP);
        when(coreRepository.fetch(anyString())).thenReturn(model);

        var dto = createPropertyShapeDTO(true, ResourceType.ASSOCIATION);
        try(var mapper = mockStatic(ResourceMapper.class)) {
            mapper.when(() -> ResourceMapper.mapToPropertyShapeResource(anyString(), any(Model.class), any(PropertyShapeDTO.class), any(YtiUser.class)))
                    .thenReturn("http://uri.suomi.fi/datamodel/ns/test/Identifier");
            mapper.when(() -> ResourceMapper.mapToIndexResource(any(Model.class), anyString())).thenReturn(new IndexResource());
            var uri = resourceService.create("test", dto, null, true);
            assertEquals("http://uri.suomi.fi/datamodel/ns/test/Identifier", uri.toString());
        }

        verify(coreRepository).resourceExistsInGraph(anyString(), anyString());
        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(terminologyService).resolveConcept(anyString());
        verify(terminologyService).resolveConcept(anyString());
        verify(coreRepository).put(anyString(), any(Model.class));
        verify(openSearchIndexer).createResourceToIndex(any(IndexResource.class));
    }

    @Test
    void updateResource() {
        when(coreRepository.resourceExistsInGraph(anyString(), anyString())).thenReturn(true);
        when(coreRepository.fetch(anyString())).thenReturn(EndpointUtils.getMockModel(OWL.Ontology));
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);
        when(userProvider.getUser()).thenReturn(EndpointUtils.mockUser);
        var dto = createResourceDTO(true, ResourceType.ASSOCIATION);
        try(var mapper = mockStatic(ResourceMapper.class)) {
            mapper.when(() -> ResourceMapper.mapToIndexResource(any(Model.class), anyString())).thenReturn(new IndexResource());
            resourceService.update("test", "Identifier", dto);
            mapper.verify(() -> ResourceMapper.mapToUpdateResource(anyString(), any(Model.class), anyString(), any(ResourceDTO.class), any(YtiUser.class)));
        }
        verify(coreRepository).resourceExistsInGraph(anyString(), anyString());
        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(terminologyService).resolveConcept(anyString());
        verify(coreRepository).put(anyString(), any(Model.class));
        verify(openSearchIndexer).updateResourceToIndex(any(IndexResource.class));
    }

    @Test
    void updatePropertyShape() {
        when(coreRepository.resourceExistsInGraph(anyString(), anyString())).thenReturn(true);
        when(coreRepository.fetch(anyString())).thenReturn(EndpointUtils.getMockModel(DCAP.DCAP));
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);
        when(userProvider.getUser()).thenReturn(EndpointUtils.mockUser);
        var dto = createPropertyShapeDTO(true, ResourceType.ASSOCIATION);
        try(var mapper = mockStatic(ResourceMapper.class)) {
            mapper.when(() -> ResourceMapper.mapToIndexResource(any(Model.class), anyString())).thenReturn(new IndexResource());
            resourceService.update("test", "Identifier", dto);
            mapper.verify(() -> ResourceMapper.mapToUpdatePropertyShape(anyString(), any(Model.class), anyString(), any(PropertyShapeDTO.class), any(YtiUser.class)));
        }
        verify(coreRepository).resourceExistsInGraph(anyString(), anyString());
        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(terminologyService).resolveConcept(anyString());
        verify(coreRepository).put(anyString(), any(Model.class));
        verify(openSearchIndexer).updateResourceToIndex(any(IndexResource.class));
    }

    @Test
    void delete() {
        //Fail because doesnt exists
        assertThrows(ResourceNotFoundException.class, () -> resourceService.delete("test", "Identifier"));

        //delete
        when(coreRepository.resourceExistsInGraph(anyString(), anyString())).thenReturn(true);
        when(coreRepository.fetch(anyString())).thenReturn(ModelFactory.createDefaultModel());
        resourceService.delete("test", "identifier");

        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(coreRepository).deleteResource(anyString());
        verify(openSearchIndexer).deleteResourceFromIndex(anyString());
    }


    @Test
    void copyPropertyShape() throws URISyntaxException {
        when(coreRepository.resourceExistsInGraph("http://uri.suomi.fi/datamodel/ns/test", "http://uri.suomi.fi/datamodel/ns/test/Identifier")).thenReturn(true);
        var m = ModelFactory.createDefaultModel();
        m.createResource("http://uri.suomi.fi/datamodel/ns/test")
                .addProperty(RDF.type, OWL.Ontology)
                .addProperty(RDF.type, Iow.ApplicationProfile);
        m.createResource("http://uri.suomi.fi/datamodel/ns/newTest")
                .addProperty(RDF.type, OWL.Ontology)
                .addProperty(RDF.type, Iow.ApplicationProfile);
        when(coreRepository.fetch(anyString())).thenReturn(m);
        try(var mapper = mockStatic(ResourceMapper.class)) {
            mapper.when(() -> ResourceMapper.mapToIndexResource(any(Model.class), anyString())).thenReturn(new IndexResource());
            var uri = resourceService.copyPropertyShape("test", "Identifier", "newTest", "newId");
            assertEquals("http://uri.suomi.fi/datamodel/ns/newTest/newId", uri.toString());
        }
        verify(coreRepository, times(2)).resourceExistsInGraph(anyString(), anyString());
        verify(coreRepository, times(2)).fetch(anyString());
        verify(authorizationManager, times(2)).hasRightToModel(anyString(), any(Model.class));
    }

    @Test
    void failCopyPropertyShapeNotExists() {
        assertThrows(ResourceNotFoundException.class, () -> resourceService.copyPropertyShape("test", "Identifier", "newTest", "newId"));
    }

    @Test
    void failCopyPropertyShapeIdInUse() {
        when(coreRepository.resourceExistsInGraph(anyString(), anyString())).thenReturn(true);

        var error = assertThrows(MappingError.class, () ->resourceService.copyPropertyShape("test", "Identifier", "newTest", "newId"));
        assertEquals("Error during mapping: Identifier in use", error.getMessage());

    }

    @Test
    void failCopyPropertyShapeModelType() {
        //Only one is application profile
        when(coreRepository.resourceExistsInGraph("http://uri.suomi.fi/datamodel/ns/test", "http://uri.suomi.fi/datamodel/ns/test/Identifier")).thenReturn(true);
        var m = ModelFactory.createDefaultModel();
        m.createResource("http://uri.suomi.fi/datamodel/ns/test")
                .addProperty(RDF.type, OWL.Ontology);
        m.createResource("http://uri.suomi.fi/datamodel/ns/newTest")
                .addProperty(RDF.type, OWL.Ontology)
                .addProperty(RDF.type, Iow.ApplicationProfile);
        when(coreRepository.fetch(anyString())).thenReturn(m);
        var error = assertThrows(MappingError.class, () ->resourceService.copyPropertyShape("test", "Identifier", "newTest", "newId"));
        assertEquals("Error during mapping: Both data models have to be application profiles", error.getMessage());

        //Both are libraries
        m = ModelFactory.createDefaultModel();
        m.createResource("http://uri.suomi.fi/datamodel/ns/test")
                .addProperty(RDF.type, OWL.Ontology);
        m.createResource("http://uri.suomi.fi/datamodel/ns/newTest")
                .addProperty(RDF.type, OWL.Ontology);
        when(coreRepository.fetch(anyString())).thenReturn(m);
        error = assertThrows(MappingError.class, () ->resourceService.copyPropertyShape("test", "Identifier", "newTest", "newId"));
        assertEquals("Error during mapping: Both data models have to be application profiles", error.getMessage());

    }

    @Test
    void checkExists(){
        var result = resourceService.exists("test", "corner-qwe");
        assertTrue(result);
        result = resourceService.exists("test", "Free");
        assertFalse(result);
        when(coreRepository.resourceExistsInGraph(anyString(), anyString())).thenReturn(true);
        result = resourceService.exists("test", "NotFree");
        assertTrue(result);
    }
    private static ResourceDTO createResourceDTO(boolean update, ResourceType resourceType){
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
        if(resourceType.equals(ResourceType.ASSOCIATION)){
            dto.setRange("http://uri.suomi.fi/datamodel/ns/int/FakeClass");
        }else{
            dto.setRange("owl:real");
        }
        dto.setNote(Map.of("fi", "test note"));
        return dto;
    }

    private static PropertyShapeDTO createPropertyShapeDTO(boolean update, ResourceType resourceType) {
        var dto = new PropertyShapeDTO();
        dto.setLabel(Map.of("fi", "test label"));
        if(!update) {
            dto.setIdentifier("Identifier");
            dto.setType(resourceType);
        }
        dto.setStatus(Status.DRAFT);
        dto.setSubject("sanastot.suomi.fi/notrealurl");
        dto.setPath("http://uri.suomi.fi/datamodel/ns/int/FakeClass");
        dto.setClassType("http://uri.suomi.fi/datamodel/ns/int/FakeClass");
        return dto;
    }

}
