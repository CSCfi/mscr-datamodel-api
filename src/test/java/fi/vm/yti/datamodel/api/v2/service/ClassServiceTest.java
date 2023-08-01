package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.ClassDTO;
import fi.vm.yti.datamodel.api.v2.dto.DCAP;
import fi.vm.yti.datamodel.api.v2.dto.NodeShapeDTO;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.endpoint.EndpointUtils;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.ClassMapper;
import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexResource;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import fi.vm.yti.security.AuthenticatedUserProvider;
import fi.vm.yti.security.YtiUser;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.OWL;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Import({
        ClassService.class
})
class ClassServiceTest {

    @MockBean
    CoreRepository coreRepository;

    @MockBean
    AuthorizationManager authorizationManager;

    @MockBean
    ResourceService resourceService;

    @MockBean
    ImportsRepository importsRepository;

    @MockBean
    AuthenticatedUserProvider userProvider;

    @MockBean
    TerminologyService terminologyService;

    @MockBean
    GroupManagementService groupManagementService;

    @MockBean
    OpenSearchIndexer openSearchIndexer;

    @MockBean
    SearchIndexService searchIndexService;

    @Autowired
    ClassService classService;

    private static final UUID RANDOM_ORG = UUID.randomUUID();

    @Test
    void get() {
        when(coreRepository.resourceExistsInGraph(anyString(), anyString())).thenReturn(true);
        when(coreRepository.fetch(anyString())).thenReturn(ModelFactory.createDefaultModel());
        when(terminologyService.mapConcept()).thenReturn(mock(Consumer.class));

        try(var mapper = mockStatic(ClassMapper.class)) {
            classService.get("test", "TestClass");
        }

        verify(coreRepository).resourceExistsInGraph(anyString(), anyString());
        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(coreRepository).getOrganizations();
    }

    @Test
    void createClass() throws URISyntaxException {
        when(coreRepository.fetch(anyString())).thenReturn(ModelFactory.createDefaultModel());
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);
        when(userProvider.getUser()).thenReturn(EndpointUtils.mockUser);
        var dto = createClassDTO(false);
        try(var mapper = mockStatic(ClassMapper.class);
            var resMapper = mockStatic(ResourceMapper.class)) {
            resMapper.when(() -> ResourceMapper.mapToIndexResource(any(Model.class), anyString())).thenReturn(new IndexResource());
            var uri = classService.create("test", dto,false);
            assertEquals("http://uri.suomi.fi/datamodel/ns/test/Identifier", uri.toString());
            mapper.verify(() -> ClassMapper.createOntologyClassAndMapToModel(anyString(), any(Model.class), any(ClassDTO.class), any(YtiUser.class)));
        }

        verify(coreRepository).resourceExistsInGraph(anyString(), anyString());
        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(terminologyService).resolveConcept(anyString());
        verify(coreRepository).put(anyString(), any(Model.class));
        verify(openSearchIndexer).createResourceToIndex(any(IndexResource.class));
    }

    @Test
    void createNodeShape() throws URISyntaxException {
        when(coreRepository.fetch(anyString())).thenReturn(ModelFactory.createDefaultModel());
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);
        when(userProvider.getUser()).thenReturn(EndpointUtils.mockUser);
        var dto = createNodeShapeDTO(false);
        try(var mapper = mockStatic(ClassMapper.class);
            var resMapper = mockStatic(ResourceMapper.class)) {
            resMapper.when(() -> ResourceMapper.mapToIndexResource(any(Model.class), anyString())).thenReturn(new IndexResource());
            var uri = classService.create("test", dto,true);
            assertEquals("http://uri.suomi.fi/datamodel/ns/test/node-shape-1", uri.toString());
            mapper.verify(() -> ClassMapper.createNodeShapeAndMapToModel(anyString(), any(Model.class), any(NodeShapeDTO.class), any(YtiUser.class)));
        }

        verify(coreRepository).resourceExistsInGraph(anyString(), anyString());
        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(terminologyService).resolveConcept(anyString());
        verify(coreRepository).put(anyString(), any(Model.class));
        verify(openSearchIndexer).createResourceToIndex(any(IndexResource.class));
    }
    @Test
    void updateClass() {
        when(coreRepository.resourceExistsInGraph(anyString(), anyString())).thenReturn(true);
        when(coreRepository.fetch(anyString())).thenReturn(EndpointUtils.getMockModel(OWL.Ontology));
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);
        when(userProvider.getUser()).thenReturn(EndpointUtils.mockUser);
        var dto = createClassDTO(true);
        try(var mapper = mockStatic(ClassMapper.class);
            var resMapper = mockStatic(ResourceMapper.class)) {
            resMapper.when(() -> ResourceMapper.mapToIndexResource(any(Model.class), anyString())).thenReturn(new IndexResource());
            classService.update("test", "Identifier", dto);
            mapper.verify(() -> ClassMapper.mapToUpdateOntologyClass( any(Model.class), anyString(),any(Resource.class), any(ClassDTO.class), any(YtiUser.class)));
        }
        verify(coreRepository).resourceExistsInGraph(anyString(), anyString());
        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(terminologyService).resolveConcept(anyString());
        verify(coreRepository).put(anyString(), any(Model.class));
        verify(openSearchIndexer).updateResourceToIndex(any(IndexResource.class));
    }

    @Test
    void updateNodeShape() {
        when(coreRepository.resourceExistsInGraph(anyString(), anyString())).thenReturn(true);
        when(coreRepository.fetch(anyString())).thenReturn(EndpointUtils.getMockModel(DCAP.DCAP));
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);
        when(userProvider.getUser()).thenReturn(EndpointUtils.mockUser);
        var dto = createNodeShapeDTO(true);
        try(var mapper = mockStatic(ClassMapper.class);
            var resMapper = mockStatic(ResourceMapper.class)) {
            resMapper.when(() -> ResourceMapper.mapToIndexResource(any(Model.class), anyString())).thenReturn(new IndexResource());
            classService.update("test", "Identifier", dto);
            mapper.verify(() -> ClassMapper.mapToUpdateNodeShape( any(Model.class), anyString(),any(Resource.class), any(NodeShapeDTO.class), anySet(), any(YtiUser.class)));
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
        when(coreRepository.resourceExistsInGraph(anyString(), anyString())).thenReturn(true);
        when(coreRepository.fetch(anyString())).thenReturn(ModelFactory.createDefaultModel());
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);
        classService.delete("test", "Identifier");

        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(coreRepository).deleteResource(anyString());
        verify(openSearchIndexer).deleteResourceFromIndex(anyString());
    }

    @Test
    void deleteNotExists() {
        assertThrows(ResourceNotFoundException.class, () -> classService.delete("test", "Identifier"));
    }

    @Test
    void exists() {
        var response = classService.exists("test", "Identifier");
         assertFalse(response);

        //reserved
        response = classService.exists("test", "corner-wqe");
        assertTrue(response);

        //exists
        when(coreRepository.resourceExistsInGraph(anyString(), anyString())).thenReturn(true);
        response = classService.exists("test", "Identifier");
        assertTrue(response);
    }

    @Test
    void handlePropertyShapeReferencePut(){
        when(coreRepository.fetch(anyString())).thenReturn(ModelFactory.createDefaultModel());
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);
        try(var mapper = mockStatic(ClassMapper.class)) {
            classService.handlePropertyShapeReference("test", "node-shape-1", "http://uri.suomi.fi/datamodel/ns/test/ref-1", false);
            mapper.verify(() -> ClassMapper.mapAppendNodeShapeProperty( any(Resource.class), anyString(), anySet()));
        }

        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(coreRepository).put(anyString(), any(Model.class));
    }

    @Test
    void handlePropertyShapeReferenceDelete(){
        when(coreRepository.fetch(anyString())).thenReturn(ModelFactory.createDefaultModel());
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);
        try(var mapper = mockStatic(ClassMapper.class)) {
            classService.handlePropertyShapeReference("test", "node-shape-1", "http://uri.suomi.fi/datamodel/ns/test/ref-1", true);
            mapper.verify(() -> ClassMapper.mapRemoveNodeShapeProperty(any(Model.class), any(Resource.class), anyString(), anySet()));
        }

        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(coreRepository).put(anyString(), any(Model.class));
    }

    @Test
    void togglePropertyShape(){
        when(coreRepository.fetch(anyString())).thenReturn(ModelFactory.createDefaultModel());
        when(coreRepository.resourceExistsInGraph(anyString(), anyString())).thenReturn(true);
        when(authorizationManager.hasRightToModel(anyString(), any(Model.class))).thenReturn(true);
        try(var mapper = mockStatic(ClassMapper.class)) {
            classService.togglePropertyShape("test", "http://uri.suomi.fi/datamodel/ns/test/Uri");
            mapper.verify(() -> ClassMapper.toggleAndMapDeactivatedProperty( any(Model.class), anyString()));
        }

        verify(coreRepository).resourceExistsInGraph(anyString(), anyString());
        verify(coreRepository).fetch(anyString());
        verify(authorizationManager).hasRightToModel(anyString(), any(Model.class));
        verify(coreRepository).put(anyString(), any(Model.class));
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

    private static NodeShapeDTO createNodeShapeDTO(boolean update) {
        var dto = new NodeShapeDTO();
        dto.setLabel(Map.of("fi", "node label"));
        if(!update){
            dto.setIdentifier("node-shape-1");
        }
        dto.setStatus(Status.DRAFT);
        dto.setProperties(Set.of());
        dto.setSubject("http://uri.suomi.fi/terminology/concept-123");

        return dto;
    }

}
