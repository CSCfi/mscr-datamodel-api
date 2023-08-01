package fi.vm.yti.datamodel.api.v2.opensearch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.datamodel.api.index.OpenSearchConnector;
import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.mapper.CrosswalkMapper;
import fi.vm.yti.datamodel.api.v2.mapper.ModelMapper;
import fi.vm.yti.datamodel.api.v2.mapper.SchemaMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import fi.vm.yti.datamodel.api.v2.service.JenaService;

@ExtendWith(SpringExtension.class)
@Import({
        OpenSearchIndexer.class,
        JenaService.class
})
class OpenSearchIndexerTest {

    @MockBean
    CoreRepository coreRepository;

    @MockBean
    ImportsRepository importsRepository;

    @MockBean
    OpenSearchConnector openSearchConnector;

    @MockBean
    ObjectMapper objectMapper;

    @MockBean
    ModelMapper modelMapper;
    
    @MockBean
    SchemaMapper schemaMapper;
    
    @MockBean
    CrosswalkMapper crosswalkMapper;

    @MockBean
    AuthorizationManager authorizationManager;

    @MockBean
    OpenSearchClient client;

    @Autowired
    OpenSearchIndexer openSearchIndexer;


    @Test
    void initModelIndexTest() {
        var model = mock(Model.class);
        var subjects = mock(ResIterator.class);
        when(coreRepository.queryConstruct(any(Query.class))).thenReturn(model);
        when(model.listSubjects()).thenReturn(subjects);
        when(objectMapper.valueToTree(any())).thenReturn(mock(JsonNode.class));

        openSearchIndexer.initModelIndex();

        verify(coreRepository).queryConstruct(any(Query.class));
    }

    @Test
    void initResourceIndexTest() {
        var model = mock(Model.class);
        var subjects = mock(ResIterator.class);
        when(coreRepository.queryConstruct(any(Query.class))).thenReturn(model);
        when(model.listSubjects()).thenReturn(subjects);
        when(objectMapper.valueToTree(any())).thenReturn(mock(JsonNode.class));

        openSearchIndexer.initResourceIndex();

        verify(coreRepository).queryConstruct(any(Query.class));
    }
}
