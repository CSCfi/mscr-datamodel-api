package fi.vm.yti.datamodel.api.v2.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import fi.vm.yti.datamodel.api.v2.dto.CodeListDTO;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import fi.vm.yti.datamodel.api.v2.repository.SchemesRepository;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@Import({
        CodeListService.class
})
class CodeListServiceTest {

    @MockBean
    @Qualifier("uriResolveClient")
    private WebClient client;
    @MockBean
    private SchemesRepository schemesRepository;
    private final ObjectMapper mapper = new ObjectMapper();
    @Captor
    private ArgumentCaptor<String> stringCaptor;
    @Captor
    private ArgumentCaptor<Model> modelCaptor;
    @Autowired
    private CodeListService service;

    static final String GRAPH = "http://uri.suomi.fi/terminology/test/testcodelist";

    @Test
    void testResolveTerminology() throws IOException {
        var stream = getClass().getResourceAsStream("/codelist_response.json");
        assertNotNull(stream);
        var codelistData = mapper.readValue(stream, CodeListDTO.class);
        mockWebClient(codelistData);

        service.resolveCodelistScheme(Set.of(GRAPH));

        verify(schemesRepository).put(stringCaptor.capture(), modelCaptor.capture());
        var label = MapperUtils.localizedPropertyToMap(modelCaptor.getValue().getResource(GRAPH), RDFS.label);

        assertEquals(GRAPH, stringCaptor.getValue());
        assertEquals("testcodelist", label.get("fi"));
    }

    @Test
    void testTerminologyNotFound() {
        mockWebClient(null);
        service.resolveCodelistScheme(Set.of(GRAPH));
        verifyNoInteractions(schemesRepository);
    }

    private void mockWebClient(CodeListDTO result) {
        var req = mock(WebClient.RequestHeadersUriSpec.class);
        var res = mock(WebClient.ResponseSpec.class);
        var mono = mock(Mono.class);

        when(res.bodyToMono(any(ParameterizedTypeReference.class))).thenReturn(mono);
        when(mono.block()).thenReturn(result);

        when(client.get()).thenReturn(req);
        when(req.uri(any(Function.class))).thenReturn(req);
        when(req.accept(any(MediaType.class))).thenReturn(req);
        when(req.retrieve()).thenReturn(res);
    }
}
