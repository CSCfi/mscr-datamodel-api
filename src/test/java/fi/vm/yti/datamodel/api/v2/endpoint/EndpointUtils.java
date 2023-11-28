package fi.vm.yti.datamodel.api.v2.endpoint;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import fi.vm.yti.security.Role;
import fi.vm.yti.security.YtiUser;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class EndpointUtils {

    public static String convertObjectToJsonString(Object obj) throws JsonProcessingException {
        var mapper = new ObjectMapper();
        return mapper.writeValueAsString(obj);
    }

    public static final YtiUser mockUser = new YtiUser("test@localhost",
            "test",
            "tester",
            UUID.randomUUID(),
            true,
            false,
            LocalDateTime.of(2001, 1, 1, 0,0),
            LocalDateTime.of(2001, 1, 1, 0,0),
            new HashMap<>(Map.of(UUID.randomUUID(), Set.of(Role.ADMIN))),
            "",
            "");

    public static Model getMockModel(Resource type) {
        var mockModel = ModelFactory.createDefaultModel();
        mockModel.createResource("http://uri.suomi.fi/datamodel/ns/test")
                .addProperty(RDF.type, type);
        return mockModel;
    }
}
