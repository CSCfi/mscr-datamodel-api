package fi.vm.yti.datamodel.api.mapper;

import fi.vm.yti.datamodel.api.v2.dto.GroupManagementOrganizationDTO;
import fi.vm.yti.datamodel.api.v2.dto.Iow;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import fi.vm.yti.datamodel.api.v2.mapper.OrganizationMapper;

import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.SKOS;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static fi.vm.yti.datamodel.api.v2.dto.ModelConstants.URN_UUID;

class OrganizationMapperTest {

    @Test
    void testMapOrganizationsToDTO() {
        var model = MapperTestUtils.getModelFromFile("/organizations.ttl");
        var organizations = OrganizationMapper.mapToListOrganizationDTO(model);

        assertEquals(3, organizations.size());

        var org = organizations.stream()
                .filter(o -> o.getId().equals("7d3a3c00-5a6b-489b-a3ed-63bb58c26a63"))
                .findFirst()
                .orElseThrow();
        var child = organizations.stream()
                .filter(o -> o.getId().equals("8fab2816-03c5-48cd-9d48-b61048f435da"))
                .findFirst()
                .orElseThrow();

        assertEquals("Yhteentoimivuusalustan yllapito", org.getLabel().get("fi"));
        assertEquals("Utvecklare av interoperabilitetsplattform", org.getLabel().get("sv"));
        assertEquals("Interoperability platform developers", org.getLabel().get("en"));

        assertEquals(UUID.fromString("74776e94-7f51-48dc-aeec-c084c4defa09"), child.getParentOrganization());
    }

    @Test
    void testMapGroupManagementOrganizationToModel() {
        var dto = new GroupManagementOrganizationDTO();
        var uuid = UUID.randomUUID().toString();
        var parentId = UUID.randomUUID().toString();

        dto.setUuid(uuid);
        dto.setPrefLabel(Map.of(
                "fi", "Organisaatio",
                "en", "Organization")
        );
        dto.setDescription(Map.of("en", "Test"));
        dto.setUrl("https://dvv.fi");
        dto.setParentId(parentId);

        var model = ModelFactory.createDefaultModel();
        OrganizationMapper.mapOrganizationsToModel(List.of(dto), model);
        var resource = model.getResource(URN_UUID + uuid);
        var label = MapperUtils.localizedPropertyToMap(resource, SKOS.prefLabel);
        var description = MapperUtils.localizedPropertyToMap(resource, DCTerms.description);

        assertEquals(FOAF.Organization, resource.getProperty(RDF.type).getObject().asResource());
        assertEquals("Organisaatio", label.get("fi"));
        assertEquals("Organization", label.get("en"));
        assertEquals("Test", description.get("en"));
        assertEquals("https://dvv.fi", resource.getProperty(FOAF.homepage).getObject().toString());
        assertEquals(URN_UUID + parentId, resource.getProperty(Iow.parentOrganization).getObject().toString());
    }
}
