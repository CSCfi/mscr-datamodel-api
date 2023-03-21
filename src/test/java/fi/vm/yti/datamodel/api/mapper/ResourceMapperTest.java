package fi.vm.yti.datamodel.api.mapper;

import fi.vm.yti.datamodel.api.v2.dto.ResourceDTO;
import fi.vm.yti.datamodel.api.v2.dto.ResourceType;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@Import({
        ResourceMapper.class
})
class ResourceMapperTest {

    @MockBean
    JenaService jenaService;

    @Autowired
    ResourceMapper mapper;
    @Test
    void mapToResourceAssociation() {
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/test_datamodel.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var dto = new ResourceDTO();
        dto.setIdentifier("Resource");
        dto.setType(ResourceType.ASSOCIATION);
        dto.setSubject("http://uri.suomi.fi/terminology/test/test1");
        dto.setEquivalentResource(Set.of("http://uri.suomi.fi/datamodel/ns/int#EqRes"));
        dto.setSubResourceOf(Set.of("https://www.example.com/ns/ext#SubRes"));
        dto.setEditorialNote("comment");
        dto.setLabel(Map.of("fi", "test label"));
        dto.setStatus(Status.DRAFT);
        dto.setNote(Map.of("fi", "test note"));

        mapper.mapToResource("http://uri.suomi.fi/datamodel/ns/test", m, dto);

        Resource modelResource = m.getResource("http://uri.suomi.fi/datamodel/ns/test");
        Resource resourceResource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#Resource");

        assertNotNull(modelResource);
        assertNotNull(resourceResource);

        assertEquals(1, modelResource.listProperties(DCTerms.hasPart).toList().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#Resource", modelResource.getProperty(DCTerms.hasPart).getString());

        assertEquals(OWL.ObjectProperty, resourceResource.getProperty(RDF.type).getResource());

        assertEquals(1, resourceResource.listProperties(RDFS.label).toList().size());
        assertEquals("test label", resourceResource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resourceResource.getProperty(RDFS.label).getLiteral().getLanguage());

        assertEquals(Status.DRAFT, Status.valueOf(resourceResource.getProperty(OWL.versionInfo).getObject().toString()));
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resourceResource.getProperty(RDFS.isDefinedBy).getObject().toString());

        assertEquals(XSDDatatype.XSDNCName, resourceResource.getProperty(DCTerms.identifier).getLiteral().getDatatype());
        assertEquals("Resource", resourceResource.getProperty(DCTerms.identifier).getLiteral().getString());

        assertEquals("comment", resourceResource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals("test note", resourceResource.getProperty(SKOS.note).getLiteral().getString());

        assertEquals(1, resourceResource.listProperties(RDFS.subPropertyOf).toList().size());
        assertEquals("https://www.example.com/ns/ext#SubRes", resourceResource.getProperty(RDFS.subPropertyOf).getObject().toString());
        assertEquals(1, resourceResource.listProperties(OWL.equivalentProperty).toList().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/int#EqRes", resourceResource.getProperty(OWL.equivalentProperty).getObject().toString());
    }

    @Test
    void mapToResourceAssociationEmptySubResourceOf() {
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/test_datamodel.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var dto = new ResourceDTO();
        dto.setIdentifier("Resource");
        dto.setType(ResourceType.ASSOCIATION);
        dto.setSubject("http://uri.suomi.fi/terminology/test/test1");
        dto.setEquivalentResource(Set.of("http://uri.suomi.fi/datamodel/ns/int#EqRes"));
        dto.setEditorialNote("comment");
        dto.setLabel(Map.of("fi", "test label"));
        dto.setStatus(Status.DRAFT);
        dto.setNote(Map.of("fi", "test note"));

        mapper.mapToResource("http://uri.suomi.fi/datamodel/ns/test", m, dto);

        Resource modelResource = m.getResource("http://uri.suomi.fi/datamodel/ns/test");
        Resource resourceResource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#Resource");

        assertNotNull(modelResource);
        assertNotNull(resourceResource);

        assertEquals(1, modelResource.listProperties(DCTerms.hasPart).toList().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#Resource", modelResource.getProperty(DCTerms.hasPart).getString());

        assertEquals(OWL.ObjectProperty, resourceResource.getProperty(RDF.type).getResource());

        assertEquals(1, resourceResource.listProperties(RDFS.label).toList().size());
        assertEquals("test label", resourceResource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resourceResource.getProperty(RDFS.label).getLiteral().getLanguage());

        assertEquals(Status.DRAFT, Status.valueOf(resourceResource.getProperty(OWL.versionInfo).getObject().toString()));
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resourceResource.getProperty(RDFS.isDefinedBy).getObject().toString());

        assertEquals(XSDDatatype.XSDNCName, resourceResource.getProperty(DCTerms.identifier).getLiteral().getDatatype());
        assertEquals("Resource", resourceResource.getProperty(DCTerms.identifier).getLiteral().getString());

        assertEquals("comment", resourceResource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals("test note", resourceResource.getProperty(SKOS.note).getLiteral().getString());

        assertEquals(1, resourceResource.listProperties(RDFS.subPropertyOf).toList().size());
        assertEquals(OWL2.topObjectProperty, resourceResource.getProperty(RDFS.subPropertyOf).getResource());
        assertEquals(1, resourceResource.listProperties(OWL.equivalentProperty).toList().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/int#EqRes", resourceResource.getProperty(OWL.equivalentProperty).getObject().toString());
    }

    @Test
    void mapToResourceAttributeEmptySubResourceOf() {
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/test_datamodel.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var dto = new ResourceDTO();
        dto.setIdentifier("Resource");
        dto.setType(ResourceType.ATTRIBUTE);
        dto.setSubject("http://uri.suomi.fi/terminology/test/test1");
        dto.setEquivalentResource(Set.of("http://uri.suomi.fi/datamodel/ns/int#EqRes"));
        dto.setEditorialNote("comment");
        dto.setLabel(Map.of("fi", "test label"));
        dto.setStatus(Status.DRAFT);
        dto.setNote(Map.of("fi", "test note"));

        mapper.mapToResource("http://uri.suomi.fi/datamodel/ns/test", m, dto);

        Resource modelResource = m.getResource("http://uri.suomi.fi/datamodel/ns/test");
        Resource resourceResource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#Resource");

        assertNotNull(modelResource);
        assertNotNull(resourceResource);

        assertEquals(1, modelResource.listProperties(DCTerms.hasPart).toList().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#Resource", modelResource.getProperty(DCTerms.hasPart).getString());

        assertEquals(OWL.DatatypeProperty, resourceResource.getProperty(RDF.type).getResource());

        assertEquals(1, resourceResource.listProperties(RDFS.label).toList().size());
        assertEquals("test label", resourceResource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resourceResource.getProperty(RDFS.label).getLiteral().getLanguage());

        assertEquals(Status.DRAFT, Status.valueOf(resourceResource.getProperty(OWL.versionInfo).getObject().toString()));
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resourceResource.getProperty(RDFS.isDefinedBy).getObject().toString());

        assertEquals(XSDDatatype.XSDNCName, resourceResource.getProperty(DCTerms.identifier).getLiteral().getDatatype());
        assertEquals("Resource", resourceResource.getProperty(DCTerms.identifier).getLiteral().getString());

        assertEquals("comment", resourceResource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals("test note", resourceResource.getProperty(SKOS.note).getLiteral().getString());

        assertEquals(1, resourceResource.listProperties(RDFS.subPropertyOf).toList().size());
        assertEquals(OWL2.topDataProperty, resourceResource.getProperty(RDFS.subPropertyOf).getResource());
        assertEquals(1, resourceResource.listProperties(OWL.equivalentProperty).toList().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/int#EqRes", resourceResource.getProperty(OWL.equivalentProperty).getObject().toString());
    }

    @Test
    void mapToResourceAttribute() {
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/test_datamodel.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var dto = new ResourceDTO();
        dto.setIdentifier("Resource");
        dto.setType(ResourceType.ATTRIBUTE);
        dto.setSubject("http://uri.suomi.fi/terminology/test/test1");
        dto.setEquivalentResource(Set.of("http://uri.suomi.fi/datamodel/ns/int#EqRes"));
        dto.setSubResourceOf(Set.of("https://www.example.com/ns/ext#SubRes"));
        dto.setEditorialNote("comment");
        dto.setLabel(Map.of("fi", "test label"));
        dto.setStatus(Status.DRAFT);
        dto.setNote(Map.of("fi", "test note"));

        mapper.mapToResource("http://uri.suomi.fi/datamodel/ns/test", m, dto);

        Resource modelResource = m.getResource("http://uri.suomi.fi/datamodel/ns/test");
        Resource resourceResource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#Resource");

        assertNotNull(modelResource);
        assertNotNull(resourceResource);

        assertEquals(1, modelResource.listProperties(DCTerms.hasPart).toList().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#Resource", modelResource.getProperty(DCTerms.hasPart).getString());

        assertEquals(OWL.DatatypeProperty, resourceResource.getProperty(RDF.type).getResource());

        assertEquals(1, resourceResource.listProperties(RDFS.label).toList().size());
        assertEquals("test label", resourceResource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resourceResource.getProperty(RDFS.label).getLiteral().getLanguage());

        assertEquals(Status.DRAFT, Status.valueOf(resourceResource.getProperty(OWL.versionInfo).getObject().toString()));
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resourceResource.getProperty(RDFS.isDefinedBy).getObject().toString());

        assertEquals(XSDDatatype.XSDNCName, resourceResource.getProperty(DCTerms.identifier).getLiteral().getDatatype());
        assertEquals("Resource", resourceResource.getProperty(DCTerms.identifier).getLiteral().getString());

        assertEquals("comment", resourceResource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals("test note", resourceResource.getProperty(SKOS.note).getLiteral().getString());

        assertEquals(1, resourceResource.listProperties(RDFS.subPropertyOf).toList().size());
        assertEquals("https://www.example.com/ns/ext#SubRes", resourceResource.getProperty(RDFS.subPropertyOf).getObject().toString());
        assertEquals(1, resourceResource.listProperties(OWL.equivalentProperty).toList().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/int#EqRes", resourceResource.getProperty(OWL.equivalentProperty).getObject().toString());
    }

    @Test
    void testMapToIndexResourceClass(){
        when(jenaService.doesResourceExistInGraph(anyString(), anyString())).thenReturn(true);
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var indexClass = mapper.mapToIndexResource(m, "http://uri.suomi.fi/datamodel/ns/test#TestClass");

        assertEquals("http://uri.suomi.fi/datamodel/ns/test#TestClass", indexClass.getId());
        assertEquals("test note fi", indexClass.getNote().get("fi"));
        assertEquals(Status.VALID, indexClass.getStatus());
        assertEquals("TestClass", indexClass.getIdentifier());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", indexClass.getIsDefinedBy());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#", indexClass.getNamespace());
        assertEquals(1, indexClass.getLabel().size());
        assertEquals("test label", indexClass.getLabel().get("fi"));
    }

    @Test
    void testMapToIndexResourceAttribute(){
        when(jenaService.doesResourceExistInGraph(anyString(), anyString())).thenReturn(true);
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var indexClass = mapper.mapToIndexResource(m, "http://uri.suomi.fi/datamodel/ns/test#TestAttribute");

        assertEquals("http://uri.suomi.fi/datamodel/ns/test#TestAttribute", indexClass.getId());
        assertEquals("test note fi", indexClass.getNote().get("fi"));
        assertEquals(Status.VALID, indexClass.getStatus());
        assertEquals("TestAttribute", indexClass.getIdentifier());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", indexClass.getIsDefinedBy());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#", indexClass.getNamespace());
        assertEquals(1, indexClass.getLabel().size());
        assertEquals("test attribute", indexClass.getLabel().get("fi"));
    }

    @Test
    void testMapToIndexResourceAssociation(){
        when(jenaService.doesResourceExistInGraph(anyString(), anyString())).thenReturn(true);
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var indexClass = mapper.mapToIndexResource(m, "http://uri.suomi.fi/datamodel/ns/test#TestAssociation");

        assertEquals("http://uri.suomi.fi/datamodel/ns/test#TestAssociation", indexClass.getId());
        assertEquals("test note fi", indexClass.getNote().get("fi"));
        assertEquals(Status.VALID, indexClass.getStatus());
        assertEquals("TestAssociation", indexClass.getIdentifier());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", indexClass.getIsDefinedBy());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#", indexClass.getNamespace());
        assertEquals(1, indexClass.getLabel().size());
        assertEquals("test association", indexClass.getLabel().get("fi"));
    }

    @Test
    void testMapToIndexResourceMinimalClass(){
        when(jenaService.doesResourceExistInGraph(anyString(), anyString())).thenReturn(true);
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_minimal_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var indexClass = mapper.mapToIndexResource(m, "http://uri.suomi.fi/datamodel/ns/test#TestClass");

        assertEquals("http://uri.suomi.fi/datamodel/ns/test#TestClass", indexClass.getId());
        assertEquals(Status.VALID, indexClass.getStatus());
        assertEquals("TestClass", indexClass.getIdentifier());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", indexClass.getIsDefinedBy());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#", indexClass.getNamespace());
        assertEquals(1, indexClass.getLabel().size());
        assertEquals("test label", indexClass.getLabel().get("fi"));
        assertNull(indexClass.getNote());
    }

    @Test
    void testMapToIndexResourceMinimalAttribute(){
        when(jenaService.doesResourceExistInGraph(anyString(), anyString())).thenReturn(true);
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_minimal_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var indexResource = mapper.mapToIndexResource(m, "http://uri.suomi.fi/datamodel/ns/test#TestAttribute");

        assertEquals("http://uri.suomi.fi/datamodel/ns/test#TestAttribute", indexResource.getId());
        assertEquals(Status.VALID, indexResource.getStatus());
        assertEquals("TestAttribute", indexResource.getIdentifier());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", indexResource.getIsDefinedBy());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#", indexResource.getNamespace());
        assertEquals(1, indexResource.getLabel().size());
        assertEquals("test attribute", indexResource.getLabel().get("fi"));
        assertNull(indexResource.getNote());
    }

    @Test
    void testMapToIndexResourceMinimalAssociation(){
        when(jenaService.doesResourceExistInGraph(anyString(), anyString())).thenReturn(true);
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_minimal_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var indexResource = mapper.mapToIndexResource(m, "http://uri.suomi.fi/datamodel/ns/test#TestAssociation");

        assertEquals("http://uri.suomi.fi/datamodel/ns/test#TestAssociation", indexResource.getId());
        assertEquals(Status.VALID, indexResource.getStatus());
        assertEquals("TestAssociation", indexResource.getIdentifier());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", indexResource.getIsDefinedBy());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#", indexResource.getNamespace());
        assertEquals(1, indexResource.getLabel().size());
        assertEquals("test association", indexResource.getLabel().get("fi"));
        assertNull(indexResource.getNote());
    }

    @Test
    void mapToResourceInfoDTOAttribute() {
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var dto = mapper.mapToResourceInfoDTO(m, "test", "TestAttribute", getOrgModel(), true);

        assertEquals(ResourceType.ATTRIBUTE, dto.getType());
        assertEquals(1, dto.getLabel().size());
        assertEquals("test attribute", dto.getLabel().get("fi"));
        assertEquals("comment visible for admin", dto.getEditorialNote());
        assertEquals(Status.VALID, dto.getStatus());
        assertEquals(1, dto.getSubResourceOf().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubResource", dto.getSubResourceOf().stream().findFirst().orElse(""));
        assertEquals(1, dto.getEquivalentResource().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#EqResource", dto.getEquivalentResource().stream().findFirst().orElse(""));
        assertEquals("http://uri.suomi.fi/terminology/test/test1", dto.getSubject());
        assertEquals("TestAttribute", dto.getIdentifier());
        assertEquals(2, dto.getNote().size());
        assertEquals("test note fi", dto.getNote().get("fi"));
        assertEquals("test note en", dto.getNote().get("en"));
        assertEquals("2023-02-03T11:46:36.404Z", dto.getModified());
        assertEquals("2023-02-03T11:46:36.404Z", dto.getCreated());
        assertEquals("test org", dto.getContributor().stream().findFirst().orElseThrow().getLabel().get("fi"));
        assertEquals("7d3a3c00-5a6b-489b-a3ed-63bb58c26a63", dto.getContributor().stream().findFirst().orElseThrow().getId());
    }

    @Test
    void mapToResourceInfoDTOAssociation() {
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var dto = mapper.mapToResourceInfoDTO(m, "test", "TestAssociation", getOrgModel(), true);

        assertEquals(ResourceType.ASSOCIATION, dto.getType());
        assertEquals(1, dto.getLabel().size());
        assertEquals("test association", dto.getLabel().get("fi"));
        assertEquals("comment visible for admin", dto.getEditorialNote());
        assertEquals(Status.VALID, dto.getStatus());
        assertEquals(1, dto.getSubResourceOf().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubResource", dto.getSubResourceOf().stream().findFirst().orElse(""));
        assertEquals(1, dto.getEquivalentResource().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#EqResource", dto.getEquivalentResource().stream().findFirst().orElse(""));
        assertEquals("http://uri.suomi.fi/terminology/test/test1", dto.getSubject());
        assertEquals("TestAssociation", dto.getIdentifier());
        assertEquals(2, dto.getNote().size());
        assertEquals("test note fi", dto.getNote().get("fi"));
        assertEquals("test note en", dto.getNote().get("en"));
        assertEquals("2023-02-03T11:46:36.404Z", dto.getModified());
        assertEquals("2023-02-03T11:46:36.404Z", dto.getCreated());
        assertEquals("test org", dto.getContributor().stream().findFirst().orElseThrow().getLabel().get("fi"));
        assertEquals("7d3a3c00-5a6b-489b-a3ed-63bb58c26a63", dto.getContributor().stream().findFirst().orElseThrow().getId());
    }

    @Test
    void mapToResourceInfoDTOAttributeMinimal() {
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_minimal_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var dto = mapper.mapToResourceInfoDTO(m, "test", "TestAttribute", getOrgModel(), true);

        assertEquals(ResourceType.ATTRIBUTE, dto.getType());
        assertEquals(1, dto.getLabel().size());
        assertEquals("test attribute", dto.getLabel().get("fi"));
        assertNull(dto.getEditorialNote());
        assertEquals(Status.VALID, dto.getStatus());
        assertEquals(0, dto.getSubResourceOf().size());
        assertEquals(0, dto.getEquivalentResource().size());
        assertNull(dto.getSubject());
        assertEquals("TestAttribute", dto.getIdentifier());
        assertEquals(0, dto.getNote().size());
        assertEquals("2023-02-03T11:46:36.404Z", dto.getModified());
        assertEquals("2023-02-03T11:46:36.404Z", dto.getCreated());
        assertEquals("test org", dto.getContributor().stream().findFirst().orElseThrow().getLabel().get("fi"));
        assertEquals("7d3a3c00-5a6b-489b-a3ed-63bb58c26a63", dto.getContributor().stream().findFirst().orElseThrow().getId());
    }

    @Test
    void mapToResourceInfoDTOAssociationMinimal() {
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_minimal_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        var dto = mapper.mapToResourceInfoDTO(m, "test", "TestAssociation", getOrgModel(), true);

        assertEquals(ResourceType.ASSOCIATION, dto.getType());
        assertEquals(1, dto.getLabel().size());
        assertEquals("test association", dto.getLabel().get("fi"));
        assertNull(dto.getEditorialNote());
        assertEquals(Status.VALID, dto.getStatus());
        assertEquals(0, dto.getSubResourceOf().size());
        assertEquals(0, dto.getEquivalentResource().size());
        assertNull(dto.getSubject());
        assertEquals("TestAssociation", dto.getIdentifier());
        assertEquals(0, dto.getNote().size());
        assertEquals("2023-02-03T11:46:36.404Z", dto.getModified());
        assertEquals("2023-02-03T11:46:36.404Z", dto.getCreated());
        assertEquals("test org", dto.getContributor().stream().findFirst().orElseThrow().getLabel().get("fi"));
        assertEquals("7d3a3c00-5a6b-489b-a3ed-63bb58c26a63", dto.getContributor().stream().findFirst().orElseThrow().getId());
    }



    @Test
    void failMapToResourceInfoDTOClass(){
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);


        assertThrowsExactly(MappingError.class, () -> mapper.mapToResourceInfoDTO(m, "test", "TestClass", getOrgModel(), true));
    }

    @Test
    void failMapToResourceInfoDTOClassMinimal(){
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_minimal_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        assertThrowsExactly(MappingError.class, () -> mapper.mapToResourceInfoDTO(m, "test", "TestClass", getOrgModel(), true));
    }

    @Test
    void mapToUpdateModelAttribute() {
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);
        var resource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#TestAttribute");

        var dto = new ResourceDTO();
        dto.setLabel(Map.of("fi", "new label"));
        dto.setStatus(Status.INVALID);
        dto.setNote(Map.of("fi", "new note"));
        dto.setEquivalentResource(Set.of("http://uri.suomi.fi/datamodel/ns/int#NewEq"));
        dto.setSubResourceOf(Set.of("https://www.example.com/ns/ext#NewSub"));
        dto.setSubject("http://uri.suomi.fi/terminology/qwe");
        dto.setEditorialNote("new editorial note");

        assertEquals(OWL.DatatypeProperty, resource.getProperty(RDF.type).getResource());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resource.getProperty(RDFS.isDefinedBy).getObject().toString());
        assertEquals("test attribute", resource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resource.getProperty(RDFS.label).getLiteral().getLanguage());
        assertEquals("TestAttribute", resource.getProperty(DCTerms.identifier).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/terminology/test/test1", resource.getProperty(DCTerms.subject).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#EqResource", resource.getProperty(OWL.equivalentProperty).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubResource", resource.getProperty(RDFS.subPropertyOf).getObject().toString());
        assertEquals(Status.VALID.name(), resource.getProperty(OWL.versionInfo).getObject().toString());
        assertEquals("comment visible for admin", resource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals(2, resource.listProperties(SKOS.note).toList().size());

        mapper.mapToUpdateResource("http://uri.suomi.fi/datamodel/ns/test", m, "TestAttribute", dto);

        assertEquals(OWL.DatatypeProperty, resource.getProperty(RDF.type).getResource());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resource.getProperty(RDFS.isDefinedBy).getObject().toString());
        assertEquals("new label", resource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resource.getProperty(RDFS.label).getLiteral().getLanguage());
        assertEquals("TestAttribute", resource.getProperty(DCTerms.identifier).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/terminology/qwe", resource.getProperty(DCTerms.subject).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/int#NewEq", resource.getProperty(OWL.equivalentProperty).getObject().toString());
        assertEquals("https://www.example.com/ns/ext#NewSub", resource.getProperty(RDFS.subPropertyOf).getObject().toString());
        assertEquals(Status.INVALID.name(), resource.getProperty(OWL.versionInfo).getObject().toString());
        assertEquals("new editorial note", resource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals(1, resource.listProperties(SKOS.note).toList().size());
        assertEquals("new note", resource.getProperty(SKOS.note).getLiteral().getString());
        assertEquals("fi", resource.getProperty(SKOS.note).getLiteral().getLanguage());
    }

    @Test
    void mapToUpdateModelAssociation() {
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);
        var resource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#TestAssociation");

        var dto = new ResourceDTO();
        dto.setLabel(Map.of("fi", "new label"));
        dto.setStatus(Status.INVALID);
        dto.setNote(Map.of("fi", "new note"));
        dto.setEquivalentResource(Set.of("http://uri.suomi.fi/datamodel/ns/int#NewEq"));
        dto.setSubResourceOf(Set.of("https://www.example.com/ns/ext#NewSub"));
        dto.setSubject("http://uri.suomi.fi/terminology/qwe");
        dto.setEditorialNote("new editorial note");

        assertEquals(OWL.ObjectProperty, resource.getProperty(RDF.type).getResource());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resource.getProperty(RDFS.isDefinedBy).getObject().toString());
        assertEquals("test association", resource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resource.getProperty(RDFS.label).getLiteral().getLanguage());
        assertEquals("TestAssociation", resource.getProperty(DCTerms.identifier).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/terminology/test/test1", resource.getProperty(DCTerms.subject).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#EqResource", resource.getProperty(OWL.equivalentProperty).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubResource", resource.getProperty(RDFS.subPropertyOf).getObject().toString());
        assertEquals(Status.VALID.name(), resource.getProperty(OWL.versionInfo).getObject().toString());
        assertEquals("comment visible for admin", resource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals(2, resource.listProperties(SKOS.note).toList().size());

        mapper.mapToUpdateResource("http://uri.suomi.fi/datamodel/ns/test", m, "TestAssociation", dto);

        assertEquals(OWL.ObjectProperty, resource.getProperty(RDF.type).getResource());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resource.getProperty(RDFS.isDefinedBy).getObject().toString());
        assertEquals("new label", resource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resource.getProperty(RDFS.label).getLiteral().getLanguage());
        assertEquals("TestAssociation", resource.getProperty(DCTerms.identifier).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/terminology/qwe", resource.getProperty(DCTerms.subject).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/int#NewEq", resource.getProperty(OWL.equivalentProperty).getObject().toString());
        assertEquals("https://www.example.com/ns/ext#NewSub", resource.getProperty(RDFS.subPropertyOf).getObject().toString());
        assertEquals(Status.INVALID.name(), resource.getProperty(OWL.versionInfo).getObject().toString());
        assertEquals("new editorial note", resource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals(1, resource.listProperties(SKOS.note).toList().size());
        assertEquals("new note", resource.getProperty(SKOS.note).getLiteral().getString());
        assertEquals("fi", resource.getProperty(SKOS.note).getLiteral().getLanguage());
    }

    @Test
    void mapToUpdateAttributeEmptySubResource(){
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);
        var resource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#TestAttribute");

        //Shouldn't change if null
        mapper.mapToUpdateResource("http://uri.suomi.fi/datamodel/ns/test", m, "TestAttribute", new ResourceDTO());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubResource", resource.getProperty(RDFS.subPropertyOf).getObject().toString());

        var dto = new ResourceDTO();
        dto.setSubResourceOf(Set.of());

        //should change if empty
        mapper.mapToUpdateResource("http://uri.suomi.fi/datamodel/ns/test", m, "TestAttribute", dto);
        assertEquals("http://www.w3.org/2002/07/owl#topDataProperty", resource.getProperty(RDFS.subPropertyOf).getObject().toString());
    }

    @Test
    void mapToUpdateAssociationEmptySubResource(){
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);
        var resource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#TestAssociation");

        //Shouldn't change if null
        mapper.mapToUpdateResource("http://uri.suomi.fi/datamodel/ns/test", m, "TestAssociation", new ResourceDTO());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubResource", resource.getProperty(RDFS.subPropertyOf).getObject().toString());

        var dto = new ResourceDTO();
        dto.setSubResourceOf(Set.of());

        //should change if empty
        mapper.mapToUpdateResource("http://uri.suomi.fi/datamodel/ns/test", m, "TestAssociation", dto);
        assertEquals("http://www.w3.org/2002/07/owl#topObjectProperty", resource.getProperty(RDFS.subPropertyOf).getObject().toString());
    }

    @Test
    void failToMapUpdateModelClass() {
        Model m = ModelFactory.createDefaultModel();
        var stream = getClass().getResourceAsStream("/models/test_datamodel_with_resources.ttl");
        assertNotNull(stream);
        RDFDataMgr.read(m, stream, RDFLanguages.TURTLE);

        assertThrowsExactly(MappingError.class, () -> mapper.mapToUpdateResource("http://uri.suomi.fi/datamodel/ns/test", m, "TestClass", new ResourceDTO()));

    }

    private Model getOrgModel(){
        var model = ModelFactory.createDefaultModel();
              model.createResource("urn:uuid:7d3a3c00-5a6b-489b-a3ed-63bb58c26a63")
                .addProperty(RDF.type, FOAF.Organization)
                .addProperty(SKOS.prefLabel, ResourceFactory.createLangLiteral("test org", "fi"));
        return model;
    }
}
