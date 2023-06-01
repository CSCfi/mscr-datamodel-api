package fi.vm.yti.datamodel.api.mapper;

import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.endpoint.EndpointUtils;
import fi.vm.yti.datamodel.api.v2.mapper.ClassMapper;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.topbraid.shacl.vocabulary.SH;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;


@ExtendWith(SpringExtension.class)
class ClassMapperTest {

	private String defaultNamespace = "http://custom.resolver.fi/datamodel/ns/";
	
    @Test
    void testCreateClassAndMapToModelLibrary() {
        var m = MapperTestUtils.getModelFromFile("/test_datamodel_library.ttl");
        var mockUser = EndpointUtils.mockUser;

        ClassDTO dto = new ClassDTO();
        dto.setIdentifier("TestClass");
        dto.setSubject("http://uri.suomi.fi/terminology/test/test1");
        dto.setEquivalentClass(Set.of(defaultNamespace + "int#EqClass"));
        dto.setSubClassOf(Set.of("https://www.example.com/ns/ext#SubClass"));
        dto.setEditorialNote("comment");
        dto.setLabel(Map.of("fi", "test label"));
        dto.setStatus(Status.DRAFT);
        dto.setNote(Map.of("fi", "test note"));

        ClassMapper.createClassAndMapToModel(defaultNamespace + "test", m, dto, mockUser);

        Resource modelResource = m.getResource(defaultNamespace + "test");
        Resource classResource = m.getResource(defaultNamespace + "test#TestClass");

        assertNotNull(modelResource);
        assertNotNull(classResource);

        assertEquals(1, modelResource.listProperties(DCTerms.hasPart).toList().size());
        assertEquals(defaultNamespace + "test#TestClass", modelResource.getProperty(DCTerms.hasPart).getObject().toString());

        assertEquals(1, classResource.listProperties(RDFS.label).toList().size());
        assertEquals("test label", classResource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", classResource.getProperty(RDFS.label).getLiteral().getLanguage());

        assertEquals(OWL.Class, classResource.getProperty(RDF.type).getResource());
        assertEquals(Status.DRAFT, Status.valueOf(classResource.getProperty(OWL.versionInfo).getObject().toString()));
        assertEquals(defaultNamespace + "test", classResource.getProperty(RDFS.isDefinedBy).getObject().toString());

        assertEquals(XSDDatatype.XSDNCName, classResource.getProperty(DCTerms.identifier).getLiteral().getDatatype());
        assertEquals("TestClass", classResource.getProperty(DCTerms.identifier).getLiteral().getString());

        assertEquals("comment", classResource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals("test note", classResource.getProperty(SKOS.note).getLiteral().getString());

        assertEquals(1, classResource.listProperties(RDFS.subClassOf).toList().size());
        assertEquals("https://www.example.com/ns/ext#SubClass", classResource.getProperty(RDFS.subClassOf).getObject().toString());
        assertEquals(1, classResource.listProperties(OWL.equivalentClass).toList().size());
        assertEquals(defaultNamespace + "int#EqClass", classResource.getProperty(OWL.equivalentClass).getObject().toString());
        assertEquals(mockUser.getId().toString(), classResource.getProperty(Iow.creator).getString());
        assertEquals(mockUser.getId().toString(), classResource.getProperty(Iow.modifier).getString());
    }

    @Test
    void testCreateClassAndMapToModelLibraryOwlThing() {
        var m = MapperTestUtils.getModelFromFile("/test_datamodel_library.ttl");
        var mockUser = EndpointUtils.mockUser;

        ClassDTO dto = new ClassDTO();
        dto.setSubClassOf(Collections.emptySet());
        dto.setStatus(Status.VALID);
        dto.setIdentifier("Identifier");

        ClassMapper.createClassAndMapToModel(defaultNamespace + "test", m, dto, mockUser);

        Resource modelResource = m.getResource(defaultNamespace + "test");
        Resource classResource = m.getResource(defaultNamespace + "test#Identifier");

        assertNotNull(modelResource);
        assertNotNull(classResource);

        assertEquals(1, classResource.listProperties(RDFS.subClassOf).toList().size());
        assertEquals(OWL.Thing, classResource.getProperty(RDFS.subClassOf).getResource());
    }

    @Test
    void testCreateClassAndMapToModelProfile() {
        var m = MapperTestUtils.getModelFromFile("/test_datamodel_profile.ttl");
        var mockUser = EndpointUtils.mockUser;

        ClassDTO dto = new ClassDTO();
        dto.setIdentifier("TestClass");
        dto.setSubject("http://uri.suomi.fi/terminology/test/test1");
        dto.setEquivalentClass(Set.of("http://uri.suomi.fi/datamodel/ns/int#EqClass"));
        dto.setSubClassOf(Set.of("https://www.example.com/ns/ext#SubClass"));
        dto.setEditorialNote("comment");
        dto.setLabel(Map.of("fi", "test label"));
        dto.setStatus(Status.DRAFT);
        dto.setNote(Map.of("fi", "test note"));
        dto.setTargetClass("http://uri.suomi.fi/datamodel/ns/target#Class");

        ClassMapper.createClassAndMapToModel("http://uri.suomi.fi/datamodel/ns/test", m, dto, mockUser);

        Resource modelResource = m.getResource("http://uri.suomi.fi/datamodel/ns/test");
        Resource classResource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#TestClass");

        assertNotNull(modelResource);
        assertNotNull(classResource);

        assertEquals(1, modelResource.listProperties(DCTerms.hasPart).toList().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#TestClass", modelResource.getProperty(DCTerms.hasPart).getObject().toString());

        assertEquals(1, classResource.listProperties(RDFS.label).toList().size());
        assertEquals("test label", classResource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", classResource.getProperty(RDFS.label).getLiteral().getLanguage());

        assertEquals(SH.NodeShape, classResource.getProperty(RDF.type).getResource());
        assertEquals(Status.DRAFT, Status.valueOf(classResource.getProperty(OWL.versionInfo).getObject().toString()));
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", classResource.getProperty(RDFS.isDefinedBy).getObject().toString());

        assertEquals(XSDDatatype.XSDNCName, classResource.getProperty(DCTerms.identifier).getLiteral().getDatatype());
        assertEquals("TestClass", classResource.getProperty(DCTerms.identifier).getLiteral().getString());

        assertEquals("comment", MapperUtils.propertyToString(classResource, DCTerms.description));
        assertEquals("test note", classResource.getProperty(SH.description).getLiteral().getString());

        assertEquals(1, classResource.listProperties(RDFS.subClassOf).toList().size());
        assertEquals("https://www.example.com/ns/ext#SubClass", classResource.getProperty(RDFS.subClassOf).getObject().toString());
        assertEquals(1, classResource.listProperties(OWL.equivalentClass).toList().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/int#EqClass", classResource.getProperty(OWL.equivalentClass).getObject().toString());
        assertEquals(mockUser.getId().toString(), classResource.getProperty(Iow.creator).getString());
        assertEquals(mockUser.getId().toString(), classResource.getProperty(Iow.modifier).getString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/target#Class", classResource.getProperty(SH.targetClass).getObject().toString());

    }

    @Test
    void testMapToClassDTOLibrary(){
        var m = MapperTestUtils.getModelFromFile("/models/test_datamodel_library_with_resources.ttl");

        var dto = ClassMapper.mapToClassDTO(m, "http://uri.suomi.fi/datamodel/ns/test", "TestClass", getOrgModel(), false, null);

        // not authenticated
        assertNull(dto.getEditorialNote());
        assertEquals(Status.VALID, dto.getStatus());
        assertEquals("TestClass", dto.getIdentifier());
        assertEquals(1, dto.getEquivalentClass().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#EqClass", dto.getEquivalentClass().stream().findFirst().orElse(null));
        assertEquals(1, dto.getSubClassOf().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubClass", dto.getSubClassOf().stream().findFirst().orElse(null));
        assertEquals(1, dto.getLabel().size());
        assertEquals("test label", dto.getLabel().get("fi"));
        assertEquals("http://uri.suomi.fi/terminology/test/test1", dto.getSubject().getConceptURI());
        assertEquals(2, dto.getNote().size());
        assertEquals("test note fi", dto.getNote().get("fi"));
        assertEquals("test note en", dto.getNote().get("en"));
        assertEquals("2023-02-03T11:46:36.404Z", dto.getModified());
        assertEquals("2023-02-03T11:46:36.404Z", dto.getCreated());
        assertEquals("test org", dto.getContributor().stream().findFirst().orElseThrow().getLabel().get("fi"));
        assertEquals("7d3a3c00-5a6b-489b-a3ed-63bb58c26a63", dto.getContributor().stream().findFirst().orElseThrow().getId());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#TestClass", dto.getUri());
    }

    @Test
    void testMapToClassDTOProfile(){
        var m = MapperTestUtils.getModelFromFile("/models/test_datamodel_profile_with_resources.ttl");

        var dto = ClassMapper.mapToClassDTO(m, "http://uri.suomi.fi/datamodel/ns/test", "TestClass", getOrgModel(), false, null);

        // not authenticated
        assertNull(dto.getEditorialNote());
        assertEquals(Status.VALID, dto.getStatus());
        assertEquals("TestClass", dto.getIdentifier());
        assertEquals(1, dto.getEquivalentClass().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#EqClass", dto.getEquivalentClass().stream().findFirst().orElse(null));
        assertEquals(1, dto.getSubClassOf().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubClass", dto.getSubClassOf().stream().findFirst().orElse(null));
        assertEquals(1, dto.getLabel().size());
        assertEquals("test label", dto.getLabel().get("fi"));
        assertEquals("http://uri.suomi.fi/terminology/test/test1", dto.getSubject().getConceptURI());
        assertEquals(2, dto.getNote().size());
        assertEquals("test note fi", dto.getNote().get("fi"));
        assertEquals("test note en", dto.getNote().get("en"));
        assertEquals("2023-02-03T11:46:36.404Z", dto.getModified());
        assertEquals("2023-02-03T11:46:36.404Z", dto.getCreated());
        assertEquals("test org", dto.getContributor().stream().findFirst().orElseThrow().getLabel().get("fi"));
        assertEquals("7d3a3c00-5a6b-489b-a3ed-63bb58c26a63", dto.getContributor().stream().findFirst().orElseThrow().getId());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#TestClass", dto.getUri());
        assertEquals("http://uri.suomi.fi/datamodel/ns/target#Class", dto.getTargetClass());
    }


    @Test
    void testMapToClassMinimalDTO(){
        var m = MapperTestUtils.getModelFromFile("/models/test_datamodel_with_minimal_resources.ttl");

        var dto = ClassMapper.mapToClassDTO(m, "http://uri.suomi.fi/datamodel/ns/test", "TestClass", getOrgModel(), true, null);

        // not authenticated
        assertNull(dto.getEditorialNote());
        assertEquals(Status.VALID, dto.getStatus());
        assertEquals("TestClass", dto.getIdentifier());
        assertTrue(dto.getEquivalentClass().isEmpty());
        assertTrue(dto.getSubClassOf().isEmpty());
        assertEquals(1, dto.getLabel().size());
        assertEquals("test label", dto.getLabel().get("fi"));
        assertNull(dto.getSubject());
        assertTrue(dto.getNote().isEmpty());
        assertEquals("2023-02-03T11:46:36.404Z", dto.getModified());
        assertEquals("2023-02-03T11:46:36.404Z", dto.getCreated());
        assertEquals("test org", dto.getContributor().stream().findFirst().orElseThrow().getLabel().get("fi"));
        assertEquals("7d3a3c00-5a6b-489b-a3ed-63bb58c26a63", dto.getContributor().stream().findFirst().orElseThrow().getId());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#TestClass", dto.getUri());
    }

    @Test
    void testMapToClassDTOAuthenticatedUser() {
        var m = MapperTestUtils.getModelFromFile("/models/test_datamodel_library_with_resources.ttl");

        Consumer<ResourceInfoBaseDTO> userMapper = (ResourceInfoBaseDTO dto) -> {
            var creator = new UserDTO("123");
            var modifier = new UserDTO("123");
            creator.setName("creator fake-user");
            modifier.setName("modifier fake-user");
            dto.setCreator(creator);
            dto.setModifier(modifier);
        };

        var dto = ClassMapper.mapToClassDTO(m, "http://uri.suomi.fi/datamodel/ns/test", "TestClass", getOrgModel(), true, userMapper);

        assertEquals("comment visible for admin", dto.getEditorialNote());
        assertEquals("creator fake-user", dto.getCreator().getName());
        assertEquals("modifier fake-user", dto.getModifier().getName());
    }

    @Test
    void testMapToUpdateClassLibrary(){
        var m = MapperTestUtils.getModelFromFile("/models/test_datamodel_library_with_resources.ttl");
        var resource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#TestClass");
        var mockUser = EndpointUtils.mockUser;

        var dto = new ClassDTO();
        dto.setLabel(Map.of("fi", "new label"));
        dto.setStatus(Status.INVALID);
        dto.setNote(Map.of("fi", "new note"));
        dto.setEquivalentClass(Set.of("http://uri.suomi.fi/datamodel/ns/int#NewEq"));
        dto.setSubClassOf(Set.of("https://www.example.com/ns/ext#NewSub"));
        dto.setSubject("http://uri.suomi.fi/terminology/qwe");
        dto.setEditorialNote("new editorial note");

        assertEquals(OWL.Class, resource.getProperty(RDF.type).getResource());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resource.getProperty(RDFS.isDefinedBy).getObject().toString());
        assertEquals("test label", resource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resource.getProperty(RDFS.label).getLiteral().getLanguage());
        assertEquals("TestClass", resource.getProperty(DCTerms.identifier).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/terminology/test/test1", resource.getProperty(DCTerms.subject).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#EqClass", resource.getProperty(OWL.equivalentClass).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubClass", resource.getProperty(RDFS.subClassOf).getObject().toString());
        assertEquals(Status.VALID.name(), resource.getProperty(OWL.versionInfo).getObject().toString());
        assertEquals("comment visible for admin", resource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals(2, resource.listProperties(SKOS.note).toList().size());

        ClassMapper.mapToUpdateClass(m, "http://uri.suomi.fi/datamodel/ns/test", resource, dto, mockUser);

        assertEquals(OWL.Class, resource.getProperty(RDF.type).getResource());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resource.getProperty(RDFS.isDefinedBy).getObject().toString());
        assertEquals("new label", resource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resource.getProperty(RDFS.label).getLiteral().getLanguage());
        assertEquals("TestClass", resource.getProperty(DCTerms.identifier).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/terminology/qwe", resource.getProperty(DCTerms.subject).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/int#NewEq", resource.getProperty(OWL.equivalentClass).getObject().toString());
        assertEquals("https://www.example.com/ns/ext#NewSub", resource.getProperty(RDFS.subClassOf).getObject().toString());
        assertEquals(Status.INVALID.name(), resource.getProperty(OWL.versionInfo).getObject().toString());
        assertEquals("new editorial note", resource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals(1, resource.listProperties(SKOS.note).toList().size());
        assertEquals("new note", resource.getProperty(SKOS.note).getLiteral().getString());
        assertEquals("fi", resource.getProperty(SKOS.note).getLiteral().getLanguage());
        assertEquals(mockUser.getId().toString(), resource.getProperty(Iow.modifier).getObject().toString());
        assertEquals("2a5c075f-0d0e-4688-90e0-29af1eebbf6d", resource.getProperty(Iow.creator).getObject().toString());
    }

    @Test
    void testMapToUpdateClassProfile(){
        var m = MapperTestUtils.getModelFromFile("/models/test_datamodel_profile_with_resources.ttl");
        var resource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#TestClass");
        var mockUser = EndpointUtils.mockUser;

        var dto = new ClassDTO();
        dto.setLabel(Map.of("fi", "new label"));
        dto.setStatus(Status.INVALID);
        dto.setNote(Map.of("fi", "new note"));
        dto.setEquivalentClass(Set.of("http://uri.suomi.fi/datamodel/ns/int#NewEq"));
        dto.setSubClassOf(Set.of("https://www.example.com/ns/ext#NewSub"));
        dto.setSubject("http://uri.suomi.fi/terminology/qwe");
        dto.setEditorialNote("new editorial note");
        dto.setTargetClass("http://uri.suomi.fi/datamodel/ns/target#NewClass");

        assertEquals(SH.NodeShape, resource.getProperty(RDF.type).getResource());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resource.getProperty(RDFS.isDefinedBy).getObject().toString());
        assertEquals("test label", resource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resource.getProperty(RDFS.label).getLiteral().getLanguage());
        assertEquals("TestClass", resource.getProperty(DCTerms.identifier).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/terminology/test/test1", resource.getProperty(DCTerms.subject).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#EqClass", resource.getProperty(OWL.equivalentClass).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubClass", resource.getProperty(RDFS.subClassOf).getObject().toString());
        assertEquals(Status.VALID.name(), resource.getProperty(OWL.versionInfo).getObject().toString());
        assertEquals("comment visible for admin", resource.getProperty(DCTerms.description).getObject().toString());
        assertEquals(2, resource.listProperties(SH.description).toList().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/target#Class", resource.getProperty(SH.targetClass).getObject().toString());

        ClassMapper.mapToUpdateClass(m, "http://uri.suomi.fi/datamodel/ns/test", resource, dto, mockUser);

        assertEquals(SH.NodeShape, resource.getProperty(RDF.type).getResource());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resource.getProperty(RDFS.isDefinedBy).getObject().toString());
        assertEquals("new label", resource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resource.getProperty(RDFS.label).getLiteral().getLanguage());
        assertEquals("TestClass", resource.getProperty(DCTerms.identifier).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/terminology/qwe", resource.getProperty(DCTerms.subject).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/int#NewEq", resource.getProperty(OWL.equivalentClass).getObject().toString());
        assertEquals("https://www.example.com/ns/ext#NewSub", resource.getProperty(RDFS.subClassOf).getObject().toString());
        assertEquals(Status.INVALID.name(), resource.getProperty(OWL.versionInfo).getObject().toString());
        assertEquals("new editorial note", resource.getProperty(DCTerms.description).getObject().toString());
        assertEquals(1, resource.listProperties(SH.description).toList().size());
        assertEquals("new note", resource.getProperty(SH.description).getLiteral().getString());
        assertEquals("fi", resource.getProperty(SH.description).getLiteral().getLanguage());
        assertEquals(mockUser.getId().toString(), resource.getProperty(Iow.modifier).getObject().toString());
        assertEquals("2a5c075f-0d0e-4688-90e0-29af1eebbf6d", resource.getProperty(Iow.creator).getObject().toString());
    }

    @Test
    void testMapToUpdateClassNullValuesDTO(){
        var m = MapperTestUtils.getModelFromFile("/models/test_datamodel_library_with_resources.ttl");
        var resource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#TestClass");
        var mockUser = EndpointUtils.mockUser;

        var dto = new ClassDTO();

        assertEquals(OWL.Class, resource.getProperty(RDF.type).getResource());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resource.getProperty(RDFS.isDefinedBy).getObject().toString());
        assertEquals("test label", resource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resource.getProperty(RDFS.label).getLiteral().getLanguage());
        assertEquals("TestClass", resource.getProperty(DCTerms.identifier).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/terminology/test/test1", resource.getProperty(DCTerms.subject).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#EqClass", resource.getProperty(OWL.equivalentClass).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubClass", resource.getProperty(RDFS.subClassOf).getObject().toString());
        assertEquals(Status.VALID.name(), resource.getProperty(OWL.versionInfo).getObject().toString());
        assertEquals("comment visible for admin", resource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals(2, resource.listProperties(SKOS.note).toList().size());

        ClassMapper.mapToUpdateClass(m, "http://uri.suomi.fi/datamodel/ns/test", resource, dto, mockUser);

        assertEquals(OWL.Class, resource.getProperty(RDF.type).getResource());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test", resource.getProperty(RDFS.isDefinedBy).getObject().toString());
        assertEquals("test label", resource.getProperty(RDFS.label).getLiteral().getString());
        assertEquals("fi", resource.getProperty(RDFS.label).getLiteral().getLanguage());
        assertEquals("TestClass", resource.getProperty(DCTerms.identifier).getLiteral().getString());
        assertEquals("http://uri.suomi.fi/terminology/test/test1", resource.getProperty(DCTerms.subject).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#EqClass", resource.getProperty(OWL.equivalentClass).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubClass", resource.getProperty(RDFS.subClassOf).getObject().toString());
        assertEquals(Status.VALID.name(), resource.getProperty(OWL.versionInfo).getObject().toString());
        assertEquals("comment visible for admin", resource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals(2, resource.listProperties(SKOS.note).toList().size());
    }

    @Test
    void testMapToUpdateClassEmptyValuesDTO(){
        var m = MapperTestUtils.getModelFromFile("/models/test_datamodel_library_with_resources.ttl");
        var resource = m.getResource("http://uri.suomi.fi/datamodel/ns/test#TestClass");
        var mockUser = EndpointUtils.mockUser;

        var dto = new ClassDTO();
        dto.setSubject("");
        dto.setEquivalentClass(Collections.emptySet());
        dto.setSubClassOf(Collections.emptySet());
        dto.setEditorialNote("");
        dto.setNote(Collections.emptyMap());

        assertEquals("http://uri.suomi.fi/terminology/test/test1", resource.getProperty(DCTerms.subject).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#EqClass", resource.getProperty(OWL.equivalentClass).getObject().toString());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#SubClass", resource.getProperty(RDFS.subClassOf).getObject().toString());
        assertEquals("comment visible for admin", resource.getProperty(SKOS.editorialNote).getObject().toString());
        assertEquals(2, resource.listProperties(SKOS.note).toList().size());

        ClassMapper.mapToUpdateClass(m, "http://uri.suomi.fi/datamodel/ns/test", resource, dto, mockUser);

        assertNull(resource.getProperty(DCTerms.subject));
        assertNull(resource.getProperty(OWL.equivalentClass));
        //OWl thing is default value if all subClassOf is emptied
        assertEquals(OWL.Thing, resource.getProperty(RDFS.subClassOf).getResource());
        assertNull(resource.getProperty(SKOS.editorialNote));
        assertNull(resource.getProperty(SKOS.note));
    }

    @Test
    void testAddDomainAndRangeToClassDTO(){
        var m = MapperTestUtils.getModelFromFile("/models/test_resource_query_model.ttl");

        var dto = new ClassInfoDTO();
        ClassMapper.addClassResourcesToDTO(m, dto);

        assertEquals(1, dto.getAttribute().size());

        var attribute = dto.getAttribute().get(0);

        assertEquals("test label", attribute.getLabel().get("fi"));
        assertEquals("test", attribute.getIdentifier());
        assertEquals("http://uri.suomi.fi/datamodel/ns/test#rangetest2", attribute.getUri());
        assertEquals("test", attribute.getModelId());
    }

    private Model getOrgModel(){
        var model = ModelFactory.createDefaultModel();
        model.createResource("urn:uuid:7d3a3c00-5a6b-489b-a3ed-63bb58c26a63")
                .addProperty(RDF.type, FOAF.Organization)
                .addProperty(SKOS.prefLabel, ResourceFactory.createLangLiteral("test org", "fi"));
        return model;
    }

}
