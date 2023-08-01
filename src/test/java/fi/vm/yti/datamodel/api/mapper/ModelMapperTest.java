package fi.vm.yti.datamodel.api.mapper;


import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.endpoint.EndpointUtils;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import fi.vm.yti.datamodel.api.v2.mapper.ModelMapper;
import fi.vm.yti.datamodel.api.v2.repository.ConceptRepository;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import fi.vm.yti.datamodel.api.v2.repository.SchemesRepository;
import fi.vm.yti.datamodel.api.v2.service.DataModelService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.security.YtiUser;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(SpringExtension.class)
@Import({
        ModelMapper.class,
        ModelConstants.class
})

class ModelMapperTest {

    
    @MockBean
    CoreRepository coreRepository;
    @MockBean
    DataModelService dataModelService;
    @MockBean
    ConceptRepository conceptRepository;
    @MockBean
    ImportsRepository importsRepository;
    @MockBean
    SchemesRepository schemesRepository;
	@MockBean
    JenaService jenaService;
    @Autowired
    ModelMapper mapper;
    @Autowired
    ModelConstants modelConstants;
    

    String defaultNamespace = "http://uri.suomi.fi/datamodel/ns/";
    String defaultResolveBase = "http://uri.suomi.fi";

    @BeforeEach
    public void init(){
        var mockGroups = ModelFactory.createDefaultModel();
        mockGroups.createResource("http://urn.fi/URN:NBN:fi:au:ptvl:v1105")
                .addProperty(SKOS.notation, "P11")
                .addProperty(RDFS.label, ResourceFactory.createLangLiteral("test group", "fi"));
        when(coreRepository.getServiceCategories()).thenReturn(mockGroups);

        var mockOrgs = ModelFactory.createDefaultModel();
        mockOrgs.createResource("urn:uuid:7d3a3c00-5a6b-489b-a3ed-63bb58c26a63")
                .addProperty(RDF.type, FOAF.Organization)
                .addProperty(SKOS.prefLabel, ResourceFactory.createLangLiteral("test org", "fi"));
        when(coreRepository.getOrganizations()).thenReturn(mockOrgs);
        
        ReflectionTestUtils.setField(mapper, "defaultNamespace", defaultNamespace);
        ReflectionTestUtils.setField(modelConstants, "defaultNamespace", defaultNamespace);
        ReflectionTestUtils.setField(modelConstants, "defaultResolveBase", defaultResolveBase);
    }

    @ParameterizedTest
    @EnumSource(ModelType.class)
    void testMapToJenaModel(ModelType modelType) {
        var mockModel = ModelFactory.createDefaultModel();
        mockModel.createResource(defaultNamespace + "newint")
                        .addProperty(RDF.type, DCAP.DCAP)
                        .addProperty(DCAP.preferredXMLNamespacePrefix, "test");
        when(coreRepository.fetch(anyString())).thenReturn(mockModel);

        UUID organizationId = UUID.randomUUID();
        YtiUser mockUser = EndpointUtils.mockUser;

        DataModelDTO dto = new DataModelDTO();
        dto.setPrefix("test");
        dto.setLabel(Map.of(
                "fi", "Test label fi",
                "sv", "Test label sv"));
        dto.setDescription(Map.of(
                "fi", "Test description fi",
                "sv", "Test description sv"));
        dto.setStatus(Status.DRAFT);
        dto.setGroups(Set.of("P11"));
        dto.setLanguages(Set.of("fi", "sv"));
        dto.setOrganizations(Set.of(organizationId));
        dto.setInternalNamespaces(Set.of(defaultNamespace + "newint"));
        dto.setContact("test@localhost");
        if (modelType.equals(ModelType.PROFILE)) {
            dto.setCodeLists(Set.of("http://uri.suomi.fi/codelist/test/testcodelist"));
        }
        dto.setTerminologies(Set.of("http://uri.suomi.fi/terminology/test"));
        dto.setDocumentation(Map.of("fi","""
                test documentation
                # Header
                **bold**
                """));
        var externalDTO = new ExternalNamespaceDTO();
        externalDTO.setName("test dto");
        externalDTO.setNamespace("http://www.w3.org/2000/01/rdf-schema#");
        externalDTO.setPrefix("ext");
        dto.setExternalNamespaces(Set.of(externalDTO));

        Model model = mapper.mapToJenaModel(dto, modelType, mockUser);

        Resource modelResource = model.getResource(defaultNamespace + "test");
        Resource groupResource = model.getResource("http://urn.fi/URN:NBN:fi:uuid:au:ptvl:v1105");
        Resource organizationResource = model.getResource(String.format("urn:uuid:%s", organizationId));

        assertNotNull(modelResource);
        assertNotNull(groupResource);
        assertNotNull(organizationResource);

        assertEquals(2, modelResource.listProperties(RDFS.label).toList().size());
        assertEquals(Status.DRAFT, Status.valueOf(modelResource.getProperty(OWL.versionInfo).getString()));


        var requires = MapperUtils.arrayPropertyToList(modelResource, DCTerms.requires);
        assertEquals(3, requires.size());
        
        if(modelType.equals(ModelType.PROFILE)){
            assertEquals(1, modelResource.listProperties(OWL.imports).toList().size());
            assertTrue(requires.containsAll(
                    List.of("http://www.w3.org/2000/01/rdf-schema#",
                            "http://uri.suomi.fi/terminology/test",
                            "http://uri.suomi.fi/codelist/test/testcodelist"
                    )));
        }else{
            assertEquals(0, modelResource.listProperties(OWL.imports).toList().size());
            assertTrue(requires.containsAll(
                    List.of("http://www.w3.org/2000/01/rdf-schema#",
                            "http://uri.suomi.fi/terminology/test",
                            "http://uri.suomi.fi/datamodel/ns/newint"
                    )));
        }

        assertNotNull(model.getResource("http://example.com/ns/ext"));

        assertEquals(mockUser.getId().toString(), MapperUtils.propertyToString(modelResource, Iow.creator));
        assertEquals(mockUser.getId().toString(), MapperUtils.propertyToString(modelResource, Iow.modifier));

        if (modelType.equals(ModelType.PROFILE)) {
            assertTrue(MapperUtils.arrayPropertyToList(modelResource, DCTerms.requires).contains("http://uri.suomi.fi/codelist/test/testcodelist"));
        } else {
            assertFalse(MapperUtils.arrayPropertyToList(modelResource, DCTerms.requires).contains("http://uri.suomi.fi/codelist/test/testcodelist"));
        }
        assertTrue(MapperUtils.arrayPropertyToList(modelResource, DCTerms.requires).contains("http://uri.suomi.fi/terminology/test"));

        assertEquals("""
                test documentation
                # Header
                **bold**
                """, MapperUtils.localizedPropertyToMap(modelResource, Iow.documentation).get("fi"));

        assertEquals("test@localhost", MapperUtils.propertyToString(modelResource, Iow.contact));
    }

    @Test
    void testMapToUpdateJenaModel() {
        var m = MapperTestUtils.getModelFromFile("/test_datamodel_library.ttl");

        when(coreRepository.fetch("test")).thenReturn(m);
        var mockModel = ModelFactory.createDefaultModel();
        mockModel.createResource(defaultNamespace + "newint")
        		.addProperty(RDF.type, OWL.Ontology)
                .addProperty(DCAP.preferredXMLNamespacePrefix, "test");
        when(coreRepository.fetch(anyString())).thenReturn(mockModel);

        UUID organizationId = UUID.randomUUID();
        YtiUser mockUser = EndpointUtils.mockUser;

        DataModelDTO dto = new DataModelDTO();
        dto.setLabel(Map.of(
                "fi", "new test label"));
        dto.setDescription(Map.of(
                "fi", "new test description"));
        dto.setStatus(Status.DRAFT);
        dto.setGroups(Set.of("P11"));
        dto.setLanguages(Set.of("fi", "sv"));
        dto.setOrganizations(Set.of(organizationId));
        dto.setContact("new@localhost");
        dto.setTerminologies(Set.of("http://uri.suomi.fi/terminology/newtest"));
        dto.setDocumentation(Map.of("fi", """
                hello
                
                new test"""));

        dto.setInternalNamespaces(Set.of(defaultNamespace + "newint"));
        var externalDTO = new ExternalNamespaceDTO();
        externalDTO.setName("test dto");
        externalDTO.setNamespace("http://www.w3.org/2000/01/rdf-schema#");
        externalDTO.setPrefix("ext");

        dto.setExternalNamespaces(Set.of(externalDTO));

        //unchanged values
        Resource modelResource = m.getResource(defaultNamespace + "test");
        assertEquals(1, modelResource.listProperties(RDFS.label).toList().size());
        assertEquals("testlabel", modelResource.listProperties(RDFS.label).next().getString());

        assertEquals(1, modelResource.listProperties(RDFS.comment).toList().size());
        assertEquals("test desc", modelResource.listProperties(RDFS.comment).next().getString());

        assertEquals(Status.VALID, Status.valueOf(modelResource.getProperty(OWL.versionInfo).getString()));

        assertEquals(1, modelResource.listProperties(OWL.imports).toList().size());
        assertEquals(defaultNamespace + "int", modelResource.listProperties(OWL.imports).next().getObject().toString());

        var requires = MapperUtils.arrayPropertyToList(modelResource, DCTerms.requires);
        assertEquals(2, requires.size());
        assertTrue(requires.containsAll(List.of("https://www.example.com/ns/ext", "http://uri.suomi.fi/terminology/test")));

        assertEquals("test@localhost", MapperUtils.propertyToString(modelResource, Iow.contact));

        assertEquals("""
                hello
                test""", MapperUtils.localizedPropertyToMap(modelResource, Iow.documentation).get("fi"));

        Model model = mapper.mapToUpdateJenaModel("test", dto, m, mockUser);

        //changed values
        modelResource = model.getResource(defaultNamespace + "test");
        Resource groupResource = model.getResource("http://urn.fi/URN:NBN:fi:uuid:au:ptvl:v1105");
        Resource organizationResource = model.getResource(String.format("urn:uuid:%s", organizationId));

        assertNotNull(modelResource);
        assertNotNull(groupResource);
        assertNotNull(organizationResource);

        assertEquals(1, modelResource.listProperties(RDFS.label).toList().size());
        assertEquals("new test label", modelResource.listProperties(RDFS.label, "fi").next().getString());

        assertEquals(Status.DRAFT, Status.valueOf(modelResource.getProperty(OWL.versionInfo).getString()));

        requires = MapperUtils.arrayPropertyToList(modelResource, DCTerms.requires);
        //assertEquals(2, requires.size());
        assertTrue(requires.containsAll(List.of("http://www.w3.org/2000/01/rdf-schema#", "http://uri.suomi.fi/terminology/newtest")));

        assertEquals(1, modelResource.listProperties(OWL.imports).toList().size());
        assertEquals("http://uri.suomi.fi/datamodel/ns/newint", modelResource.listProperties(OWL.imports).next().getObject().toString());
        assertEquals(mockUser.getId().toString(), modelResource.getProperty(Iow.modifier).getString());
        assertEquals("2a5c075f-0d0e-4688-90e0-29af1eebbf6d", modelResource.getProperty(Iow.creator).getObject().toString());

        assertEquals("new@localhost", MapperUtils.propertyToString(modelResource, Iow.contact));
        assertEquals("""
                hello
                
                new test""", MapperUtils.localizedPropertyToMap(modelResource, Iow.documentation).get("fi"));
    }

    @Test
    void testMapToUpdateModelProfile(){
        //Testing profile specific properties
        var m = MapperTestUtils.getModelFromFile("/test_datamodel_profile.ttl");
        when(coreRepository.fetch("test")).thenReturn(m);
        var mockModel = ModelFactory.createDefaultModel();
        mockModel.createResource("http://uri.suomi.fi/datamodel/ns/newint")
                .addProperty(RDF.type, DCAP.DCAP)
                .addProperty(DCAP.preferredXMLNamespacePrefix, "test");
        when(coreRepository.fetch(anyString())).thenReturn(mockModel);

        YtiUser mockUser = EndpointUtils.mockUser;

        DataModelDTO dto = new DataModelDTO();
        dto.setStatus(Status.DRAFT);
        dto.setCodeLists(Set.of("http://uri.suomi.fi/codelist/test/newcodelist"));

        Resource modelResource = m.getResource("http://uri.suomi.fi/datamodel/ns/test");
        assertEquals("http://uri.suomi.fi/codelist/test/testcodelist", MapperUtils.propertyToString(modelResource, DCTerms.requires));

        Model model = mapper.mapToUpdateJenaModel("test", dto, m, mockUser);


        //changed values
        modelResource = model.getResource("http://uri.suomi.fi/datamodel/ns/test");
        assertEquals("http://uri.suomi.fi/codelist/test/newcodelist", MapperUtils.propertyToString(modelResource, DCTerms.requires));


    }

    @Test
    void testMapToDatamodelDTO() {
        var m = MapperTestUtils.getModelFromFile("/test_datamodel_library.ttl");

        var result = mapper.mapToDataModelDTO("test", m, null);

        assertEquals("test", result.getPrefix());
        assertEquals(ModelType.LIBRARY, result.getType());
        assertEquals(Status.VALID, result.getStatus());

        assertEquals(1, result.getLabel().size());
        assertTrue(result.getLabel().containsValue("testlabel"));
        assertTrue(result.getLabel().containsKey("fi"));


        assertEquals(1, result.getDescription().size());
        assertTrue(result.getDescription().containsValue("test desc"));
        assertTrue(result.getDescription().containsKey("fi"));

        assertEquals(1, result.getLanguages().size());
        assertTrue(result.getLanguages().contains("fi"));

        assertEquals(1, result.getOrganizations().size());
        assertEquals("7d3a3c00-5a6b-489b-a3ed-63bb58c26a63", result.getOrganizations().stream().findFirst().orElseThrow().getId());

        assertEquals(1, result.getGroups().size());
        assertEquals("P11", result.getGroups().stream().findFirst().orElseThrow().getIdentifier());

        assertEquals("test@localhost", result.getContact());

        assertEquals("""
                hello
                test""", result.getDocumentation().get("fi"));
    }

    @Test
    void testMapToDatamodelDtoOld(){
        var mOld = MapperTestUtils.getModelFromFile("/test_datamodel_v1.ttl");

        var resultOld = mapper.mapToDataModelDTO("testaa", mOld, null);

        assertEquals("testaa", resultOld.getPrefix());
        assertEquals(ModelType.PROFILE, resultOld.getType());
        assertEquals(Status.DRAFT, resultOld.getStatus());

        assertEquals(1, resultOld.getLabel().size());
        assertTrue(resultOld.getLabel().containsValue("Testaa"));
        assertTrue(resultOld.getLabel().containsKey("fi"));


        assertEquals(1, resultOld.getDescription().size());
        assertTrue(resultOld.getDescription().containsValue("Testaa desc"));
        assertTrue(resultOld.getDescription().containsKey("fi"));

        assertEquals(2, resultOld.getLanguages().size());
        assertTrue(resultOld.getLanguages().contains("fi"));
        assertTrue(resultOld.getLanguages().contains("en"));

        assertEquals(1, resultOld.getOrganizations().size());
        assertEquals("7d3a3c00-5a6b-489b-a3ed-63bb58c26a63", resultOld.getOrganizations().stream().findFirst().orElseThrow().getId());

        assertEquals(1, resultOld.getGroups().size());
        assertEquals("P11", resultOld.getGroups().stream().findFirst().orElseThrow().getIdentifier());
    }

    @Test
    void testMapToIndexModel() {
        var m = MapperTestUtils.getModelFromFile("/test_datamodel_library.ttl");

        var result = mapper.mapToIndexModel("test", m);

        assertEquals("test", result.getPrefix());
        //assertEquals(ModelConstants.DEFAULT_NAMESPACE + "test", result.getId());
        assertEquals(ModelType.LIBRARY, result.getType());
        assertEquals(Status.VALID, result.getStatus());
        assertEquals("2023-01-03T12:44:45.799Z", result.getModified());
        assertEquals("2023-01-03T12:44:45.799Z", result.getCreated());
        assertEquals("2023-01-03T12:44:45.799Z", result.getContentModified());

        assertEquals(1, result.getLabel().size());
        assertTrue(result.getLabel().containsValue("testlabel"));
        assertTrue(result.getLabel().containsKey("fi"));

        assertEquals(1, result.getComment().size());
        assertTrue(result.getComment().containsValue("test desc"));
        assertTrue(result.getComment().containsKey("fi"));

        assertEquals(1, result.getLanguage().size());
        assertTrue(result.getLanguage().contains("fi"));

        assertEquals(1, result.getContributor().size());
        assertTrue(result.getContributor().contains(UUID.fromString("7d3a3c00-5a6b-489b-a3ed-63bb58c26a63")));

        assertEquals(1, result.getIsPartOf().size());
        assertTrue(result.getIsPartOf().contains("P11"));
    }

    @Test
    void testMapToIndexModelOld(){
        var mOld = MapperTestUtils.getModelFromFile("/test_datamodel_v1.ttl");

        var resultOld = mapper.mapToIndexModel("testaa", mOld);

        assertEquals("testaa", resultOld.getPrefix());
        //assertEquals(ModelConstants.DEFAULT_NAMESPACE + "testaa", resultOld.getId());
        assertEquals(ModelType.PROFILE, resultOld.getType());
        assertEquals(Status.DRAFT, resultOld.getStatus());
        assertEquals("2018-03-20T16:21:07.067Z", resultOld.getModified());
        assertEquals("2018-03-20T17:59:44", resultOld.getCreated());

        assertEquals(1, resultOld.getLabel().size());
        assertTrue(resultOld.getLabel().containsValue("Testaa"));
        assertTrue(resultOld.getLabel().containsKey("fi"));

        assertEquals(1, resultOld.getComment().size());
        assertTrue(resultOld.getComment().containsValue("Testaa desc"));
        assertTrue(resultOld.getComment().containsKey("fi"));

        assertEquals(2, resultOld.getLanguage().size());
        assertTrue(resultOld.getLanguage().contains("fi"));
        assertTrue(resultOld.getLanguage().contains("en"));

        assertEquals(1, resultOld.getContributor().size());
        assertTrue(resultOld.getContributor().contains(UUID.fromString("7d3a3c00-5a6b-489b-a3ed-63bb58c26a63")));

        assertEquals(1, resultOld.getIsPartOf().size());
        assertTrue(resultOld.getIsPartOf().contains("P11"));
    }
}
