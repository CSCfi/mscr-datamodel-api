package fi.vm.yti.datamodel.api.mapper;

import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.mapper.ModelMapper;
import fi.vm.yti.datamodel.api.v2.mapper.VisualizationMapper;
import org.apache.jena.rdf.model.ModelFactory;
import org.junit.jupiter.api.BeforeEach;
import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.mapper.VisualizationMapper;
import org.apache.jena.vocabulary.DCTerms;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;


@ExtendWith(SpringExtension.class)
@Import({
    VisualizationMapper.class,
    ModelConstants.class
})
class VisualizationDataMapperTest {

    @Autowired
    VisualizationMapper mapper;
    @Autowired
    ModelConstants modelConstants;
    
    String defaultNamespace = "http://uri.suomi.fi/datamodel/ns/";

    @BeforeEach
    public void init(){    	
    	//ReflectionTestUtils.setField(mapper, "defaultNamespace", defaultNamespace);
    }

    private final Map<String, String> libraryNamespaces = Map.of(
            "http://uri.suomi.fi/datamodel/ns/yti-model", "yti-model",
            "https://www.example.com/ns/external", "ext");

    private final Map<String, String> profileNamespaces = Map.of(
            "http://uri.suomi.fi/datamodel/ns/personprof", "personprof",
            "http://uri.suomi.fi/datamodel/ns/ytm", "ytm"
    );

    @Test
    void mapLibraryClassWithAttributes() {
        var model = MapperTestUtils.getModelFromFile("/models/test_datamodel_visualization.ttl");

        var classDTO = VisualizationMapper.mapClass("http://uri.suomi.fi/datamodel/ns/test/testclass1", model, libraryNamespaces);
        assertEquals("Label 1", classDTO.getLabel().get("fi"));
        assertEquals("testclass1", classDTO.getIdentifier());
        assertEquals("yti-model:TestClass", classDTO.getParentClasses().iterator().next());

        var attribute = model.getResource("http://uri.suomi.fi/datamodel/ns/test/attribute-1");
        var association = model.getResource("http://uri.suomi.fi/datamodel/ns/test/association-1");

        VisualizationMapper.mapResource(classDTO, attribute, model, libraryNamespaces);
        VisualizationMapper.mapResource(classDTO, association, model, libraryNamespaces);

        assertEquals(1, classDTO.getAttributes().size());
        assertEquals(1, classDTO.getAssociations().size());

        assertEquals("testiattribuutti", classDTO.getAttributes().get(0).getLabel().get("fi"));
        assertEquals("testiassosiaatio", classDTO.getAssociations().get(0).getLabel().get("fi"));
        assertEquals("attribute-1", classDTO.getAttributes().get(0).getIdentifier());
        assertEquals("association-1", classDTO.getAssociations().get(0).getIdentifier());
        assertEquals("testclass2", classDTO.getAssociations().get(0).getReferenceTarget());
    }

    @Test
    void mapLibraryClassWithExternalResources() {
        var model = MapperTestUtils.getModelFromFile("/models/test_datamodel_visualization.ttl");

        var classDTO = VisualizationMapper.mapClass("http://uri.suomi.fi/datamodel/ns/test/testclass3", model, libraryNamespaces);

        var association = model.getResource("http://uri.suomi.fi/datamodel/ns/test/association-3");

        VisualizationMapper.mapResource(classDTO, association, model, libraryNamespaces);

        assertEquals("ext:ExternalClass", classDTO.getParentClasses().iterator().next());
        assertEquals("ext:ExternalClass", classDTO.getAssociations().get(0).getReferenceTarget());
    }

    @Test
    void mapExternalClass() {
        String identifier = "ext:TestClass";

        var external = VisualizationMapper.mapExternalClass(identifier, Set.of("fi", "en"));

        assertEquals(identifier, external.getIdentifier());
        assertEquals(Map.of("fi", identifier, "en", identifier), external.getLabel());
    }

    @Test
    void mapApplicationProfileClass() {
        var model = MapperTestUtils.getModelFromFile("/models/test_application_profile_visualization.ttl");

        var classDTO = (VisualizationNodeShapeDTO) VisualizationMapper
                .mapClass("http://uri.suomi.fi/datamodel/ns/visuprof/person", model,profileNamespaces);

        assertEquals("person", classDTO.getIdentifier());
        assertEquals("Henkilö", classDTO.getLabel().get("fi"));
        assertEquals("ytm:person", classDTO.getTargetClass());

        var attribute = model.getResource("http://uri.suomi.fi/datamodel/ns/visuprof/age");
        var association = model.getResource("http://uri.suomi.fi/datamodel/ns/visuprof/is-address");

        VisualizationMapper.mapResource(classDTO, attribute, model, profileNamespaces);
        VisualizationMapper.mapResource(classDTO, association, model, profileNamespaces);

        var mappedAttribute = (VisualizationPropertyShapeAttributeDTO) classDTO.getAttributes().get(0);
        var mappedAssociation = (VisualizationPropertyShapeAssociationDTO) classDTO.getAssociations().get(0);

        assertNotNull(mappedAttribute);
        assertNotNull(mappedAssociation);

        assertEquals("Ikä", mappedAttribute.getLabel().get("fi"));
        assertEquals("age", mappedAttribute.getIdentifier());
        assertEquals("xsd:integer", mappedAttribute.getDataType());
        assertEquals("ytm:age", mappedAttribute.getPath());
        assertEquals(1, mappedAttribute.getMinCount());
        assertEquals(1, mappedAttribute.getMaxCount());

        assertEquals("onOsoite", mappedAssociation.getLabel().get("fi"));
        assertEquals("is-address", mappedAssociation.getIdentifier());
        assertEquals("ytm:is-address", mappedAssociation.getPath());
        assertEquals(1, mappedAssociation.getMinCount());
        assertEquals(2, mappedAssociation.getMaxCount());
        assertEquals("address", mappedAssociation.getReferenceTarget());
    }

    @Test
    void mapPositionDataToGraph() {
        var positionGraphURI = ModelConstants.MODEL_POSITIONS_NAMESPACE + "test-model";

        var position = new PositionDataDTO();
        position.setIdentifier("class-1");
        position.setX(3.0);
        position.setY(5.0);
        position.setReferenceTargets(Set.of("class-2"));

        var positionModel = VisualizationMapper.mapPositionDataToModel(positionGraphURI, List.of(position));

        var resource = positionModel.getResource(positionGraphURI + ModelConstants.RESOURCE_SEPARATOR + position.getIdentifier());

        assertEquals("class-1", resource.getProperty(DCTerms.identifier).getString());
        assertEquals(3.0, resource.getProperty(Iow.posX).getLiteral().getDouble());
        assertEquals(5.0, resource.getProperty(Iow.posY).getLiteral().getDouble());
        assertEquals("class-2", resource.listProperties(Iow.referenceTarget).toList().get(0).getString());
    }

    @Test
    void mapPositionDataWithParentClasses() {
        var modelPrefix = "test-model";
        var positionGraphURI = ModelConstants.MODEL_POSITIONS_NAMESPACE + modelPrefix;

        var node1 = new PositionDataDTO();
        node1.setIdentifier("class-1");
        node1.setX(1.0);
        node1.setY(2.0);
        node1.setReferenceTargets(Set.of("class-2"));

        var node2 = new PositionDataDTO();
        node2.setIdentifier("class-2");
        node2.setX(0.0);
        node2.setY(10.0);

        var postionModel = VisualizationMapper.mapPositionDataToModel(positionGraphURI,
                List.of(node1, node2));

        var classDTO1 = new VisualizationClassDTO();
        classDTO1.setIdentifier("class-1");
        classDTO1.setParentClasses(Set.of("class-2"));

        var classDTO2 = new VisualizationClassDTO();
        classDTO2.setIdentifier("class-2");

        var classes = new HashSet<VisualizationClassDTO>();
        classes.add(classDTO1);
        classes.add(classDTO2);

        var hiddenNodes = VisualizationMapper.mapPositionsDataToDTOsAndCreateHiddenNodes(postionModel, modelPrefix, classes);
        assertEquals(0, hiddenNodes.size());

        var c1 = findClass(classes, "class-1");
        assertNotNull(c1);
        assertEquals(1.0, c1.getPosition().getX());
        assertEquals(2.0, c1.getPosition().getY());

        assertEquals(2, classes.size());
        assertEquals("class-2", c1.getParentClasses().iterator().next());
    }

    @Test
    void mapPositionDataWithParentClassesAndHiddenNode() {
        var modelPrefix = "test-model";
        var positionGraphURI = ModelConstants.MODEL_POSITIONS_NAMESPACE + modelPrefix;

        var node1 = new PositionDataDTO();
        node1.setIdentifier("class-1");
        node1.setX(1.0);
        node1.setY(2.0);
        node1.setReferenceTargets(Set.of("corner-1"));

        var node2 = new PositionDataDTO();
        node2.setIdentifier("class-2");
        node2.setX(0.0);
        node2.setY(10.0);

        var corner1 = new PositionDataDTO();
        corner1.setIdentifier("corner-1");
        corner1.setX(0.0);
        corner1.setY(0.0);
        corner1.setReferenceTargets(Set.of("class-2"));

        var postionModel = VisualizationMapper.mapPositionDataToModel(positionGraphURI,
                List.of(node1, node2, corner1));

        var classDTO1 = new VisualizationClassDTO();
        classDTO1.setIdentifier("class-1");
        classDTO1.setParentClasses(Set.of("class-2"));

        var classDTO2 = new VisualizationClassDTO();
        classDTO2.setIdentifier("class-2");

        var classes = new HashSet<VisualizationClassDTO>();
        classes.add(classDTO1);
        classes.add(classDTO2);

        var hiddenNodes = VisualizationMapper.mapPositionsDataToDTOsAndCreateHiddenNodes(postionModel, modelPrefix, classes);

        var c1 = findClass(classes, "class-1");
        var hidden = findHiddenNode(hiddenNodes, "corner-1");
        assertNotNull(c1);
        assertNotNull(hidden);

        assertEquals(2, classes.size());
        assertEquals(1, hiddenNodes.size());
        assertEquals("corner-1", c1.getParentClasses().iterator().next());
        assertEquals("class-2", hidden.getReferenceTarget());
    }

    @Test
    void mapPositionDataWithParentClassesAndAssociationsAndHiddenNodes() {
        var modelPrefix = "test-model";
        var positionGraphURI = ModelConstants.MODEL_POSITIONS_NAMESPACE + modelPrefix;

        // class-1 has parent class reference to class-2 via corner-1 and corner-2
        // class-1 has association to class-3 via corner-3
        // class-2 has parent class reference to class-3 directly
        var node1 = new PositionDataDTO();
        node1.setIdentifier("class-1");
        node1.setX(1.0);
        node1.setY(2.0);
        node1.setReferenceTargets(Set.of("corner-1", "corner-3"));

        var node2 = new PositionDataDTO();
        node2.setIdentifier("class-2");
        node2.setX(0.0);
        node2.setY(10.0);
        node2.setReferenceTargets(Set.of("class-3"));

        var node3 = new PositionDataDTO();
        node3.setIdentifier("class-3");
        node3.setX(20.0);
        node3.setY(15.0);

        var corner1 = new PositionDataDTO();
        corner1.setX(6.0);
        corner1.setY(7.0);
        corner1.setIdentifier("corner-1");
        corner1.setReferenceTargets(Set.of("corner-2"));

        var corner2 = new PositionDataDTO();
        corner2.setX(3.0);
        corner2.setY(6.0);
        corner2.setIdentifier("corner-2");
        corner2.setReferenceTargets(Set.of("class-2"));

        var corner3 = new PositionDataDTO();
        corner3.setX(3.0);
        corner3.setY(6.0);
        corner3.setIdentifier("corner-3");
        corner3.setReferenceTargets(Set.of("class-3"));

        var postionModel = VisualizationMapper.mapPositionDataToModel(positionGraphURI,
                List.of(node1, node2, node3, corner1, corner2, corner3));

        var association = new VisualizationAssociationDTO();
        association.setReferenceTarget("class-3");
        association.setIdentifier("assoc-1");

        var classDTO1 = new VisualizationClassDTO();
        classDTO1.setIdentifier("class-1");
        classDTO1.setParentClasses(Set.of("class-2"));
        classDTO1.setAssociations(List.of(association));

        var classDTO2 = new VisualizationClassDTO();
        classDTO2.setIdentifier("class-2");
        classDTO2.setParentClasses(Set.of("class-3"));

        var classDTO3 = new VisualizationClassDTO();
        classDTO3.setIdentifier("class-3");

        var classes = new HashSet<VisualizationClassDTO>();
        classes.add(classDTO1);
        classes.add(classDTO2);
        classes.add(classDTO3);

        var hiddenNodes = VisualizationMapper.mapPositionsDataToDTOsAndCreateHiddenNodes(postionModel, modelPrefix, classes);

        // should contain three classes and three hidden nodes
        assertEquals(3, classes.size());
        assertEquals(3, hiddenNodes.size());

        var identifiers = hiddenNodes.stream().map(VisualizationHiddenNodeDTO::getIdentifier).toList();
        assertTrue(identifiers.containsAll(List.of("corner-1", "corner-2", "corner-3")));

        var classItem = findClass(classes, "class-1");
        assertNotNull(classItem);

        var classItem2 = findClass(classes, "class-2");
        assertNotNull(classItem2);
        assertEquals("class-3", classItem2.getParentClasses().iterator().next());

        assertEquals("corner-1", classItem.getParentClasses().iterator().next());
        assertEquals(1.0, classItem.getPosition().getX());
        assertEquals(2.0, classItem.getPosition().getY());

        var cornerItem1 = findHiddenNode(hiddenNodes, "corner-1");
        assertNotNull(cornerItem1);
        assertEquals(6.0, cornerItem1.getPosition().getX());
        assertEquals(7.0, cornerItem1.getPosition().getY());
        assertEquals("corner-2", cornerItem1.getReferenceTarget());

        var cornerItem2 = findHiddenNode(hiddenNodes, "corner-2");
        assertNotNull(cornerItem2);
        assertEquals("class-2", cornerItem2.getReferenceTarget());

        var cornerItem3 = findHiddenNode(hiddenNodes, "corner-3");
        assertNotNull(cornerItem3);
        assertEquals("class-3", cornerItem3.getReferenceTarget());
    }

    @Test
    void mapPositionDataAssociationRecursive() {
        var modelPrefix = "test-model";
        var positionGraphURI = ModelConstants.MODEL_POSITIONS_NAMESPACE + modelPrefix;

        var association = new VisualizationAssociationDTO();
        association.setReferenceTarget("class-1");
        association.setIdentifier("assoc-1");

        var node1 = new PositionDataDTO();
        node1.setIdentifier("class-1");
        node1.setX(1.0);
        node1.setY(2.0);
        node1.setReferenceTargets(Set.of("corner-1"));

        var corner1 = new PositionDataDTO();
        corner1.setX(6.0);
        corner1.setY(7.0);
        corner1.setIdentifier("corner-1");
        corner1.setReferenceTargets(Set.of("corner-2"));

        var corner2 = new PositionDataDTO();
        corner2.setX(6.0);
        corner2.setY(7.0);
        corner2.setIdentifier("corner-2");
        corner2.setReferenceTargets(Set.of("class-1"));

        var positionModel = VisualizationMapper.mapPositionDataToModel(positionGraphURI,
                List.of(node1, corner1, corner2));

        var classDTO1 = new VisualizationClassDTO();
        classDTO1.setIdentifier("class-1");
        classDTO1.setAssociations(List.of(association));

        var classes = new HashSet<VisualizationClassDTO>();
        classes.add(classDTO1);

        var hiddenNodes = VisualizationMapper.mapPositionsDataToDTOsAndCreateHiddenNodes(positionModel, modelPrefix, classes);

        assertEquals(1, classes.size());
        assertEquals(2, hiddenNodes.size());
    }

    private static VisualizationClassDTO findClass(Set<VisualizationClassDTO> classes, String identifier) {
        return classes.stream().filter(c -> c.getIdentifier().equals(identifier)).findFirst().orElse(null);
    }

    private static VisualizationHiddenNodeDTO findHiddenNode(Set<VisualizationHiddenNodeDTO> hiddenNodes, String identifier) {
        return hiddenNodes.stream().filter(c -> c.getIdentifier().equals(identifier)).findFirst().orElse(null);
    }
}
