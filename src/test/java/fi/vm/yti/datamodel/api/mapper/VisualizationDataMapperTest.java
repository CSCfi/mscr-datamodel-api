package fi.vm.yti.datamodel.api.mapper;

import fi.vm.yti.datamodel.api.v2.mapper.ModelMapper;
import fi.vm.yti.datamodel.api.v2.mapper.VisualizationMapper;
import org.apache.jena.rdf.model.ModelFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.*;


@ExtendWith(SpringExtension.class)
@Import({
    VisualizationMapper.class
})
class VisualizationDataMapperTest {

    @Autowired
    VisualizationMapper mapper;
    
    String defaultNamespace = "http://custom.resolver.fi/datamodel/ns/";

    @BeforeEach
    public void init(){    	
    	ReflectionTestUtils.setField(mapper, "defaultNamespace", defaultNamespace);
    }
    @Test
    void mapVisualizationData() {
        var positions = ModelFactory.createDefaultModel();
        var model = MapperTestUtils.getModelFromFile("/models/test_datamodel_visualization.ttl");

        var result = mapper.mapVisualizationData("test", model, positions);

        assertEquals(3, result.size());

        var class1 = result.stream().filter(r -> r.getIdentifier().equals("testclass1"))
                .findFirst();
        assertTrue(class1.isPresent());

        var class2 = result.stream().filter(r -> r.getIdentifier().equals("testclass2"))
                .findFirst();
        assertTrue(class2.isPresent());

        var class3 = result.stream().filter(r -> r.getIdentifier().equals("testclass3"))
                .findFirst();
        assertTrue(class3.isPresent());

        assertEquals("Label 1", class1.get().getLabel().get("fi"));
        assertEquals("yti-model:TestClass", class1.get().getParentClasses().iterator().next());

        var parent2 = class2.get().getParentClasses().iterator().next();
        assertEquals("testclass1", parent2);

        var parent3 = class3.get().getParentClasses().iterator().next();
        assertEquals("ext:ExternalClass", parent3);

        var attributes = class1.get().getAttributes();
        var associations = class1.get().getAssociations();
        assertEquals(1, attributes.size());
        assertEquals(1, associations.size());

        var attribute = attributes.get(0);
        var association = associations.get(0);

        var externalAssociation1 = class2.get().getAssociations().get(0);
        var externalAssociation2 = class3.get().getAssociations().get(0);

        assertEquals("testiattribuutti", attribute.getLabel().get("fi"));
        assertEquals("attribute-1", attribute.getIdentifier());
        assertEquals("testiassosiaatio", association.getLabel().get("fi"));
        assertEquals("association-1", association.getIdentifier());
        assertEquals("testclass2", ((LinkedList<String>)association.getPath()).getLast());
        assertEquals("yti-model:SomeClass", ((LinkedList<String>)externalAssociation1.getPath()).getLast());
        assertEquals("ext:ExternalClass", ((LinkedList<String>)externalAssociation2.getPath()).getLast());
    }
}
