package fi.vm.yti.datamodel.api.v2.service;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.rdf.api.RDF;
import org.apache.jena.rdf.model.Model;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.mapper.ClassMapper;
import fi.vm.yti.datamodel.api.v2.mapper.MappingMapper;
import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.JSONSchemaMapper;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.XSDMapper;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.service.dtr.DTRClient;

@ExtendWith(SpringExtension.class)
@Import({
	SchemaService.class,	
	CrosswalkService.class,
	JSONSchemaMapper.class,
	XSDMapper.class,
	DTRClient.class,
	MappingMapper.class,
})
class CrosswalkServiceTest {

	@Autowired
	private CrosswalkService service;
	
	@Autowired
	private SchemaService schemaService;

	
	private byte[] getBytesFromPath(String schemaPath) throws Exception, IOException {
		InputStream inputSchemaInputStream = getClass().getClassLoader().getResourceAsStream(schemaPath);
		byte[] inputSchemaInByte = inputSchemaInputStream.readAllBytes();
		return inputSchemaInByte;
	}
	
	@Test
	void testSimpleSSSOMLiteralMapping() throws Exception {
		String crosswalkPID = "hdl:crosswalk:test1";
		String sourcePID = "hdl:schema:1";
		String targetPID = "hdl:schema:2";
		Model sourceModel = schemaService.transformEnumSkos(sourcePID, getBytesFromPath("enum/cf-small1.csv"));
		Model targetModel = schemaService.transformEnumSkos(targetPID, getBytesFromPath("enum/gcmd-small1.csv"));
		byte[] crosswalkBytes = getBytesFromPath("sssom/cf-to-gcmd-small1-mappings.csv");
		Model m = service.transformSSSOMToInternal(crosswalkPID, crosswalkBytes, sourcePID, sourceModel, targetPID, targetModel);
		m.write(System.out, "TURTLE");
		
		assertEquals(6, m.listSubjectsWithProperty(org.apache.jena.vocabulary.RDF.type, MSCR.MAPPING).toList().size());
	}

	@Test
	void testSimpleSSSOMLiteralMappingOneMissing() throws Exception {
		String crosswalkPID = "hdl:crosswalk:test1";
		String sourcePID = "hdl:schema:1";
		String targetPID = "hdl:schema:2";
		Model sourceModel = schemaService.transformEnumSkos(sourcePID, getBytesFromPath("enum/cf-small1.csv"));
		Model targetModel = schemaService.transformEnumSkos(targetPID, getBytesFromPath("enum/gcmd-small1.csv"));
		byte[] crosswalkBytes = getBytesFromPath("sssom/cf-to-gcmd-small1-mappings-one-miss.csv");
		Model m = service.transformSSSOMToInternal(crosswalkPID, crosswalkBytes, sourcePID, sourceModel, targetPID, targetModel);
		m.write(System.out, "TURTLE");
		
		assertEquals(5, m.listSubjectsWithProperty(org.apache.jena.vocabulary.RDF.type, MSCR.MAPPING).toList().size());
	}

}
