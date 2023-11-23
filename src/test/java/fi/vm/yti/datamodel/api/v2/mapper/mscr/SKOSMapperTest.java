package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import static org.junit.jupiter.api.Assertions.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.Test;
import org.topbraid.shacl.vocabulary.SH;

class SKOSMapperTest {
	
	
	
	private byte[] getByteStreamFromPath(String schemaPath) throws Exception, IOException {
		InputStream inputSchemaInputStream = getClass().getClassLoader().getResourceAsStream(schemaPath);
		byte[] inputSchemaInByte = inputSchemaInputStream.readAllBytes();
		inputSchemaInputStream.close();

		return inputSchemaInByte;
	}	
	

	@Test
	void testClarin1() throws Exception {
		byte[] data = getByteStreamFromPath("skos/CSDToolTasks.ttl");
		assertNotNull(data);
		SKOSMapper mapper = new SKOSMapper();
		Model m = mapper.mapToModel("urn:test1", data);
	}
	
	
	@Test
	void testOKM() throws Exception {
		byte[] data = getByteStreamFromPath("skos/okm-tieteenala-skos.ttl");
		assertNotNull(data);
		SKOSMapper mapper = new SKOSMapper();
		Model m = mapper.mapToModel("urn:test1", data);
	}
	
	@Test
	void testSimpleNoTopConcetps() throws Exception {
		byte[] data = getByteStreamFromPath("skos/simple-no-topconcepts.ttl");
		assertNotNull(data);
		SKOSMapper mapper = new SKOSMapper();
		Model m = mapper.mapToModel("urn:test1", data);
	}
	
	@Test
	void testMeta() throws Exception {
		byte[] data = getByteStreamFromPath("skos/metatietosanasto-skos.ttl");
		assertNotNull(data);
		SKOSMapper mapper = new SKOSMapper();
		Model m = mapper.mapToModel("urn:test1", data);
	}
}
