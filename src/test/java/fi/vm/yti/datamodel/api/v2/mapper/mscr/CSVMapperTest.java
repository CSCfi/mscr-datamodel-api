package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.Test;
import org.topbraid.shacl.vocabulary.SH;

class CSVMapperTest {
	
	
	
	private byte[] getByteStreamFromPath(String schemaPath) throws Exception, IOException {
		InputStream inputSchemaInputStream = getClass().getClassLoader().getResourceAsStream(schemaPath);
		byte[] inputSchemaInByte = inputSchemaInputStream.readAllBytes();
		inputSchemaInputStream.close();

		return inputSchemaInByte;
	}	
	

	@Test
	void testValidCSV() throws Exception {
		byte[] data = getByteStreamFromPath("csvschema/semicolon_delimiter.csv");
		assertNotNull(data);
		CSVMapper mapper = new CSVMapper();
		Model m = mapper.mapToModel("urn:test1", data, ";");
		
		assertEquals(3, m.listResourcesWithProperty(RDF.type, SH.PropertyShape).toList().size());							
	}
	
	@Test
	void testValidCSVMultilne() throws Exception {
		byte[] data = getByteStreamFromPath("csvschema/semicolon_delimiter_multiline.csv");
		assertNotNull(data);
		CSVMapper mapper = new CSVMapper();
		Model m = mapper.mapToModel("urn:test1", data, ";");
		
		assertEquals(3, m.listResourcesWithProperty(RDF.type, SH.PropertyShape).toList().size());		
	}

	@Test
	void testValidCSVCommaDelimited() throws Exception {
		byte[] data = getByteStreamFromPath("csvschema/comma_delimiter.csv");
		assertNotNull(data);
		CSVMapper mapper = new CSVMapper();
		Model m = mapper.mapToModel("urn:test1", data, ",");
		
		assertEquals(3, m.listResourcesWithProperty(RDF.type, SH.PropertyShape).toList().size());							
	}

	
	@Test
	void testValidCSVWrongDelimiter() throws Exception {
		byte[] data = getByteStreamFromPath("csvschema/semicolon_delimiter.csv");
		assertNotNull(data);
		CSVMapper mapper = new CSVMapper();
		Model m = mapper.mapToModel("urn:test1", data, ",");
		// the whole line is considered as one property
		assertEquals(1, m.listResourcesWithProperty(RDF.type, SH.PropertyShape).toList().size());							
	}

	
}
