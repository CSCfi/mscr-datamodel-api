package fi.vm.yti.datamodel.api.v2.transformation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.underscore.U;

import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.service.DataTransformationService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.impl.FnoDataTransformationServiceImpl;

@ExtendWith(SpringExtension.class)
@Import({
	JenaService.class,		
	CoreRepository.class,
	FnoDataTransformationServiceImpl.class
})
@Disabled
public class FnoTransformerTest {
	
	@Autowired
	private DataTransformationService service;
	
	/*
	 * csv2csv
	 * field names changes
	 */
	
	
	@Test
	public void testSimple1CSVtoCSV1() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/source1.csv");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/source1-mapping1.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "csv", mappings, "csv");		
		System.out.println(output);	
		assertEquals("device,temperature,temperature_and_id\n"
				+ "3,32,32|3\n", output);
	}
	
	@Test
	public void testSimple1CSVtoJSON1() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/source1.csv");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/source1-mapping1.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "csv", mappings, "json");		
		System.out.println(output);
		assertEquals("{\"device\":\"3\",\"temperature\":\"32\",\"temperature_and_id\":\"32|3\"}", output);
	}
	@Test
	public void testSimple1CSVtoXML1() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/source1.csv");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/source1-mapping1.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "csv", mappings, "xml");		
		System.out.println(output);		
		assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" 
				+ "<root>\n"
				+ "  <device>3</device>\n"
				+ "  <temperature>32</temperature>\n"
				+ "  <temperature_and_id>32|3</temperature_and_id>\n"
				+ "</root>", output);
	}	
	@Test
	public void testSimple1_1JSONtoJSON1() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/source1.1.json");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/source1-mapping1.1.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "json", mappings, "json");		
		System.out.println(output);	
		assertEquals("{\"device\":\"1\",\"temperature1\":\"20\",\"timestamp_and_id\":\"1|1696330740\"}",
			output);
	}
	
	
	/*
	 * csv2cscv
	 * field names changes
	 * date transformation
	 */
	
	@Test
	public void testSimple1CSVtoCSV2() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/source1.csv");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/source1-mapping2.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "csv", mappings, "csv");		
		System.out.println(output);
		assertEquals("temperature_celcius,timestamp\n"
				+ "32,2023-10-04\n", output);
	}
		
	
	/*
	 * csv2cscv
	 * field names changes
	 * date preprosessing and one to many transformation
	 */	
	
	@Test
	public void testSimple1CSVtoCSV3() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/source1.csv");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/source1-mapping3.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "csv", mappings, "csv");		
		System.out.println(output);
		assertEquals("device,timestamp_year,timestamp_month,timestamp_day\n"
				+ "3,2023,10,04\n", output);
	}
	
	@Test
	public void testSimple1CSVtoCSV4() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/source1.csv");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/source1-mapping4.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "csv", mappings, "csv");		
		System.out.println(output);		
		assertEquals("device,temperature_fahrenheit,temperature_celsius\n3,89.6,32.0\n", output);
	}
	@Test
	public void testSimple1CSVtoJSON4() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/source1.csv");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/source1-mapping4.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "csv", mappings, "json");		
		System.out.println(output);
		assertEquals("{\"device\":\"3\",\"temperature_fahrenheit\":89.6,\"temperature_celsius\":32.0}", output);
	}	
	@Test
	public void testSimple1CSVtoXML3() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/source1.csv");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/source1-mapping3.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "csv", mappings, "xml");		
		System.out.println(output);
		assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
				+ "<root>\n"
				+ "  <device>3</device>\n"
				+ "  <timestamp_year>2023</timestamp_year>\n"
				+ "  <timestamp_month>10</timestamp_month>\n"
				+ "  <timestamp_day>04</timestamp_day>\n"
				+ "</root>", output);
	}	
	
	
	@Test
	public void testTemperatureComplexJSONtoJSON1() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/sourceTemperatureComplex.json");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/sourceTemperatureComplex-mapping1.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "json", mappings, "json");		
		System.out.println(output);
		assertEquals("{\"persons\":[{\"fullName\":\"Test, Test\"},{\"fullName\":\"Test2, Test2\"}]}", output);
	}	
	
	@Test
	public void testDataCiteXmlToJSON() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/datacite-example-full-v4.1.xml");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/datacite-mapping1.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "xml", mappings, "json");		
		System.out.println(output);			
	}

	
	@Test
	public void testCollection() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/missing-element.xml");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/collectiontest.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		String output = service.transform(IOUtils.toString(input), "xml", mappings, "xml");		
		System.out.println(output);
		assertEquals("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
				+ "<root>\n"
				+ "  <id1>id1</id1>\n"
				+ "  <fundingReferences>\n"
				+ "    <name>test1</name>\n"
				+ "    <address>street3</address>\n"
				+ "  </fundingReferences>\n"
				+ "  <fundingReferences>\n"
				+ "    <name>test2</name>\n"
				+ "    <alt>alt2</alt>\n"
				+ "    <address>street</address>\n"
				+ "    <address>street2</address>\n"
				+ "  </fundingReferences>\n"
				+ "  <id2>id2</id2>\n"
				+ "</root>", output);
	}
	

	
}
