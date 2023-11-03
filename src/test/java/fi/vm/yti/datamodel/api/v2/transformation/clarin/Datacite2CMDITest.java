package fi.vm.yti.datamodel.api.v2.transformation.clarin;

import static org.junit.jupiter.api.Assertions.*;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.xmlunit.assertj.XmlAssert;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

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
class Datacite2CMDITest {
	
	private Map<String, String> namespaces = Map.of(
			"", "http://www.clarin.eu/cmd/1/profiles/clarin.eu:cr1:p_1610707853541",
			"cmd", "http://www.clarin.eu/cmd/1",				
	         "cmdp", "http://www.clarin.eu/cmd/1/profiles/clarin.eu:cr1:p_1610707853541",
	         "xsi", "http://www.w3.org/2001/XMLSchema-instance",
	         "datacite_cmd", "http://www.clarin.eu/cmd/conversion/ddi/cmd",			
			"xml", "http://www.w3.org/XML/1998/namespace");		
	
	private String rootElement = "cmd:CMD";
	private String targetTemplate = 
			"<cmd:CMD xmlns=\"http://www.clarin.eu/cmd/1/profiles/clarin.eu:cr1:p_1610707853541\"\n"
			+ "         xmlns:cmd=\"http://www.clarin.eu/cmd/1\"\n"
			+ "         xmlns:cmdp=\"http://www.clarin.eu/cmd/1/profiles/clarin.eu:cr1:p_1610707853541\"\n"
			+ "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
			+ "         xmlns:datacite_cmd=\"http://www.clarin.eu/cmd/conversion/ddi/cmd\"\n"
			+ "         CMDVersion=\"1.2\"\n"
			+ "         xsi:schemaLocation=\"http://www.clarin.eu/cmd/1 https://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/1.x/profiles/clarin.eu:cr1:p_1610707853541/xsd\">\n"
			+ "			%s"
			+ "</cmd:CMD>";
	@Autowired
	private DataTransformationService service;
	
	private List<MappingDTO> getCrosswalk(String path) throws Exception {
		ObjectMapper mapper = new ObjectMapper();

		InputStream sharedMappingsInput = getClass().getClassLoader().getResourceAsStream("fno/clarin/datacite/shared-mappings.json");
		List<MappingDTO> sharedMappings = mapper.readValue(sharedMappingsInput, new TypeReference<List<MappingDTO>>(){});

		
		InputStream mapping = getClass().getClassLoader().getResourceAsStream(path);
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		mappings.addAll(sharedMappings);
		
		return mappings;		
	}
	
	private String getSourceDoc(String path) throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream(path);
		return IOUtils.toString(input);
	}

	private String doTransformation(String sourcePath, String mappingPath) throws Exception {
		String sourceDoc = getSourceDoc(sourcePath);
		String r = service.transform(sourceDoc, "xml", getCrosswalk(mappingPath), "xml", namespaces, rootElement);		
		return r;
	}

	@Test
	public void testDOIDocument() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/sources/doi_10_24416_uu01_2so9te.xml");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/clarin/datacite/datacite-cdmi-mapping.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		Map<String, String> namespaces = Map.of(
				"", "http://www.clarin.eu/cmd/1/profiles/clarin.eu:cr1:p_161070785354",
				"cmd", "http://www.clarin.eu/cmd/1",				
				"xml", "http://www.w3.org/XML/1998/namespace");		
		
		String output = service.transform(IOUtils.toString(input), "xml", mappings, "xml", namespaces, "CMD");		
		System.out.println(output);
	}
	
	@Test
	public void testZenodoDocument() throws Exception {
		InputStream input = getClass().getClassLoader().getResourceAsStream("fno/sources/datacite-zenodo-document.xml");
		InputStream mapping = getClass().getClassLoader().getResourceAsStream("fno/clarin/datacite/datacite-cdmi-mapping.json");
		ObjectMapper mapper = new ObjectMapper();
		List<MappingDTO> mappings = mapper.readValue(mapping, new TypeReference<List<MappingDTO>>(){});
		
		Map<String, String> namespaces = Map.of(
				"", "http://www.clarin.eu/cmd/1/profiles/clarin.eu:cr1:p_161070785354",
				"cmd", "http://www.clarin.eu/cmd/1",				
				"xml", "http://www.w3.org/XML/1998/namespace");		
		
		String output = service.transform(IOUtils.toString(input), "xml", mappings, "xml", namespaces, "CMD");		
		System.out.println(output);
	}
	
	
	@Test
	void testHeader() throws Exception {
		// no source document, static content
		String sourcePath = "fno/sources/doi_10_24416_uu01_2so9te.xml";
		String mappingPath = "fno/clarin/datacite/header-mapping.json";		
		String targetDoc = 			  
				"<cmd:Header>\n"
				+ "      <cmd:MdProfile>clarin.eu:cr1:p_1610707853541</cmd:MdProfile>\n"
				+ "</cmd:Header>";
		
		String r = doTransformation(sourcePath, mappingPath);
		System.out.println(r);
		String target = String.format(targetTemplate, targetDoc);		
		XmlAssert.assertThat(r).and(target).ignoreWhitespace().areIdentical();		
	}	
	
	@Test
	void testResources() throws Exception {
		String sourcePath = "fno/sources/doi_10_24416_uu01_2so9te.xml";
		String mappingPath = "fno/clarin/datacite/resource-mapping.json";		
		String targetDoc = 			  
"""
   <cmd:Resources>
      <cmd:ResourceProxyList>
         <cmd:ResourceProxy id="d1e3">
            <cmd:ResourceType>Resource</cmd:ResourceType>
            <cmd:ResourceRef>https://doi.org/10.24416/UU01-2SO9TE</cmd:ResourceRef>
         </cmd:ResourceProxy>
      </cmd:ResourceProxyList>
      <cmd:JournalFileProxyList/>
      <cmd:ResourceRelationList/>
   </cmd:Resources>				
""";
				
		
		String r = doTransformation(sourcePath, mappingPath);
		System.out.println(r);
		String target = String.format(targetTemplate, targetDoc);		
		XmlAssert.assertThat(r).and(target).ignoreWhitespace().areIdentical();		
	}
	
	@Test
	void testIdentificationInfo() throws Exception {
		String sourcePath = "fno/sources/doi_10_24416_uu01_2so9te.xml";
		String mappingPath = "fno/clarin/datacite/dataciterecord-IdentificationInfo.json";		
		String targetDoc = 			  
"""
   <cmd:Components>
      <DataCiteRecord>
         <IdentificationInfo>
            <identifier type="DOI">https://doi.org/10.24416/UU01-2SO9TE</identifier>
         </IdentificationInfo>
      </DataCiteRecord>
  </cmd:Components>
      			
""";
				
		
		String r = doTransformation(sourcePath, mappingPath);
		System.out.println(r);
		String target = String.format(targetTemplate, targetDoc);		
		XmlAssert.assertThat(r).and(target).ignoreWhitespace().areIdentical();		
	}	
	/*
	@Test
	void testCreator() throws Exception {
		String sourcePath = "fno/sources/doi_10_24416_uu01_2so9te.xml";
		String mappingPath = "fno/clarin/datacite/dataciterecord-Creator.json";		
		String targetDoc = 			  
"""
   <cmd:Components>
      <DataCiteRecord>
         <Creator>
            <identifier>0000-0002-1166-1424</identifier>
            <label>Struiksma, Marijn</label>
            <AgentInfo>
               <PersonInfo>
                  <name>Struiksma, Marijn</name>
                  <alternativeName type="familyName">Struiksma</alternativeName>
                  <affiliation>Utrecht University</affiliation>
               </PersonInfo>
            </AgentInfo>
         </Creator>
         <Creator>
            <identifier>0000-0002-2384-9504</identifier>
            <label xml:lang="en">'t Hart, Björn</label>
            <AgentInfo>
               <PersonInfo>
                  <name>'t Hart, Björn</name>
                  <alternativeName type="givenName">Björn</alternativeName>
                  <affiliation>Utrecht University2</affiliation>
               </PersonInfo>
            </AgentInfo>
         </Creator>
         <Creator>
            <identifier>0000-0003-1673-4845</identifier>
            <label>van Berkum, Jos</label>
            <AgentInfo>
               <PersonInfo>
                  <name>van Berkum, Jos</name>
                  <alternativeName type="givenName">van Berkum</alternativeName>
                  <alternativeName type="familyName">Jos</alternativeName>
                  <affiliation>Utrecht University</affiliation>
               </PersonInfo>
            </AgentInfo>
         </Creator>
      </DataCiteRecord>
  </cmd:Components>
      			
""";
				
		
		String r = doTransformation(sourcePath, mappingPath);
		System.out.println(r);
		String target = String.format(targetTemplate, targetDoc);		
		XmlAssert.assertThat(r).and(target).ignoreWhitespace().areIdentical();		
	}
	*/		
}
