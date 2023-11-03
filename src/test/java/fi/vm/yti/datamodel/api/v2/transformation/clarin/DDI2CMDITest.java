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
class DDI2CMDITest {
	
	private Map<String, String> namespaces = Map.of(
			"", "http://www.clarin.eu/cmd/1/profiles/clarin.eu:cr1:p_1595321762428",
			"cmd", "http://www.clarin.eu/cmd/1",				
	         "xsi", "http://www.w3.org/2001/XMLSchema-instance",
			"xml", "http://www.w3.org/XML/1998/namespace");		
	
	private String rootElement = "cmd:CMD";
	private String targetTemplate = 
			"<cmd:CMD xmlns=\"http://www.clarin.eu/cmd/1/profiles/clarin.eu:cr1:p_1595321762428\"\n"
			+ "         xmlns:cmd=\"http://www.clarin.eu/cmd/1\"\n"
			+ "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
			+ "         CMDVersion=\"1.2\"\n"
			+ "         xsi:schemaLocation=\"http://www.clarin.eu/cmd/1 https://catalog.clarin.eu/ds/ComponentRegistry/rest/registry/1.x/profiles/clarin.eu:cr1:p_1595321762428/xsd\">\n"
			+ "			%s"
			+ "</cmd:CMD>";
	@Autowired
	private DataTransformationService service;
	
	private List<MappingDTO> getCrosswalk(String path) throws Exception {
		ObjectMapper mapper = new ObjectMapper();

		InputStream sharedMappingsInput = getClass().getClassLoader().getResourceAsStream("fno/clarin/ddi/shared-mappings.json");
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
	void testHeader() throws Exception {
		String sourcePath = "fno/sources/razjed10-en.xml";
		String mappingPath = "fno/clarin/ddi/header.json";		
		String targetDoc = 			  
"""
    <cmd:Header>
        <cmd:MdCreator>Adam, Frane</cmd:MdCreator>
        <cmd:MdCreator>Sanja Lužar, ADP</cmd:MdCreator>
		<cmd:MdCreationDate>2010-05-01</cmd:MdCreationDate>
        <cmd:MdProfile>clarin.eu:cr1:p_1595321762428</cmd:MdProfile>
    </cmd:Header>				
""";
				
		
		String r = doTransformation(sourcePath, mappingPath);
		System.out.println(r);
		String target = String.format(targetTemplate, targetDoc);		
		XmlAssert.assertThat(r).and(target).ignoreWhitespace().areIdentical();		
	}
	
	@Test
	void testResourceProxyList() throws Exception {
		String sourcePath = "fno/sources/razjed10-en.xml";
		String mappingPath = "fno/clarin/ddi/resourceProxyList.json";		
		
		// TODO: generate ID func
		String targetDoc = 			  
"""
<cmd:Resources>				
        <cmd:ResourceProxyList>
            <!-- /codeBook/stdyDscr/dataAccs/setAvail/accsPlac -->
            <cmd:ResourceProxy>
                <cmd:ResourceType>LandingPage</cmd:ResourceType>
                <cmd:ResourceRef>https://doi.org/10.17898/ADP_RAZJED10_V1</cmd:ResourceRef>
            </cmd:ResourceProxy>
            <!-- /codeBook/fileDscr/@ID -->
            <cmd:ResourceProxy id="F2">
                <cmd:ResourceType>Resource</cmd:ResourceType>
                <!-- /codeBook/fileDscr/@URI -->
                <cmd:ResourceRef>https://www.adp.fdv.uni-lj.si/media/podatki/razjed10/razjed10-go-i.pdf</cmd:ResourceRef>
            </cmd:ResourceProxy>
            <!-- /codeBook/fileDscr/@ID -->
            <cmd:ResourceProxy id="F3">
                <cmd:ResourceType>Resource</cmd:ResourceType>
                <!-- /codeBook/fileDscr/@URI -->
                <cmd:ResourceRef>https://www.adp.fdv.uni-lj.si/media/podatki/razjed10/razjed10-jv-i.pdf</cmd:ResourceRef>
            </cmd:ResourceProxy>
            <!-- /codeBook/fileDscr/@ID -->
            <cmd:ResourceProxy id="F4">
                <cmd:ResourceType>Resource</cmd:ResourceType>
                <!-- /codeBook/fileDscr/@URI -->
                <cmd:ResourceRef>https://www.adp.fdv.uni-lj.si/media/podatki/razjed10/razjed10-ok-i.pdf</cmd:ResourceRef>
            </cmd:ResourceProxy>
            <!-- /codeBook/fileDscr/@ID -->
            <cmd:ResourceProxy id="F5">
                <cmd:ResourceType>Resource</cmd:ResourceType>
                <!-- /codeBook/fileDscr/@URI -->
                <cmd:ResourceRef>https://www.adp.fdv.uni-lj.si/media/podatki/razjed10/razjed10-os-i.pdf</cmd:ResourceRef>
            </cmd:ResourceProxy>
            <!-- /codeBook/fileDscr/@ID -->
            <cmd:ResourceProxy id="F1">
                <cmd:ResourceType>Resource</cmd:ResourceType>
                <!-- /codeBook/fileDscr/@URI -->
                <cmd:ResourceRef>https://www.adp.fdv.uni-lj.si/media/podatki/razjed10/razjed10-po-i.pdf</cmd:ResourceRef>
            </cmd:ResourceProxy>
        </cmd:ResourceProxyList>			
</cmd:Resources>
""";
				
		
		String r = doTransformation(sourcePath, mappingPath);
//		System.out.println(r);
		String target = String.format(targetTemplate, targetDoc);		
		XmlAssert.assertThat(r).and(target).ignoreWhitespace().ignoreComments().areIdentical();		
		
	}
	@Test
	void testIdentificationInfoInfo() throws Exception {
		String sourcePath = "fno/sources/razjed10-en.xml";
		String mappingPath = "fno/clarin/ddi/codebook-IdentificationInfo.json";		
		
		// TODO: generate ID func
		String targetDoc = 			  
"""
    <cmd:Components>
        <DDICodebook>
            <IdentificationInfo>
                <!-- /codeBook/stdyDscr/citation/titlStmt/IDNo -->
                <identifier>https://doi.org/10.17898/ADP_RAZJED10_V1</identifier>
                <!-- /codeBook/stdyDscr/citation/titlStmt/IDNo -->
                <internalIdentifier>RAZJED10</internalIdentifier>
            </IdentificationInfo>
        </DDICodebook>
    </cmd:Components>
""";
				
		
		String r = doTransformation(sourcePath, mappingPath);
		System.out.println(r);
		String target = String.format(targetTemplate, targetDoc);		
		XmlAssert.assertThat(r).and(target).ignoreWhitespace().ignoreComments().areIdentical();		
		
	}
	
	@Test
	void testTitleInfoInfo() throws Exception {
		String sourcePath = "fno/sources/razjed10-en.xml";
		String mappingPath = "fno/clarin/ddi/codebook-TitleInfo.json";		
		
		// TODO: generate ID func
		String targetDoc = 			  
"""
    <cmd:Components>
        <DDICodebook>
            <TitleInfo>
                <!-- /codeBook/stdyDscr/citation/titlStmt/titl -->
                <title xml:lang="en-GB">Local and regional developmental cores</title>
                <!-- /codeBook/stdyDscr/citation/titlStmt/parTitl -->
                <title xml:lang="sl-SI">Lokalna in regionalna razvojna jedra</title>
            </TitleInfo>
        </DDICodebook>
    </cmd:Components>
""";
				
		
		String r = doTransformation(sourcePath, mappingPath);
		System.out.println(r);
		String target = String.format(targetTemplate, targetDoc);		
		XmlAssert.assertThat(r).and(target).ignoreWhitespace().ignoreComments().areIdentical();		
		
	}
	
	@Test
	void testResourceType() throws Exception {
		String sourcePath = "fno/sources/razjed10-en.xml";
		String mappingPath = "fno/clarin/ddi/codebook-ResourceType.json";		
		
		// TODO: generate ID func
		String targetDoc = 			  
"""
    <cmd:Components>
        <DDICodebook>
            <ResourceType>
                <!-- /codeBook/stdyDscr/stdyInfo/sumDscr/dataKind -->
                <identifier>urn:ddi-cv:GeneralDataFormat:2.0:Text</identifier>
                <label xml:lang="en-GB">Text</label>
            </ResourceType>
        </DDICodebook>
    </cmd:Components>
""";
				
		
		String r = doTransformation(sourcePath, mappingPath);
		System.out.println(r);
		String target = String.format(targetTemplate, targetDoc);		
		XmlAssert.assertThat(r).and(target).ignoreWhitespace().ignoreComments().areIdentical();		
		
	}	
	
	
	@Test
	void testCreator() throws Exception {
		String sourcePath = "fno/sources/razjed10-en.xml";
		String mappingPath = "fno/clarin/ddi/codebook-Creator.json";		
		
		// TODO: generate ID func
		String targetDoc = 			  
"""
    <cmd:Components>
        <DDICodebook>				
            <Creator>
                <!-- /codeBook/stdyDscr/citation/rspStmt/AuthEnty -->
                <label>Adam, Frane</label>
                <AgentInfo>
                    <PersonInfo>
                        <!-- /codeBook/stdyDscr/citation/rspStmt/AuthEnty -->
                        <name>Adam, Frane</name>
                        <!-- ./@affiliation -->
                        <affiliation>The institute for developmental and strategic analysis</affiliation>
                    </PersonInfo>
                </AgentInfo>
            </Creator>
        </DDICodebook>
    </cmd:Components>            
""";
				
		
		String r = doTransformation(sourcePath, mappingPath);
		System.out.println(r);
		String target = String.format(targetTemplate, targetDoc);		
		XmlAssert.assertThat(r).and(target).ignoreWhitespace().ignoreComments().areIdentical();		
		
	}	
	
	@Test
	void testContributor() throws Exception {
		String sourcePath = "fno/sources/razjed10-en.xml";
		String mappingPath = "fno/clarin/ddi/codebook-Contributor.json";		
		
		// TODO: generate ID func
		String targetDoc = 			  
"""
    <cmd:Components>
        <DDICodebook>				
            <Contributor>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/p -->
                <label>Kristan, Primož</label>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/@role -->
                <role>conceptualization collaborator</role>
            </Contributor>
            <Contributor>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/p -->
                <label>Vojvodić, Ana</label>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/@role -->
                <role>conceptualization collaborator</role>
            </Contributor>
            <Contributor>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/p -->
                <label>Rončević, Borut</label>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/@role -->
                <role>conceptualization collaborator</role>
            </Contributor>
            <Contributor>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/p -->
                <label>Kalčić, Špela</label>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/@role -->
                <role>conceptualization collaborator</role>
            </Contributor>
            <Contributor>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/p -->
                <label>Podmenik, Dane</label>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/@role -->
                <role>conceptualization collaborator</role>
            </Contributor>
            <Contributor>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/p -->
                <label>Šinkovec, Urša</label>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/@role -->
                <role>conceptualization collaborator</role>
            </Contributor>
            <Contributor>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/p -->
                <label>Besednjak, Tamara</label>
                <!-- /codeBook/stdyDscr/citation/rspStmt/othId/@role -->
                <role>conceptualization collaborator</role>
            </Contributor>
        </DDICodebook>
    </cmd:Components>            
""";

		String r = doTransformation(sourcePath, mappingPath);
		System.out.println(r);
		String target = String.format(targetTemplate, targetDoc);		
		XmlAssert.assertThat(r).and(target).ignoreWhitespace().ignoreComments().areIdentical();		
		
	}	
}
