package fi.vm.yti.datamodel.api.v2.endpoint;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Set;

import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.io.FileUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.xerces.xs.XSModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ResponseStatusException;
import org.topbraid.shacl.vocabulary.SH;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;

import fi.vm.yti.datamodel.api.v2.dto.CrosswalkInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.mapper.CrosswalkMapper;
import fi.vm.yti.datamodel.api.v2.mapper.SchemaMapper;
import fi.vm.yti.datamodel.api.v2.service.CrosswalkService;
import fi.vm.yti.datamodel.api.v2.service.GroupManagementService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.PIDService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFile;
import fi.vm.yti.datamodel.api.v2.transformation.RMLGenerator2;
import fi.vm.yti.datamodel.api.v2.transformation.XSLTGenerator2;
import fi.vm.yti.security.AuthenticatedUserProvider;
import fi.vm.yti.security.YtiUser;
import io.apptik.json.JsonElement;
import io.apptik.json.generator.JsonGenerator;
import io.apptik.json.generator.JsonGeneratorConfig;
import io.apptik.json.schema.SchemaV4;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jlibs.xml.sax.XMLDocument;
import jlibs.xml.xsd.XSInstance;
import jlibs.xml.xsd.XSParser;


@RestController
@RequestMapping("v2")
@Tag(name="Transformation")
public class DataTransformationController {

	private final CrosswalkService crosswalkService;
	private final JenaService jenaService;
	private final CrosswalkMapper mapper;
	private final RMLGenerator2 rmlGenerator;
	private final XSLTGenerator2 xsltGenerator;
	private final WebClient webClient;
	private final GroupManagementService groupManagementService;
	private final AuthenticatedUserProvider userProvider;
	private final PIDService PIDService;
	private final StorageService storageService;
	private final SchemaMapper schemaMapper;

	
	@Value("${transformation.xslt.url}")
	private String xsltTransformationServiceUrl;

	@Value("${transformation.rml.url}")
	private String rmlTransformationServiceUrl;

	
	public DataTransformationController(
			CrosswalkService crosswalkService,
			JenaService jenaService,
			CrosswalkMapper mapper,
			RMLGenerator2 rmlGenerator,
			XSLTGenerator2 xsltGenerator,
			WebClient.Builder webClientBuilder,
			GroupManagementService groupManagementService,
			AuthenticatedUserProvider userProvider,
			PIDService PIDService,
			StorageService storageService,
			SchemaMapper schemaMapper

			) {
		this.crosswalkService = crosswalkService;
		this.jenaService = jenaService;
		this.mapper = mapper;
		this.rmlGenerator = rmlGenerator;
		this.xsltGenerator = xsltGenerator;
		this.webClient = webClientBuilder.build();
		this.groupManagementService = groupManagementService;
		this.userProvider = userProvider;
		this.PIDService = PIDService;
		this.storageService = storageService;
		this.schemaMapper = schemaMapper;
	}

	@SecurityRequirement(name = "Bearer Authentication")
	@GetMapping(value = "/crosswalk/{pid}/test", produces = APPLICATION_JSON_VALUE)
	public ResponseEntity testCrosswalk(@PathVariable(name = "pid") String pid) {
		return testCrosswalk(pid, null);
	}
	
	@Hidden	
	@GetMapping(value = "/crosswalk/{pid}/{suffix}/test", produces = APPLICATION_JSON_VALUE)
	public ResponseEntity testCrosswalk(@PathVariable String pid,
			@PathVariable String suffix) {
		YtiUser user = userProvider.getUser();
		if(user == null || user.isAnonymous()) {
			throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "API token is required for this endpoint.");
		}
		
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}		
		try {
			pid = PIDService.mapToInternal(pid);
			var userMapper = groupManagementService.mapUser();
			var ownerMapper = groupManagementService.mapOwner();

			Model crosswalkMetadataModel = jenaService.getCrosswalk(pid);
			if(crosswalkMetadataModel == null || crosswalkMetadataModel.isEmpty()) {
				throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Crosswalk with internal id " +  pid + " was not found", null);
			}
			CrosswalkInfoDTO metadata = mapper.mapToCrosswalkDTO(pid, crosswalkMetadataModel, false, true, userMapper, ownerMapper);

			ResponseEntity sample = generateSample(metadata.getSourceSchema());
			String sourceFormat = metadata.getSourceSchemaInfo().format();
			String targetFormat = metadata.getTargetSchemaInfo().format();

			return transformInternal(pid, ((String)sample.getBody()).getBytes(), metadata.getSourceSchema(), sourceFormat, metadata.getTargetSchema(), targetFormat);
			
		}catch(Exception ex) {
			ex.printStackTrace();
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage(), ex);
		}		
	}
	
	@SecurityRequirement(name = "Bearer Authentication")
	@GetMapping(value = "/schema/{pid}/sample", produces = APPLICATION_JSON_VALUE)
	public ResponseEntity generateSample(@PathVariable(name = "pid") String pid) {
		return generateSample(pid, null);
	}

	@Hidden	
	@GetMapping(value = "/schema/{pid}/{suffix}/sample", produces = APPLICATION_JSON_VALUE)
	public ResponseEntity generateSample(@PathVariable String pid,
			@PathVariable String suffix) {
		
		YtiUser user = userProvider.getUser();
		if(user == null || user.isAnonymous()) {
			throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "API token is required for this endpoint.");
		}
		
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);
			Model model = jenaService.getSchema(pid);
			var userMapper = groupManagementService.mapUser();
			var ownerMapper = groupManagementService.mapOwner();

			SchemaInfoDTO dto = schemaMapper.mapToSchemaDTO(pid, model, userMapper, ownerMapper);
			List<StoredFile> files = storageService.retrieveAllSchemaFiles(pid);
			StoredFile file = files.get(0); // there is currently always only one file

			if(dto.getFormat() == SchemaFormat.JSONSCHEMA) {
				JsonElement json = JsonElement.readFrom(new String(file.data(), "UTF-8"));
				io.apptik.json.schema.Schema schema = new SchemaV4().wrap(json.asJsonObject());
				JsonGeneratorConfig gConf = new JsonGeneratorConfig();
				gConf.globalArrayItemsMin = 2;
				gConf.globalArrayItemsMax = 5;				
				JsonElement result = new JsonGenerator(schema, gConf).generate();
				return new ResponseEntity(result.toString(), HttpStatusCode.valueOf(200));
				
			}
			else if(dto.getFormat() == SchemaFormat.XSD) {
				// determine the root element
				Model contentModel = jenaService.getSchemaContent(pid);
				Resource root = contentModel.getResource(pid + "#root-Root").getPropertyResourceValue(SH.property).getPropertyResourceValue(SH.node);
								
				String rootElementName = root.getProperty(MSCR.localName).getString();
				
				String namespace = null;
				if(root.hasProperty(MSCR.namespace)) {
					namespace = root.getProperty(MSCR.namespace).getResource().getURI();
				}
				XSInstance xsInstance = new XSInstance();
				xsInstance.minimumElementsGenerated = 2;
				xsInstance.maximumElementsGenerated = 4;
				xsInstance.generateOptionalElements = Boolean.TRUE;
				
				QName rootElement = new QName(namespace, rootElementName);
				File tempFile = File.createTempFile("sample", ".xml");
				FileUtils.writeByteArrayToFile(tempFile, file.data());
				XSModel xsModel = new XSParser().parse(tempFile.getAbsolutePath());
				
				StringWriter writer = new StringWriter();
			    StreamResult result = new StreamResult(writer);
				
				XMLDocument sampleXml = new XMLDocument(result , true, 4, null);
				
				xsInstance.generate(xsModel,rootElement, sampleXml);
				writer.flush();
				String response = writer.toString();
				writer.close();
				
				return new ResponseEntity(response, HttpStatusCode.valueOf(200));
			}
			else if(dto.getFormat() == SchemaFormat.CSV) {
				InputStream input = new ByteArrayInputStream(file.data());
				CSVParser parser = new CSVParserBuilder().withSeparator(';').build();
				CSVReader reader = new CSVReaderBuilder(new InputStreamReader(input)).withCSVParser(parser).build();
								
				String[] columns = reader.readNext();		
				input.close();
				reader.close();
				
				StringWriter strWriter = new StringWriter();
				CSVWriter writer = new CSVWriter(strWriter, ';', CSVWriter.DEFAULT_QUOTE_CHARACTER, CSVWriter.DEFAULT_ESCAPE_CHARACTER, CSVWriter.DEFAULT_LINE_END);
				writer.writeNext(columns);
				String[] line1 = new String[columns.length];
				String[] line2 = new String[columns.length];
				for(int i = 0; i < line1.length; i++) {
					line1[i] = "value-1-" + i;
					line2[i] = "value-2-" + i;
				}				
				writer.writeNext(line1);
				writer.writeNext(line2);
				writer.flush();
				String response = strWriter.toString();
				writer.close();
				strWriter.close();
				return new ResponseEntity(response, HttpStatusCode.valueOf(200));				

			}
			else {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Sample functionality is only available for CSV, XSD and JSONSchema formats");
			}
			
		}catch(Exception ex) {
			ex.printStackTrace();
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage(), ex);
		}
		
		
	}
	
	
	@SecurityRequirement(name = "Bearer Authentication")
	@PostMapping(path = "/transform", produces = "text/plain", consumes = "multipart/form-data")
	public ResponseEntity transform(
			@RequestParam(name = "outputMethod", required = true) String outputMethod,
			@RequestParam(name = "inputFile", required = false) MultipartFile inputFile,
			@RequestParam(name = "crosswalkInternalID", required = true) String crosswalkInternalID) throws Exception {
		
		YtiUser user = userProvider.getUser();
		if(user == null || user.isAnonymous()) {
			throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "API token is required for this endpoint.");
		}
		// get the crosswalk metadata
		Model crosswalkMetadataModel = jenaService.getCrosswalk(crosswalkInternalID);
		if(crosswalkMetadataModel == null || crosswalkMetadataModel.isEmpty()) {
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Crosswalk with internal id " +  crosswalkInternalID + " was not found", null);
		}
		var userMapper = groupManagementService.mapUser();
		var ownerMapper = groupManagementService.mapOwner();

		CrosswalkInfoDTO metadata = mapper.mapToCrosswalkDTO(crosswalkInternalID, crosswalkMetadataModel, false, true, userMapper, ownerMapper);
		// validate transformation and figure out what to generate
		
		String sourceFormat = metadata.getSourceSchemaInfo().format().equals(SchemaFormat.MSCR.name()) ? metadata.getSourceSchemaInfo().originalFormat() : metadata.getSourceSchemaInfo().format();
		String targetFormat = metadata.getTargetSchemaInfo().format().equals(SchemaFormat.MSCR.name()) ? metadata.getTargetSchemaInfo().originalFormat() : metadata.getTargetSchemaInfo().format() ;
		
		return transformInternal(crosswalkInternalID, inputFile.getBytes(), metadata.getSourceSchema(), sourceFormat, metadata.getTargetSchema(), targetFormat);
		
	}
	
	private ResponseEntity transformInternal(String crosswalkInternalID, byte[] data, String sourceSchema, String sourceFormat, String targetSchema, String targetFormat) throws Exception {
		String exportFormat = null;
		String outputMethod = "text";
		if(targetFormat.equals("XSD")) {
			outputMethod = "xml";
		}

		if(Set.of("CSV", "XSD", "JSONSCHEMA").contains(sourceFormat) && targetFormat.equals("SHACL")) {
			exportFormat = "rml";
		}
		else if(Set.of("CSV", "XSD", "JSONSCHEMA").contains(sourceFormat) && Set.of("CSV", "XSD", "JSONSCHEMA").contains(targetFormat)) {
			exportFormat = "xslt";
		}
		else {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Could not transform data with the given crosswalk. Supported transformations are CSV/XSD/JSONSchema -> SHACL and CSV/XSD/JSONSchema -> CSV/XSD/JSONSchema.", null);
		}
		// generate
		Model crosswalkModel = jenaService.getCrosswalkContent(crosswalkInternalID);
		Model sourceSchemaModel = jenaService.getSchemaContent(sourceSchema);
		Model targetSchemaModel = jenaService.getSchemaContent(targetSchema);
		
		MultiValueMap<String, String> formData = new LinkedMultiValueMap<String, String>();
		formData.add("outputMethod", outputMethod);
		
		// forward to appropriate backend service and return results
		switch (exportFormat) {
		case "rml": {
			formData.add("inputData", new String(data, "UTF-8"));

			
			crosswalkModel.add(sourceSchemaModel);
			crosswalkModel.add(targetSchemaModel);
			crosswalkModel.add(jenaService.getCrosswalk(crosswalkInternalID));
			crosswalkModel.add(jenaService.getSchema(sourceSchema));
			
			Model outputModel = rmlGenerator.generateRMLFromMSCRGraph(crosswalkModel, crosswalkInternalID, sourceSchema);
			
			StringWriter writer = new StringWriter();
			outputModel.write(writer, "TURTLE");
			writer.flush();
			String rmlContent = writer.toString();
			formData.add("crosswalkData", rmlContent);
			
			writer.close();
			
			return webClient.post().uri(rmlTransformationServiceUrl)
				.bodyValue(formData)
				.accept(MediaType.ALL)
				.header("Content-Type", "multipart/form-data")
				.retrieve()
				.toEntity(String.class).block();			
		}
		case "xslt": {
			String xslt = null;
			String inputDoc = new String(data, "UTF-8");
			if((sourceFormat.equals(SchemaFormat.XSD.name())) && (targetFormat.equals(SchemaFormat.XSD.name()))) {
				xslt = xsltGenerator.generateXMLtoXML(sourceSchema,jenaService.getSchemaContent(sourceSchema), crosswalkModel, jenaService.getSchemaContent(targetSchema));
			}
			if((sourceFormat.equals(SchemaFormat.XSD.name())) && (targetFormat.equals(SchemaFormat.JSONSCHEMA.name()))) {
				xslt = xsltGenerator.generateXMLtoJSON(sourceSchema,jenaService.getSchemaContent(sourceSchema), crosswalkModel, jenaService.getSchemaContent(targetSchema));
			}			
			if((sourceFormat.equals(SchemaFormat.XSD.name())) && (targetFormat.equals(SchemaFormat.CSV.name()))) {
				xslt = xsltGenerator.generateXMLtoCSV(sourceSchema,jenaService.getSchemaContent(sourceSchema), crosswalkModel, jenaService.getSchemaContent(targetSchema));
			}
			if((sourceFormat.equals(SchemaFormat.JSONSCHEMA.name())) && (targetFormat.equals(SchemaFormat.JSONSCHEMA.name()))) {
				inputDoc = "<data><![CDATA[" + inputDoc + "]]></data>";
				xslt = xsltGenerator.generateJSONtoJSON(sourceSchema,jenaService.getSchemaContent(sourceSchema), crosswalkModel, jenaService.getSchemaContent(targetSchema));
			}	
			if((sourceFormat.equals(SchemaFormat.JSONSCHEMA.name())) && (targetFormat.equals(SchemaFormat.XSD.name()))) {
				inputDoc = "<data><![CDATA[" + inputDoc + "]]></data>";
				xslt = xsltGenerator.generateJSONtoXML(sourceSchema,jenaService.getSchemaContent(sourceSchema), crosswalkModel, jenaService.getSchemaContent(targetSchema));
			}
			if((sourceFormat.equals(SchemaFormat.JSONSCHEMA.name())) && (targetFormat.equals(SchemaFormat.CSV.name()))) {
				inputDoc = "<data><![CDATA[" + inputDoc + "]]></data>";
				xslt = xsltGenerator.generateJSONtoCSV(sourceSchema,jenaService.getSchemaContent(sourceSchema), crosswalkModel, jenaService.getSchemaContent(targetSchema));
			}			
			if((sourceFormat.equals(SchemaFormat.CSV.name())) && (targetFormat.equals(SchemaFormat.CSV.name()))) {
				inputDoc = "<data><![CDATA[" + inputDoc + "]]></data>";				
				xslt = xsltGenerator.generateCSVtoCSV(sourceSchema,jenaService.getSchemaContent(sourceSchema), crosswalkModel, jenaService.getSchemaContent(targetSchema));
			}					
			if((sourceFormat.equals(SchemaFormat.CSV.name())) && (targetFormat.equals(SchemaFormat.XSD.name()))) {
				inputDoc = "<data><![CDATA[" + inputDoc + "]]></data>";				
				xslt = xsltGenerator.generateCSVtoXML(sourceSchema,jenaService.getSchemaContent(sourceSchema), crosswalkModel, jenaService.getSchemaContent(targetSchema));
			}					
			if((sourceFormat.equals(SchemaFormat.CSV.name())) && (targetFormat.equals(SchemaFormat.JSONSCHEMA.name()))) {
				inputDoc = "<data><![CDATA[" + inputDoc + "]]></data>";				
				xslt = xsltGenerator.generateCSVtoJSON(sourceSchema,jenaService.getSchemaContent(sourceSchema), crosswalkModel, jenaService.getSchemaContent(targetSchema));
			}					
			
			if(xslt != null) {
				formData.add("inputData", inputDoc);
				
				formData.add("crosswalkData", xslt);

				return webClient.post().uri(xsltTransformationServiceUrl)
						.bodyValue(formData)
						.accept(MediaType.ALL)
						.header("Content-Type", "multipart/form-data")
						.retrieve()
						.toEntity(String.class).block();
				
			}
			else {
				throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Generated XSLT was null", null);
			}
		}
		default:
			throw new IllegalArgumentException("No export format available");
		}
	}
			
}
