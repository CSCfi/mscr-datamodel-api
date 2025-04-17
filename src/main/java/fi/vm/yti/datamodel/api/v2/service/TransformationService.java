package fi.vm.yti.datamodel.api.v2.service;

import java.io.StringWriter;
import java.util.Set;

import org.apache.jena.rdf.model.Model;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ResponseStatusException;

import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.transformation.RMLGenerator2;
import fi.vm.yti.datamodel.api.v2.transformation.XSLTGenerator;
import fi.vm.yti.datamodel.api.v2.transformation.XSLTGenerator2;

@Service
public class TransformationService {
	private final JenaService jenaService;
	private final RMLGenerator2 rmlGenerator;
	private final XSLTGenerator2 xsltGenerator;
	private final XSLTGenerator xsltGenerator1;
	private final WebClient webClient;

	
	@Value("${transformation.xslt.url}")
	private String xsltTransformationServiceUrl;

	@Value("${transformation.rml.url}")
	private String rmlTransformationServiceUrl;

	public TransformationService(
			JenaService jenaService,
			RMLGenerator2 rmlGenerator,
			XSLTGenerator2 xsltGenerator,
			XSLTGenerator xsltGenerator1,
			WebClient.Builder webClientBuilder

			) {
		this.jenaService = jenaService;
		this.rmlGenerator = rmlGenerator;
		this.xsltGenerator = xsltGenerator;
		this.xsltGenerator1 = xsltGenerator1;
		this.webClient = webClientBuilder.build();

	}
	public ResponseEntity transformInternal(String crosswalkInternalID, byte[] data, String sourceSchema, String sourceFormat, String targetSchema, String targetFormat) throws Exception {
		Model crosswalkModel = jenaService.getCrosswalkContent(crosswalkInternalID);
		crosswalkModel.add(jenaService.getCrosswalk(crosswalkInternalID));
		
		Model sourceSchemaModel = jenaService.getSchemaContent(sourceSchema);
		sourceSchemaModel.add(jenaService.getSchema(sourceSchema));
		Model targetSchemaModel = jenaService.getSchemaContent(targetSchema);
		
		return transformInternal(crosswalkInternalID, crosswalkModel, data, sourceSchema, sourceSchemaModel, sourceFormat, targetSchema, targetSchemaModel, targetFormat);
	}
	
	/**
	 * Crosswalk and sourceschemaModel must contain both metadata and content graphs.
	 * 
	 * 
	 * @param crosswalkModel
	 * @param data
	 * @param sourceSchema
	 * @param sourceFormat
	 * @param targetSchema
	 * @param targetFormat
	 * @return
	 * @throws Exception
	 */
	public ResponseEntity transformInternal(String crosswalkInternalID, Model crosswalkModel, byte[] data, String sourceSchema, Model sourceSchemaModel, String sourceFormat, String targetSchema, Model targetSchemaModel, String targetFormat) throws Exception {
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
		
		
		MultiValueMap<String, String> formData = new LinkedMultiValueMap<String, String>();
		formData.add("outputMethod", outputMethod);
		
		// forward to appropriate backend service and return results
		switch (exportFormat) {
		case "rml": {
			formData.add("inputData", new String(data, "UTF-8"));

			
			crosswalkModel.add(sourceSchemaModel);
			crosswalkModel.add(targetSchemaModel);
			
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
				xslt = xsltGenerator.generateXMLtoXML(sourceSchema, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			}
			if((sourceFormat.equals(SchemaFormat.JSONSCHEMA.name())) && (targetFormat.equals(SchemaFormat.XSD.name()))) {
				xslt = xsltGenerator.generateJSONtoXML(sourceSchema, sourceSchemaModel, crosswalkModel, targetSchemaModel);
				inputDoc = "<data><![CDATA[" + inputDoc + "]]></data>";
			}
			if((sourceFormat.equals(SchemaFormat.JSONSCHEMA.name())) && (targetFormat.equals(SchemaFormat.JSONSCHEMA.name()))) {
				inputDoc = "<data><![CDATA[" + inputDoc + "]]></data>";

				xslt = xsltGenerator.generateJSONtoJSON(sourceSchema, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			}
			if((sourceFormat.equals(SchemaFormat.XSD.name())) && (targetFormat.equals(SchemaFormat.JSONSCHEMA.name()))) {
				xslt = xsltGenerator.generateXMLtoJSON(sourceSchema, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			}
			/*
			if((sourceFormat.equals(SchemaFormat.XSD.name())) && (sourceFormat.equals(SchemaFormat.CSV.name()))) {
				xslt = xsltGenerator1.generateXMLtoCSV(mappings, crosswalkModel);
			}
			if((sourceFormat.equals(SchemaFormat.JSONSCHEMA.name())) && (sourceFormat.equals(SchemaFormat.CSV.name()))) {
				xslt = xsltGenerator1.generateXMLtoCSV(mappings, crosswalkModel);
			}
			*/
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
