package fi.vm.yti.datamodel.api.v2.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersionDetector;
import com.networknt.schema.ValidationMessage;

public class JSONValidationService {

	private static Map<String, String> schemaURIandPathMap = Map.of("http://json-schema.org/draft-04/schema#",
			"/schema_v4_mscr");

	public static ValidationRecord validateJSONSchema(byte[] inputSchemaFile) throws Exception, IOException {
		ObjectMapper mapper = new ObjectMapper();

		JsonNode inputSchemaNode = mapper.readTree(inputSchemaFile);

		
		if(!inputSchemaNode.has("$schema")) {
			throw new Exception("Missing $schema property. Cannot validate schema.");
		}
		String inputSchemaVersion = inputSchemaNode.get("$schema").asText();

		if (schemaURIandPathMap.containsKey(inputSchemaVersion)) {

			InputStream metaSchemaFile = JSONValidationService.class
					.getResourceAsStream(schemaURIandPathMap.get(inputSchemaVersion));
			JsonNode metaSchemaNode = mapper.readTree(metaSchemaFile);
			JsonSchemaFactory schemaFactory = JsonSchemaFactory.getInstance(SpecVersionDetector.detect(metaSchemaNode));

			JsonSchema schema = schemaFactory.getSchema(metaSchemaNode);
			Set<ValidationMessage> validationResult = schema.validate(inputSchemaNode);
			
			if (validationResult.isEmpty()) {
				return new ValidationRecord(true, Arrays.asList());
			} else {
				List<String> errorMessages = new ArrayList<String>();
				validationResult.forEach(vm -> errorMessages.add(vm.getMessage()));
				return new ValidationRecord(false, errorMessages);
			}

		} else {
			throw new Exception(
					String.format("Validation failed. JSON schema %s is not supported. Supported schemas are: %s",
							inputSchemaVersion, String.join("\n", schemaURIandPathMap.keySet())));
		}
	}

}
