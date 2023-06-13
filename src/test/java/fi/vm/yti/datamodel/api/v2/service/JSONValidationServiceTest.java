package fi.vm.yti.datamodel.api.v2.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)

@Import({ JSONValidationService.class })

public class JSONValidationServiceTest {

	String metaSchemaPath = "schema_v4";
	InputStream metaSchemaInputStream = getClass().getClassLoader().getResourceAsStream(metaSchemaPath);

	private byte[] byteStreamFromPath(String schemaPath) throws Exception, IOException {
		InputStream inputSchemaInputStream = getClass().getClassLoader().getResourceAsStream(schemaPath);
		byte[] inputSchemaInByte = inputSchemaInputStream.readAllBytes();
		inputSchemaInputStream.close();

		return inputSchemaInByte;
	}

	@Test
	void testValidJSONSchema() throws Exception, IOException {
		String validInputSchemaPath = "test_jsonschema_b2share.json";

		assertEquals(JSONValidationService.validateJSONSchema(byteStreamFromPath(validInputSchemaPath)),
				new ValidationRecord(true, Arrays.asList()));

	}

	@Test
	void testInvalidJSONSchema() throws Exception, IOException {
		String firstInvalidInputSchemaPath = "test_jsonschema_invalid_types.json";
		assertFalse(
				JSONValidationService.validateJSONSchema(byteStreamFromPath(firstInvalidInputSchemaPath)).isValid());
	}

	@Test
	void testInvalidJSONSchemaDetailsType() throws Exception, IOException {
		String firstInvalidInputSchemaPath = "test_jsonschema_invalid_types.json";
		List<String> expectedErrorMessages = Arrays.asList(
				"$.properties.creators.type: does not have a value in the enumeration [array, boolean, integer, null, number, object, string]",
				"$.properties.creators.type: string found, array expected");
		assertLinesMatch(JSONValidationService.validateJSONSchema(byteStreamFromPath(firstInvalidInputSchemaPath))
				.validationOutput(), expectedErrorMessages);
	}

	@Test
	void testInvalidJSONSchemaDetailsSchema() throws Exception, IOException {
		String secondInvalidInputSchemaPath = "test_jsonschema_invalid_schema.json";
		String expectedErrorMessage = "Validation failed. JSON schema http://json-fsdfs.org/draft-04/schema# is not supported. Supported schemas are: http://json-schema.org/draft-04/schema#";

		Throwable exception = Assertions.assertThrows(Exception.class, () -> {
			JSONValidationService.validateJSONSchema(byteStreamFromPath(secondInvalidInputSchemaPath));
		});

		assertEquals(expectedErrorMessage, exception.getMessage());
	}
}