package fi.vm.yti.datamodel.api.v2.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertLinesMatch;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

	private byte[] byteStreamFromPath(String schemaPath) throws Exception, IOException {
		InputStream inputSchemaInputStream = getClass().getClassLoader().getResourceAsStream(schemaPath);
		byte[] inputSchemaInByte = inputSchemaInputStream.readAllBytes();
		inputSchemaInputStream.close();

		return inputSchemaInByte;
	}

	@Test
	void testValidJSONSchema() throws Exception, IOException {
		String validInputSchemaPath = "jsonschema/test_jsonschema_valid_schema.json";
		ValidationRecord vr = JSONValidationService.validateJSONSchema(byteStreamFromPath(validInputSchemaPath));
		
		assertTrue(vr.isValid());

	}

	@Test
	void testInvalidJSONSchema() throws Exception, IOException {
		String firstInvalidInputSchemaPath = "jsonschema/test_jsonschema_invalid_types.json";
		assertFalse(
				JSONValidationService.validateJSONSchema(byteStreamFromPath(firstInvalidInputSchemaPath)).isValid());
	}

	@Test
	void testInvalidJSONSchemaDetailsType() throws Exception, IOException {
		String firstInvalidInputSchemaPath = "jsonschema/test_jsonschema_invalid_types.json";
		List<String> expectedErrorMessages = Arrays.asList(
				"$.properties.creators.type: does not have a value in the enumeration [array, boolean, integer, null, number, object, string]",
				"$.properties.creators.type: string found, array expected");
		ValidationRecord vr = JSONValidationService.validateJSONSchema(byteStreamFromPath(firstInvalidInputSchemaPath)); 
		assertLinesMatch(
				expectedErrorMessages,
				vr.validationOutput()
				);
	}

	@Test
	void testInvalidJSONSchemaDetailsSchema() throws Exception, IOException {
		String secondInvalidInputSchemaPath = "jsonschema/test_jsonschema_invalid_schema.json";
		String expectedErrorMessage = "Validation failed. JSON schema http://json-fsdfs.org/draft-04/schema# is not supported. Supported schemas are: http://json-schema.org/draft-04/schema#";

		Throwable exception = Assertions.assertThrows(Exception.class, () -> {
			JSONValidationService.validateJSONSchema(byteStreamFromPath(secondInvalidInputSchemaPath));
		});

		assertEquals(expectedErrorMessage, exception.getMessage());
	}
	
	@Test
	void testMissingSchema() {
		String inputSchemaPath = "jsonschema/test_jsonschema_missing_schema.json";
		
		Throwable exception = Assertions.assertThrows(Exception.class, () -> {
			JSONValidationService.validateJSONSchema(byteStreamFromPath(inputSchemaPath));
		});
		assertTrue(exception.getMessage().contains("Missing"));		

	}
	
	@Test
	void testUnsupportedFeature() throws Exception {
		String inputSchemaPath = "jsonschema/test_jsonschema_unsupported_features.json";
		ValidationRecord vr = JSONValidationService.validateJSONSchema(byteStreamFromPath(inputSchemaPath));
		assertFalse(vr.isValid());
		
	}
	
	@Test
	void testInvalidDatatype() throws Exception {
		String inputSchemaPath = "jsonschema/test_jsonschema_invalid_datatype.json";
		ValidationRecord vr = JSONValidationService.validateJSONSchema(byteStreamFromPath(inputSchemaPath));
		assertFalse(vr.isValid());
		
	}	
}