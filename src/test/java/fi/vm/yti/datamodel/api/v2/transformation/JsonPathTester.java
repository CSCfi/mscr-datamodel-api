package fi.vm.yti.datamodel.api.v2.transformation;

import org.junit.jupiter.api.Test;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

public class JsonPathTester {

	@Test
	public void test() {
		String c = "{\"root\":[{\"temperature\":\" 20\",\"id\":\"1\",\"timestamp\":\" 1696330740\"},{\"temperature\":\" 32\",\"id\":\"3\",\"timestamp\":\" 1696398906439\"}]}";
		
		DocumentContext source = JsonPath.parse(c);
		var r = source.read("$.root[*].id");
		System.out.println(r);
	}
}
