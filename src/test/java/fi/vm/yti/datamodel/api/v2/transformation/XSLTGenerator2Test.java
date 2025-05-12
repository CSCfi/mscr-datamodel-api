package fi.vm.yti.datamodel.api.v2.transformation;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

import javax.xml.transform.stream.StreamSource;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.builder.Input;
import org.xmlunit.diff.Diff;

import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.Serializer;
import net.sf.saxon.s9api.Xslt30Transformer;
import net.sf.saxon.s9api.XsltCompiler;
import net.sf.saxon.s9api.XsltExecutable;

class XSLTGenerator2Test {
	
	public static String arraysXMLData = """
<?xml version="1.0" encoding="UTF-8" ?>
 <root>
     <fruits>apple</fruits>
     <fruits>orange</fruits>
     <fruits>pear</fruits>
     <vegetables>
         <veggieName>potato</veggieName>
         <veggieLike>true</veggieLike>
     </vegetables>
     <vegetables>
         <veggieName>broccoli</veggieName>
         <veggieLike>false</veggieLike>
     </vegetables>
 </root>					
			""".trim();
	
	public static String arraysJSONData = """
<data>{
    "vegetables": [
        {
            "veggieLike": "true",
            "veggieName": "potato"
        },
        {
            "veggieLike": "false",
            "veggieName": "broccoli"
        }
    ],
    "fruits": [
        "apple",
        "orange",
        "pear"
    ]
}
</data>						
			""".trim();
	
	public static String personJSONData = """
<data>{
  "firstName": "John",
  "lastName": "Doe",
  "age": 21
}	
</data>				
			""".trim();
	
	public static String person2JSONData = """
<data>{
  "firstName": "John",
  "lastName": "Doe",
  "hobbies": "test,test2",
  "age": 21
}	
</data>				
			""".trim();	
	
	public static String person3XMLData = """
<root>
    <name>Doe, John</name>
    <age>21</age>
    <hobbies>
        <hobby>archery</hobby>
        <hobby>fencing</hobby>
    </hobbies>
</root>			
			""".trim();		
	
	private String getStringFromPath(String schemaPath) throws Exception, IOException {
		InputStream inputSchemaInputStream = getClass().getClassLoader().getResourceAsStream(schemaPath);
		byte[] inputSchemaInByte = inputSchemaInputStream.readAllBytes();
		inputSchemaInputStream.close();

		return new String(inputSchemaInByte);
	}
	private String transform(String inputData, String xslt, String outputMethod) throws Exception {
		System.setProperty("javax.xml.transform.TransformerFactory", "net.sf.saxon.TransformerFactoryImpl");
		
		Writer writer = new StringWriter();
		Reader inputReader = new StringReader(inputData);
		Reader crosswalkReader = new StringReader(xslt);
		
		Processor processor = new Processor(false);
		XsltCompiler compiler = processor.newXsltCompiler();
		XsltExecutable stylesheet = compiler.compile(new StreamSource(crosswalkReader));
		Serializer out = processor.newSerializer(writer);
		out.setOutputProperty(Serializer.Property.METHOD, outputMethod);
		out.setOutputProperty(Serializer.Property.INDENT, "yes");
		Xslt30Transformer transformer = stylesheet.load30();
		transformer.transform(new StreamSource(inputReader), out);
		writer.flush();
		return writer.toString();		
	}

	@Test
	void testPersonSimpleJSONtoXML() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:5afc2153-ad54-440c-a16f-0854bd77285b";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/person-json2xml-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/person-json.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/person-xml.ttl") ;
		
		try {
			String xslt = g.generateJSONtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);

			String result = transform(personJSONData, xslt, "xml");
			String expectedResult = """
<?xml version="1.0" encoding="UTF-8" ?>
 <root>
     <firstName>John</firstName>
     <lastName>Doe</lastName>
     <age>21</age>
 </root>					
										""";
			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			assertFalse(d.hasDifferences());						

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		

	}
	
	@Test
	void testPersonSplitJSONtoXML() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:5afc2153-ad54-440c-a16f-0854bd77285b";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/person-split-json2xml-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/person3-json.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/person3-xml.ttl") ;
		
		try {
			String xslt = g.generateJSONtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String result = transform(person2JSONData, xslt, "xml");			
			String expectedResult = """
<?xml version="1.0" encoding="UTF-8" ?>
 <root>
     <name>Doe, John</name>
     <hobbies>
        <hobby>test</hobby>
        <hobby>test2</hobby>
    </hobbies>
 </root>					
										""";
			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			assertFalse(d.hasDifferences());						

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		

	}	
	
	@Test
	void testPersonSplitXMLtoJSON() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:5afc2153-ad54-440c-a16f-0854bd77285b";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/person-split-xml2.json-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/person3-xml.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/person3-json.ttl") ;
		
		try {
			String xslt = g.generateXMLtoJSON(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String result = transform(person3XMLData, xslt, "text");
			String expectedResult = """
{"firstName":"John","lastName":"Doe", "hobbies": "archery|fencing"}				
										""".trim();
			JSONAssert.assertEquals(expectedResult, result, false);						

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		

	}	
		
	@Test
	void testPersonConcatJSONtoXML() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:5afc2153-ad54-440c-a16f-0854bd77285b";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/person-concat-json2xml-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/person-json.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/person2-xml.ttl") ;
		
		try {
			String xslt = g.generateJSONtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String result = transform(personJSONData, xslt, "xml");
			String expectedResult = """
<?xml version="1.0" encoding="UTF-8" ?>
 <root>
     <name>Doe, John</name>
     <age>21</age>
 </root>					
										""";
			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			assertFalse(d.hasDifferences());						

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		

	}	
	
	@Test
	void testPersonConcatAddPrefixJSONtoXML() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:5afc2153-ad54-440c-a16f-0854bd77285b";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/person-concataddprefix-json2xml-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/person-json.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/person2-xml.ttl") ;
		
		try {
			String xslt = g.generateJSONtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String result = transform(personJSONData, xslt, "xml");
			String expectedResult = """
<?xml version="1.0" encoding="UTF-8" ?>
 <root>
     <name>Doe, John</name>
     <age>21</age>
 </root>					
										""";
			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			assertFalse(d.hasDifferences());						

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		

	}
	
	
	@Test
	void testPersonConcatPrefixSuffixJSONtoXML() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:5afc2153-ad54-440c-a16f-0854bd77285b";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/person-concatprefixsuffix-json2xml-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/person-json.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/person2-xml.ttl") ;
		
		try {
			String xslt = g.generateJSONtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String result = transform(personJSONData, xslt, "xml");
			String expectedResult = """
<?xml version="1.0" encoding="UTF-8" ?>
 <root>
     <name>Doe, John-test</name>
     <age>21</age>
 </root>					
										""";
			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			assertFalse(d.hasDifferences());						

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		

	}		
	@Test
	void testSimpleXMLtoXML() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:5afc2153-ad54-440c-a16f-0854bd77285b";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/arrays-xml2xml-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/arrays-xml.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/arrays-xml-2.ttl") ;
		
		try {
			String xslt = g.generateXMLtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			
			String result = transform(arraysXMLData, xslt, "xml");
			String expectedResult = """
<?xml version="1.0" encoding="UTF-8"?>
<root2>
    <fruits2>apple</fruits2>
    <fruits2>orange</fruits2>
    <fruits2>pear</fruits2>
    <vegetables2>
        <veggieName2>potato</veggieName2>
    </vegetables2>
    <vegetables2>
        <veggieName2>broccoli</veggieName2>
    </vegetables2>
</root2>					
										""";
			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			assertFalse(d.hasDifferences());						

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Test
	void testComplexXMLtoXML() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:5afc2153-ad54-440c-a16f-0854bd77285b";
	
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/arrays-xml-xml-crosswalk-functions.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/arrays-xml.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/arrays-xml-2.ttl") ;
		
		try {
			String xslt = g.generateXMLtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String result = transform(arraysXMLData, xslt, "xml");
			String expectedResult = """
<?xml version="1.0" encoding="UTF-8"?>
<root2>
    <fruits2>apple</fruits2>
    <fruits2>orange</fruits2>
    <fruits2>pear</fruits2>
    <vegetables2>
        <veggieName2>true-prefix-potato-suffix</veggieName2>
        <veggieLike2>potato-test</veggieLike2>
    </vegetables2>
    <vegetables2>
        <veggieName2>false-prefix-broccoli-suffix</veggieName2>
        <veggieLike2>broccoli-test</veggieLike2>
    </vegetables2>
</root2>										
					""";
			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			assertFalse(d.hasDifferences());	

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}	
	
	@Test
	void testSimpleJSONtoXML() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:fd0b6d7a-86b3-45f6-aa90-050164cfbed0";

		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/arrays-json2xml-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/arrays-json.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/arrays-xml.ttl") ;
		String r;
		try {
			String xslt = g.generateJSONtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String result = transform(arraysJSONData, xslt, "xml");
			
			String expectedResult = arraysXMLData;
			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			assertFalse(d.hasDifferences());	

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	@Test
	void testComplexJSONtoXML() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:fd0b6d7a-86b3-45f6-aa90-050164cfbed0";

		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/arrays-json2xml-crosswalk-functions.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/arrays-json.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/arrays-xml.ttl") ;

		try {
			String xslt = g.generateJSONtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);

			String result = transform(arraysJSONData, xslt, "xml");
			String expectedResult = """
<?xml version="1.0" encoding="UTF-8"?>
<root xmlns:f="http://www.w3.org/2005/xpath-functions">
    <fruits>apple|orange|pear</fruits>
    <vegetables>
        <veggieName>potato=true</veggieName>
        <veggieLike>true</veggieLike>
    </vegetables>
    <vegetables>
        <veggieName>broccoli=false</veggieName>
        <veggieLike>false</veggieLike>
    </vegetables>
</root>					
					""";
			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			System.out.println(d.fullDescription());
			assertFalse(d.hasDifferences());	
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	@Test
	void testSimpleXMLtoJSON() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:5afc2153-ad54-440c-a16f-0854bd77285b";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/arrays-xml2json-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/arrays-xml.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/arrays-json.ttl") ;
		
		try {
			String xslt = g.generateXMLtoJSON(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);			
			String result = transform(arraysXMLData, xslt, "text");
			String expectedResult = """
{
    "vegetables": [
        {
            "veggieLike": true,
            "veggieName": "potato"
        },
        {
            "veggieLike": false,
            "veggieName": "broccoli"
        }
    ],
    "fruits": [
        "apple",
        "orange",
        "pear"
    ]
}					
					
					""";
			JSONAssert.assertEquals(expectedResult, result, false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	@Test
	void testSimpleJSONtoJSON() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:fd0b6d7a-86b3-45f6-aa90-050164cfbed0";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/arrays-json2json-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/arrays-json.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/arrays-json.ttl") ;
		
		try {
			String xslt = g.generateJSONtoJSON(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String result = transform(arraysJSONData, xslt, "text");
			String expectedResult = """
{
    "vegetables": [
        {
            "veggieName": "apple orange pear"
        }
    ]
}										
					""";
			JSONAssert.assertEquals(expectedResult, result, false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	 	
	
	
	@Test
	void testClimate() {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:3b24ee60-73b5-425f-990b-18351687d567";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/climate-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/climate-source.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/climate-target.ttl") ;

		try {
			String xslt = g.generateJSONtoJSON(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			
			String inputData = """
<data>
[
    {
        "data": {
            "value": "test"          
        },
        "type": "AGGREGATION_LEVEL"
    },
    {
        "data": {
            "value": "not test"          
        },
        "type": "URL"
    }    

]
</data>				
								""";			
			String result = transform(inputData, xslt, "text");
			String expectedResult = """
{
    "records": [
        {
            "URL": "not test",
            "AGGREGATION_LEVEL": "test"
        }
    ]
}				
					""";
			JSONAssert.assertEquals(expectedResult, result, false);


		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testzbMath() throws Exception {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:3b24ee60-73b5-425f-990b-18351687d567";
		Model crosswalkModel = RDFDataMgr.loadModel("zbmath/zbmath-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("zbmath/zbmath-source.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("zbmath/zbmath-target.ttl") ;

		try {
			String xslt = g.generateXMLtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String inputData = getStringFromPath("zbmath/input-data.xml") ;			
			String result = transform(inputData, xslt, "xml");
			String expectedResult = getStringFromPath("zbmath/output-data.xml") ;
			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			if(d.hasDifferences()) {
				System.out.println(d.fullDescription());
			}
			assertFalse(d.hasDifferences());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	@Test
	public void testSample() throws Exception {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:02ecb697-255b-4a49-b0bf-3db47f90d771";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/sample-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/sample-generated.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/sample-generated.ttl") ;
		try {
			String xslt = g.generateXMLtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String inputData = getStringFromPath("xsltgenerator/sample-input-data.xml") ;			
			String result = transform(inputData, xslt, "xml");
			String expectedResult = """
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<purchaseOrder xmlns=\"http://tempuri.org/po.xsd#\">
   <shipTo country="US">
      <zip>-868.126</zip>
   </shipTo>
</purchaseOrder>										
					""";

			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			if(d.hasDifferences()) {
				System.out.println(d.fullDescription());
			}
			assertFalse(d.hasDifferences());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testSample2() throws Exception {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:02ecb697-255b-4a49-b0bf-3db47f90d771";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/sample-crosswalk2.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/sample-generated.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/sample-generated.ttl") ;
		try {
			String xslt = g.generateXMLtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String inputData = getStringFromPath("xsltgenerator/sample-input-data.xml") ;			
			String result = transform(inputData, xslt, "xml");
			String expectedResult = """
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<purchaseOrder xmlns=\"http://tempuri.org/po.xsd#\">
   <shipTo country="US">
      <zip>-868.126</zip>
   </shipTo>
   <items>
	  <item partNum="partNum1">
		<productName>productName1</productName>
	  </item>
	  <item partNum="partNum2">
		<productName>productName2</productName>
	  </item>
   </items>
</purchaseOrder>										
					""";

			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			if(d.hasDifferences()) {
				System.out.println(d.fullDescription());
			}
			assertFalse(d.hasDifferences());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	
	@Test
	public void testSample3() throws Exception {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:02ecb697-255b-4a49-b0bf-3db47f90d771";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/sample-crosswalk3.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/sample-generated.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/sample-generated.ttl") ;
		try {
			String xslt = g.generateXMLtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String inputData = getStringFromPath("xsltgenerator/sample-input-data.xml") ;			
			String result = transform(inputData, xslt, "xml");
			String expectedResult = """
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<purchaseOrder xmlns=\"http://tempuri.org/po.xsd#\">
   <shipTo country="US">
      <zip>-868.126</zip>
   </shipTo>
   <items>
	  <item partNum="partNum1">
	  </item>
	  <item partNum="partNum2">
	  </item>
   </items>
</purchaseOrder>										
					""";

			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			if(d.hasDifferences()) {
				System.out.println(d.fullDescription());
			}
			assertFalse(d.hasDifferences());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	@Test
	public void testSample4() throws Exception {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:02ecb697-255b-4a49-b0bf-3db47f90d771";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/sample-crosswalk4.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/sample-generated.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/sample-generated.ttl") ;
		try {
			String xslt = g.generateXMLtoXML(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String inputData = getStringFromPath("xsltgenerator/sample-input-data.xml") ;			
			String result = transform(inputData, xslt, "xml");
			String expectedResult = """
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<purchaseOrder xmlns=\"http://tempuri.org/po.xsd#\">
   <shipTo country="US">
      <zip>-868.126</zip>
   </shipTo>
   <items>
	  <item>
		<productName>productName1</productName>
	  </item>
	  <item>
		<productName>productName2</productName>
	  </item>
   </items>
</purchaseOrder>										
					""";

			Diff d = DiffBuilder.compare(Input.fromString(expectedResult))
		              .withTest(Input.fromString(result))
		              .ignoreWhitespace()
		              .build();
			if(d.hasDifferences()) {
				System.out.println(d.fullDescription());
			}
			assertFalse(d.hasDifferences());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	@Test
	public void testCSVtoCSV() throws Exception {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:eddcc67e-6c5d-494d-973a-65a2024d2acd";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/csv1-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/csv1-source.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/csv1-target.ttl") ;
		try {
			String xslt = g.generateCSVtoCSV(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			String inputData =
"""
<data>firstName;lastName;test
Jane;Doe;"value; with a semicolon"
Test; Tester; testing
</data>
""".trim();
			String result = transform(inputData, xslt, "text");
			String expectedResult = 
"""
name;test
"Doe Jane";"value; with a semicolon"
" Tester Test";" testing"
""".trim();

			assertEquals(expectedResult, result);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}		
	
	@Test
	public void testJSONtoCSV() throws Exception {
		XSLTGenerator2 g = new XSLTGenerator2();
		String sourceSchemaURI = "mscr:schema:1c1e5342-d5c4-4b02-8f82-a5e2eb3874ed";
		Model crosswalkModel = RDFDataMgr.loadModel("xsltgenerator/json-to-csv-1-crosswalk.ttl") ;
		Model sourceSchemaModel = RDFDataMgr.loadModel("xsltgenerator/json-to-csv-1-source.ttl") ;
		Model targetSchemaModel = RDFDataMgr.loadModel("xsltgenerator/json-to-csv-1-target.ttl") ;
		try {
			String xslt = g.generateJSONtoCSV(sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel);
			System.out.println(xslt);
			String inputData =
"""
<data>
	{
    "document": {
        "author": {
            "firstname": "Joe",
            "lastname": "Doe"
        },
        "editor": "editor",
        "header": "header"
    }
}
</data>
""".trim();
			String result = transform(inputData, xslt, "text");
			String expectedResult = 
"""
name;test
"Doe, Joe";"header"
""".trim();

			assertEquals(expectedResult, result);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
		
}
