package fi.vm.yti.datamodel.api.v2.transformation;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import be.ugent.idlab.knows.functions.agent.Agent;
import be.ugent.idlab.knows.functions.agent.AgentFactory;
import be.ugent.idlab.knows.functions.agent.Arguments;

@TestInstance(Lifecycle.PER_CLASS)
public class FnOTest {

	private Agent agent;
	
	
	@BeforeAll
	public void createAgent()  throws Exception{		
	    this.agent = AgentFactory.createFromFnO("src/test/resources/fno/test1.ttl");
	}
	
	@Test
	public void testSimpleStringToString() throws Exception {
	    // prepare the parameters for the function
	    Arguments arguments = new Arguments()
	        .add("http://uri.suomi.fi/datamodel/ns/mscr#inputString", "test");
	    // execute the function
	    String result = (String) this.agent.execute("http://uri.suomi.fi/datamodel/ns/mscr#stringToStringFunc", arguments);
	    assert (result.equals("test"));
	}
	
	@Test
	public void testSimpleStringToInt() throws Exception {
	    // prepare the parameters for the function
	    Arguments arguments = new Arguments()
	        .add("http://uri.suomi.fi/datamodel/ns/mscr#inputString", "33");
	    // execute the function
	    int result = (Integer) this.agent.execute("http://uri.suomi.fi/datamodel/ns/mscr#stringToIntFunc", arguments);
	    assert (result == 33);
	}	
	
	@Test
	public void testCelsiusToFahrenheit() throws Exception {
		double input = 24.0;
	    Arguments arguments = new Arguments()
		        .add("http://uri.suomi.fi/datamodel/ns/mscr#inputDouble", input);
		    // execute the function
		    double result = (Double) this.agent.execute("http://uri.suomi.fi/datamodel/ns/mscr#celsiusToFahrenheitFunc", arguments);
		    assert (result == 75.2);
		
	}

	@Test
	public void testAnyToString() throws Exception {
		Object input = 24.0;
	    Arguments arguments = new Arguments()
		        .add("http://uri.suomi.fi/datamodel/ns/mscr#inputObject", input);
		    // execute the function
		    String result = (String) this.agent.execute("http://uri.suomi.fi/datamodel/ns/mscr#anyToStringFunc", arguments);
		    assert (result.equals("24.0"));		
	}
	
	@Test
	public void testSimpleCoordToComplex() throws Exception {
	    Arguments arguments = new Arguments()
		        .add("http://uri.suomi.fi/datamodel/ns/mscr#inputString", "24.2323232,92.38238238");
		    // execute the function
		    String result = (String) this.agent.execute("http://uri.suomi.fi/datamodel/ns/mscr#simpleCoordinateToComplexFunc", arguments);
		    assert (result.equals("<coordinate><lang>24.2323232</lang><long>92.38238238</long></coordinate>"));		
	}

	
}
