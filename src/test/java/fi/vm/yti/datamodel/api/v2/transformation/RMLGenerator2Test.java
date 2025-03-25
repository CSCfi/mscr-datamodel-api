package fi.vm.yti.datamodel.api.v2.transformation;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.Test;

class RMLGenerator2Test {

	@Test
	void test() {
		RMLGenerator2 g = new RMLGenerator2();
		
		Model m = RDFDataMgr.loadModel("rmlgenerator/crosswalk1-input.ttl") ;
		Model outputModel = g.generateRMLFromMSCRGraph(m, "mscr:crosswalk:170c9c32-3b18-4ac1-853c-5a48aa1b3172", "mscr:schema:b2973de1-f9e6-4778-9390-039a86cb0771");
		outputModel.write(System.out, "TURTLE");
	}
	

	@Test
	void test2() {
		try {
			RMLGenerator2 g = new RMLGenerator2();
			
			Model m = RDFDataMgr.loadModel("rmlgenerator/arrays_xml2shacl/crosswalk_content.ttl") ;
			m.add(RDFDataMgr.loadModel("rmlgenerator/arrays_xml2shacl/crosswalk_metadata.ttl"));
			m.add(RDFDataMgr.loadModel("rmlgenerator/arrays_xml2shacl/source_metadata.ttl"));
			m.add(RDFDataMgr.loadModel("rmlgenerator/arrays_xml2shacl/target_metadata.ttl"));
			m.add(RDFDataMgr.loadModel("rmlgenerator/arrays_xml2shacl/source_content.ttl"));
			m.add(RDFDataMgr.loadModel("rmlgenerator/arrays_xml2shacl/target_content.ttl"));
			Model outputModel = g.generateRMLFromMSCRGraph(m, "mscr:crosswalk:e0434bd1-310f-4de8-99c6-1dee0b41086f", "mscr:schema:5afc2153-ad54-440c-a16f-0854bd77285b");
			outputModel.write(System.out, "TURTLE");
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}	


	@Test
	void test3() {
		try {
		RMLGenerator2 g = new RMLGenerator2();
		
		Model m = RDFDataMgr.loadModel("rmlgenerator/arrays_json2shacl/crosswalk_content.ttl") ;
		m.add(RDFDataMgr.loadModel("rmlgenerator/arrays_json2shacl/crosswalk_metadata.ttl"));
		m.add(RDFDataMgr.loadModel("rmlgenerator/arrays_json2shacl/source_metadata.ttl"));
		m.add(RDFDataMgr.loadModel("rmlgenerator/arrays_json2shacl/target_metadata.ttl"));
		m.add(RDFDataMgr.loadModel("rmlgenerator/arrays_json2shacl/source_content.ttl"));
		m.add(RDFDataMgr.loadModel("rmlgenerator/arrays_json2shacl/target_content.ttl"));
		Model outputModel = g.generateRMLFromMSCRGraph(m, "mscr:crosswalk:ab141a25-0be4-4586-9120-6ebc431dbdec", "mscr:schema:fd0b6d7a-86b3-45f6-aa90-050164cfbed0");
		outputModel.write(System.out, "TURTLE");
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}	


	@Test
	void test4() {
		try {
		RMLGenerator2 g = new RMLGenerator2();
		
		Model m = RDFDataMgr.loadModel("rmlgenerator/person_xml2shacl/crosswalk_content.ttl") ;
		m.add(RDFDataMgr.loadModel("rmlgenerator/person_xml2shacl/crosswalk_metadata.ttl"));
		m.add(RDFDataMgr.loadModel("rmlgenerator/person_xml2shacl/source_metadata.ttl"));
		m.add(RDFDataMgr.loadModel("rmlgenerator/person_xml2shacl/target_metadata.ttl"));
		m.add(RDFDataMgr.loadModel("rmlgenerator/person_xml2shacl/source_content.ttl"));
		m.add(RDFDataMgr.loadModel("rmlgenerator/person_xml2shacl/target_content.ttl"));
		Model outputModel = g.generateRMLFromMSCRGraph(m, null, "mscr:schema:ea943766-2604-4f6d-b474-cff8a78830dd");
		outputModel.write(System.out, "TURTLE");
		}catch(Exception ex) {
			ex.printStackTrace();
		}
	}	
}
