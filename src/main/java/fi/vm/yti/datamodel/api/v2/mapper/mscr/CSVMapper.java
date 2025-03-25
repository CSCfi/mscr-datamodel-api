package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.Scanner;
import java.util.UUID;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;
import org.apache.jena.vocabulary.XSD;
import org.topbraid.shacl.vocabulary.SH;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;

public class CSVMapper {
	
	
	private Model addRootShape(String pid, Model model) {
		Resource root = model.createResource(pid + "#root");
		root.addLiteral(SH.name, "root");
		root.addProperty(RDF.type, SH.NodeShape);		
				
		return model;		
	}
	
	private Model addProperties(String pid, Model model, String[] properties) {
		String rootURI = pid + "#root";
		Resource root = model.getResource(rootURI);
		int c = 1;
		for(String propertyName : properties) {
			propertyName = propertyName.trim();
			Resource property = model.createResource(pid + "#" + UUID.randomUUID().toString());			
			property.addProperty(RDF.type, SH.PropertyShape);
			property.addProperty(DCTerms.type, OWL.DatatypeProperty);
			property.addProperty(SH.datatype, XSD.xstring);
			property.addLiteral(SH.maxCount, 1);
			property.addLiteral(SH.minCount, 1);
			property.addLiteral(model.createProperty(MSCR.URI + "column"), c);
			property.addLiteral(SH.order, model.createTypedLiteral(c));
			property.addProperty(SH.path, model.createResource("mscr:column_" + c));
			property.addLiteral(SH.name, propertyName);
			root.addProperty(SH.property, property);
			
			c = c + 1;
		}
		return model;
	}
	public Model mapToModel(String pid, byte[] data, String delimiter) throws Exception {
		Model m = ModelFactory.createDefaultModel();
		m.setNsPrefix("", pid +"#");
		InputStream input = new ByteArrayInputStream(data);
		CSVParser parser = new CSVParserBuilder().withSeparator(';').build();
		CSVReader reader = new CSVReaderBuilder(new InputStreamReader(input)).withCSVParser(parser).build();
		
		long lines = reader.getLinesRead();
		if(lines > 1) {
			reader.close();
			throw new Exception("CSV schema must have exactly one line.");
		}		
		
		String[] columns = reader.readNext();		
		input.close();
		reader.close();
		
		addRootShape(pid, m);
		addProperties(pid, m, columns);
				
		return m;
	}

}
