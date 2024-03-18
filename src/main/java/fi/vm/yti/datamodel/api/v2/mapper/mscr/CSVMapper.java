package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.Scanner;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VOID;
import org.apache.jena.vocabulary.XSD;
import org.topbraid.shacl.vocabulary.SH;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;

public class CSVMapper {
	
	
	private Model addRootShape(String pid, Model model) {
		Resource root = model.createResource(pid + "#root/Root");
		root.addLiteral(SH.name, "root");
		root.addProperty(RDF.type, SH.NodeShape);		
				
		return model;		
	}
	
	private Model addProperties(String pid, Model model, String[] properties) {
		String rootURI = pid + "#root/Root";
		Resource root = model.getResource(rootURI);
		int c = 1;
		for(String propertyName : properties) {
			propertyName = propertyName.trim().replaceAll(" ", "-");
			Resource property = model.createResource(rootURI + "/" + URLEncoder.encode(propertyName));			
			property.addProperty(RDF.type, SH.PropertyShape);
			property.addProperty(SH.datatype, XSD.xstring);
			property.addLiteral(SH.maxCount, 1);
			property.addLiteral(model.createProperty(MSCR.URI + "column"), c);
			property.addProperty(SH.path, model.createResource(pid + "#" + propertyName));
			property.addLiteral(SH.name, propertyName);
			root.addProperty(SH.property, property);
			
			c = c + 1;
		}
		return model;
	}
	public Model mapToModel(String pid, byte[] data, String delimiter) throws Exception {
		Model m = ModelFactory.createDefaultModel();		
		InputStream input = new ByteArrayInputStream(data);
		Scanner scanner = new Scanner(input);		
		if(!scanner.hasNextLine() ) {
			scanner.close();
			throw new Exception("CSV schema must have exactly one line.");
		}		
		String[] columns = scanner.nextLine().split(delimiter);		
		input.close();
		scanner.close();
		
		addRootShape(pid, m);
		addProperties(pid, m, columns);
				
		return m;
	}

}
