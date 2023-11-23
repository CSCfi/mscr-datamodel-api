package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DC;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.SKOS;
import org.apache.jena.vocabulary.VOID;
import org.apache.jena.vocabulary.XSD;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;

import fi.vm.yti.datamodel.api.v2.dto.Iow;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;

@Service
public class SKOSMapper {
	
	
	
	private Resource getParent(Resource concept, Model model) {
		if(concept.hasProperty(SKOS.broader)) {
			return concept.getPropertyResourceValue(SKOS.broader);
		}
		else if(model.listSubjectsWithProperty(SKOS.narrower, concept).hasNext()) {
			return model.listSubjectsWithProperty(SKOS.narrower, concept).next();
		}
		return null;
	}
	
	private List<Resource> getChildren(Resource concept, Model model) {
		List<Resource> children = new ArrayList<Resource>();
		ResIterator i = model.listSubjectsWithProperty(SKOS.broader, concept);
		NodeIterator i2 = model.listObjectsOfProperty(concept, SKOS.narrower);
		while(i.hasNext()) {
			children.add(i.next());
		}
		while(i2.hasNext()) {
			Resource candidate = i2.next().asResource(); 
			if(!children.contains(candidate)) {
				children.add(candidate);	
			}
			
		}
		
		return children;
	}	
	
	public Model mapToModel(String pid, byte[] data) throws Exception {	
		Model outputModel = ModelFactory.createDefaultModel() ;
		Resource schema = outputModel.createResource(pid);
		
		Model inputModel = ModelFactory.createDefaultModel() ;
		InputStream is = new ByteArrayInputStream(data);
		inputModel.read(is, null, "TURTLE");
		is.close();

		
		// get the concept scheme 
		ResIterator schemes =  inputModel.listSubjectsWithProperty(RDF.type, SKOS.ConceptScheme);
		if(!schemes.hasNext()) {
			throw new Exception("No ConceptScheme found.");
		}
		Resource scheme = schemes.next(); // just getting the first one
		
		// add the root node - scheme
		String rootURI = pid + "#root/Root";
		Resource rootShape = outputModel.createResource(rootURI);
		rootShape.addLiteral(MSCR.localName, "root");
		rootShape.addLiteral(SH.name, "root");
		rootShape.addProperty(RDF.type, SH.NodeShape);		
		
		
		schema.addProperty(VOID.rootResource, rootShape);
		
		// loop through the top concepts 		
		NodeIterator topConcepts = inputModel.listObjectsOfProperty(scheme, SKOS.hasTopConcept);
		if(!topConcepts.hasNext()) {
			//throw new Exception("No top concepts found");
			List<Resource> leafs = getLeafConcepts(inputModel);
			for(Resource leaf : leafs) {
				traverseUp(leaf, inputModel, outputModel, rootURI);
			}
		}
		else {
			while(topConcepts.hasNext()) {
				Resource shape = handleConcept(rootURI, topConcepts.next().asResource(), inputModel, outputModel);
				rootShape.addProperty(SH.property, shape);			
			}
			
		}

		return outputModel;
	}
	
	
	private boolean isTopConcept(Resource r, Model model) {
		return !r.hasProperty(SKOS.broader) && !model.listSubjectsWithProperty(SKOS.narrower, r).hasNext();

	}
	
	private void traverseUp(Resource r, Model inputModel, Model outputModel, String rootURI) throws Exception {
		  // Add n to A to maintain bottom up nature
		  if (r == null) return;
		  
		  Resource rShape = handleConcept(rootURI, r, inputModel, outputModel);
		  if(isTopConcept(r, inputModel)) {
			  Resource rootShape = outputModel.createResource(rootURI);
			  rootShape.addProperty(SH.property, rShape);		
		  }
		  // Go to parent
		  Resource parent = getParent(r, inputModel);
		  if (parent == null) {
			  return;
		  }
		  
		  // For each child of p other than n, do a post order traversal
		  List<Resource> children = getChildren(parent, inputModel);
		  for(Resource child : children) {
			  //if(child.equals(r)) {
			//	  continue;
			  //}
			  handleConcept(rootURI, child, inputModel, outputModel);
		  }
		  // When done with adding all p's children, continue traversing up
		  traverseUp(parent, inputModel, outputModel, rootURI);
		}	
	
	private List<Resource> getLeafConcepts(Model inputModel) {
		List<Resource> r = new ArrayList<Resource>();
		
		String queryString = 
"""
prefix skos: <http://www.w3.org/2004/02/skos/core#> 
select ?uri
where {
  ?uri a skos:Concept
  minus {
    ?uri skos:narrower ?other .
    ?other2 skos:broader ?uri .
  }
}

""";
		Query qry = QueryFactory.create(queryString);
	    QueryExecution qe = QueryExecutionFactory.create(qry, inputModel);
	    ResultSet rs = qe.execSelect();
	    while(rs.hasNext()) {
	    	QuerySolution qs = rs.next();
	    	r.add(inputModel.getResource(qs.get("uri").toString()));
	    }
		return r;
	}
	
	private String getPrefLabel(Resource r) {
		
		if(r.getProperty(SKOS.prefLabel, "en") != null) {			
			return r.getRequiredProperty(SKOS.prefLabel, "en").getString();
		}
		else if(r.getProperty(SKOS.prefLabel) != null) {
			return r.getProperty(SKOS.prefLabel).getString();	
		}
		else if(r.getLocalName() != null && !r.getLocalName().equals("")) {
			return r.getLocalName();
		}
		else {
			return r.getURI();
		}
	}
	
	private String getLocalName(Resource r) {
		if(r.getLocalName() != null && !r.getLocalName().equals("")) {
			return r.getLocalName();
		}
		else if(!r.getURI().substring(r.getURI().lastIndexOf("/") + 1).equals("")) {
			return r.getURI().substring(r.getURI().lastIndexOf("/") + 1);
		}
		else {
			return r.getURI();
		}	
	}
	
	private Resource handleConcept(String rootURI, Resource inputConcept, Model inputModel, Model outputModel) throws Exception {
		String prefLabel = getPrefLabel(inputConcept);
		String localName = getLocalName(inputConcept);
		String shapeURI = rootURI + "/x" + localName + "-" + UUID.randomUUID().toString();
		Resource shape = outputModel.createResource(shapeURI);
		shape.addProperty(RDF.type, SH.PropertyShape);
		shape.addLiteral(SH.name, prefLabel);
		shape.addProperty(SH.path, ResourceFactory.createResource(rootURI + "/x" + localName));	
		shape.addProperty(SH.maxCount, outputModel.createTypedLiteral(1));
		shape.addProperty(SH.minCount, outputModel.createTypedLiteral(1));
		List<Resource> children = getChildren(inputConcept, inputModel);
		if(children.size() == 0) {
			// leaf node - data property
			shape.addProperty(DCTerms.type, OWL.DatatypeProperty);
			shape.addProperty(SH.datatype, XSD.xstring);			
		}
		else {
			shape.addProperty(DCTerms.type, OWL.ObjectProperty);
			// add node
			Resource nodeShape = outputModel.createResource(shapeURI + "-node");
			nodeShape.addProperty(RDF.type, SH.NodeShape);
			nodeShape.addProperty(SH.name, prefLabel);
			nodeShape.addProperty(SH.description, "desc");
			nodeShape.addLiteral(MSCR.localName, "node");
			
			shape.addProperty(SH.node, nodeShape);
			for(Resource child : children) {
				Resource prop = handleConcept(rootURI, child, inputModel, outputModel);
				nodeShape.addProperty(SH.property, prop);	
			}
		}
		
		return shape;

	}
}
