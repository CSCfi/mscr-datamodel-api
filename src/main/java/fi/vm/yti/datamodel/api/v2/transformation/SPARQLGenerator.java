package fi.vm.yti.datamodel.api.v2.transformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;

import fi.vm.yti.datamodel.api.v2.dto.MappingInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.NodeInfo;

@Service
public class SPARQLGenerator {

	
	private String getIteratorShape(Model crosswalkModel) {

		String q = "PREFIX mscr: <http://uri.suomi.fi/datamodel/ns/mscr#> \n PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n select ?row where { ?mapping mscr:target/rdf:_1/mscr:id ?targetID . ?mapping mscr:source/rdf:_1/mscr:uri ?row . FILTER(?targetID=\"iterator\")} limit 1";
		System.out.println(q);
		QueryExecution qe = QueryExecutionFactory.create(q, crosswalkModel);
		ResultSet rs = qe.execSelect();
		if(rs.hasNext()) {
			QuerySolution soln = rs.next();
			return soln.getResource("row").getURI();
			
		}
		else {
			return null;
		}

	}
	
	private String makeVarname(String v) {
		v = v.replaceAll(" ", "_");
		v = v.replaceAll("/", "");
		v = v.replaceAll("#", "-");
		v = v.replaceAll("-", "_");
		v = v.replaceAll(":", "_");
		v = v.replaceAll("\\.", "_");
		return "?" + v;
	}
	
	private String getVarname(Resource r, Model m, String suffix) {
		// check for name 
		if(m.contains(r, SH.name, (RDFNode)null)) {
			return makeVarname(m.getProperty(r, SH.name).getString()) + suffix;
		}
		if(m.qnameFor(r.getURI()) != null) {
			return makeVarname(m.qnameFor(r.getURI()) + suffix);
		}
		return makeVarname(r.getURI()+suffix);
	}
	public String generateRDFtoCSV(String crosswalkPID, List<MappingInfoDTO> mappings, Model crosswalkModel, Model sourceModel, Model targetModel) throws Exception {
		String q = "";
		List<String> whereLines = new ArrayList<String>();
		
		String varSuffix = "_" + makeVarname(crosswalkPID).substring(1);
		String rowVar = "?" + varSuffix;
		
		String rowIteratorShape = getIteratorShape(crosswalkModel);		
		if(rowIteratorShape == null) {
			throw new Exception("No row iterator mapped.");
		}
		NodeIterator ni1 = sourceModel.listObjectsOfProperty(sourceModel.getResource(rowIteratorShape), SH.targetClass);
		if(!ni1.hasNext()) {
			throw new Exception("Could not find sh:targetclass for iterator shape "+ rowIteratorShape);
		}
		Resource iteratorClass = ni1.next().asResource();
		String rowIterator = rowVar + " a <" + iteratorClass.getURI() + "> . \n"; 
		
		for(MappingInfoDTO mapping : mappings) {
			List<NodeInfo> targets = mapping.getTarget();
			List<NodeInfo> sources = mapping.getSource();
			// ha	
			for(NodeInfo target : targets) {
				if(!target.getId().equals("iterator")) {
					Resource targetPropertyShape = targetModel.getResource(target.getUri());
					NodeInfo source = sources.get(0);
					Resource sourcePropertyShape =  sourceModel.getResource(source.getUri());
					Resource sourceNodeShape = sourceModel.listSubjectsWithProperty(SH.property, sourcePropertyShape).next();				
					
					Resource sourcePropertyClass = sourceNodeShape.getPropertyResourceValue(SH.targetClass);
					if(sourcePropertyClass == null) {
						throw new Exception("No target class found for shape " + sourcePropertyShape.getURI());
					}
					Resource propertyPath = sourcePropertyShape.getPropertyResourceValue(SH.path);
					String propertyShapeVar = getVarname(sourceNodeShape, sourceModel, varSuffix);
					
					String targetTitle = targetPropertyShape.getProperty(SH.name).getString(); // this is always there
					String targetVar = "?" + targetTitle + varSuffix;
					// if source property is part of the row shape 
					if(sourceNodeShape.getURI().equals(rowIteratorShape)) {
						whereLines.add(rowVar + " <" + propertyPath.getURI() + "> " + targetVar + " . \n");
					}
					else {
						whereLines.add(rowVar + " (<>|!<>)* " + propertyShapeVar + " . \n");
						whereLines.add(propertyShapeVar + " a <" + sourcePropertyClass.getURI() + "> . \n");
						whereLines.add(propertyShapeVar + " <" + propertyPath.getURI() + "> " + targetVar + " . \n");
					}
					
					
				}
			}
		}
		
		
		// generate select row from the target model
		String select = "select ";
		String targetColQuery = "select ?prop ?label where { ?prop a <http://www.w3.org/ns/shacl#PropertyShape> . ?prop <http://www.w3.org/ns/shacl#name> ?label . ?prop <http://www.w3.org/ns/shacl#order> ?order} order by asc(?order)";
		QueryExecution qe = QueryExecutionFactory.create(targetColQuery, targetModel);
		ResultSet selectColsResultSet = qe.execSelect();
		while (selectColsResultSet.hasNext()) {
			
			QuerySolution soln = selectColsResultSet.next();
			Resource r = soln.getResource("prop");
			String colLabel = soln.getLiteral("label").getString().replaceAll(" ", "_");
			String propertyVar = getVarname(r, targetModel, varSuffix);
			select = select + "(" +propertyVar + " as ?" + colLabel + ") ";
			
		}
		
		
		
		q = select + " where {" + rowIterator + String.join("", whereLines) + " }";
		System.out.println(q);
		return q;
	}
}
