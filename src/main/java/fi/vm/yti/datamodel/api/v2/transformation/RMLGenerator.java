package fi.vm.yti.datamodel.api.v2.transformation;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MappingInfoDTO;

@Service
public class RMLGenerator {

	record TreeNode(List<TreeNode> children, String targetXPath, String targetElementName,
			List<MappingInfoDTO> mappings, String sourceURI) {
	}

	String nsRML = "http://semweb.mmlab.be/ns/rml#";
	String nsQL = "http://semweb.mmlab.be/ns/ql#";
	String nsRR = "http://www.w3.org/ns/r2rml#";

	private String getXpathFromId(String id) {

		String temp = id.substring(id.indexOf("Root") + 4);
		// remove all parts with capital letter from the temp string
		String[] parts = temp.split("/");
		List<String> newParts = new ArrayList<String>();
		for (String part : parts) {
			if (part.length() > 0 && Character.isLowerCase(part.charAt(0))) {
				newParts.add(part);
			}
		}
		temp = "/" + StringUtils.join(newParts, "/");
		return temp;
	}

	private String getPropertyNodeIdFromXpath(String pid, String xpath) {
		String[] parts = xpath.split("/");
		String path = "";
		int i = 0;
		for (; i < parts.length; i++) {
			String part = parts[i];
			if (part != "") {
				if (i < parts.length - 1) {
					path = path + "/" + part + "/" + part.substring(0, 1).toUpperCase() + part.substring(1);
				} else {
					path = path + "/" + part;
				}

			}
		}
		String id = pid + "#root/Root" + path;
		return id;

	}

	private Element addElementByPath(XPath xpath, Element parent, String path) throws Exception {
		var node = parent;
		var parts = path.split("/");
		var dom = parent.getOwnerDocument();
		for (String part : parts) {
			if (!part.equals("")) {
				XPathExpression expr = xpath.compile(part);
				NodeList nodes = (NodeList) expr.evaluate(node, XPathConstants.NODESET);
				if (nodes.getLength() > 0) {
					node = (Element) nodes.item(0);
				} else {
					node = (Element) node.appendChild(dom.createElement(part));
				}
			}

		}

		return node;
	}

	private String getXPath(Node node) {
		Node parent = node.getParentNode();
		if (parent == null) {
			return "";
		}
		return getXPath(parent) + "/" + node.getNodeName();
	}

	public void handleTree(TreeNode targetTree, Node node, Map<String, List<MappingInfoDTO>> infos,
			Map<String, TreeNode> sourceLookup, String pid) {
		NodeList _list = node.getChildNodes();
		for (int i = 0; i < _list.getLength(); i++) {
			Node _node = _list.item(i);
			String cxpath = getXPath(_node).substring(6);
			String id = getPropertyNodeIdFromXpath(pid, cxpath);
			TreeNode child = new TreeNode(new ArrayList<TreeNode>(), cxpath, _node.getNodeName(), infos.get(cxpath),
					id);
			targetTree.children.add(child);
			sourceLookup.put(cxpath, child);
			handleTree(child, _node, infos, sourceLookup, pid);

		}
	}

	// need to go through the parts of the xpath one by one and figure out
	// whether they are repetable or not
	private String getJsonPathFromXPath(Model m, String xpath, Map<String, TreeNode> sourceLookup) {
		String[] parts = xpath.split("/");
		String currentXPath = "";
		String ref = "$";
		int i = 0;
		for (String part : parts) {
			if (!part.equals("")) {
				ref = ref + "." + part;
				currentXPath = currentXPath + "/" + part;
				TreeNode node = sourceLookup.get(currentXPath);
				if (node != null) {
					Resource sourceProperty = m.getResource(node.sourceURI);
					System.out.println("Source property: " + node.sourceURI);
					Statement maxStmt = m.getProperty(sourceProperty, SH.maxCount);
					if (maxStmt == null || maxStmt.getObject().asLiteral().getInt() > 1) {
						ref = ref + "[*]";
					}

				}
			}
		}
		return ref;
	}

	private Resource createLogicalSourceJSON(Model sourceModel, Model m, String iteratorXPath,
			Map<String, TreeNode> sourceLookup) {
		Resource logicalSource = m.createResource();
		logicalSource.addProperty(RDF.type, m.createResource(nsRML + "BaseSource"));
		Resource formulation = m.createResource(nsQL + "JSONPath");
		logicalSource.addProperty(m.createProperty(nsRML + "referenceFormulation"), formulation);
		logicalSource.addProperty(m.createProperty(nsRML + "source"), m.createLiteral("data/openalex.json"));

		String iteratorJsonPath = getJsonPathFromXPath(sourceModel, iteratorXPath, sourceLookup);
		logicalSource.addProperty(m.createProperty(nsRML + "iterator"), iteratorJsonPath);
		return logicalSource;
	}

	private void createPredicateObjectMap(Model m, Model sourceModel, String iteratorPropertyUri,
			String iteratorReference, String targetClass, Resource triplesMap, Map<String, TreeNode> sourceLookup) {
		String q = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX mscr: <http://uri.suomi.fi/datamodel/ns/mscr#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" + "PREFIX dcterms: <http://purl.org/dc/terms/>\n"
				+ "PREFIX sh: <http://www.w3.org/ns/shacl#>\n"
				+ "select distinct ?source ?type ?property ?classRef ?path \n" + "where {\n"
				+ "?mapping rdf:type mscr:Mapping . \n" + "?mapping mscr:target/rdf:_1/mscr:uri ?property .\n"
				+ "?mapping mscr:source/rdf:_1/mscr:uri ?source.\n" + "<" + targetClass
				+ "> sh:property ?property.OPTIONAL {?property sh:class ?classRef}.\n" // type of class

				+ "FILTER(!strstarts( STR(?property), \"iterator:\") && !strstarts( STR(?property), \"subject:\"))\n"
				+ "  FILTER(strstarts( STR(?source), \"" + iteratorPropertyUri + "\"))\n"
				+ "?property sh:path ?path .\n"

//				+ "OPTIONAL {?property rdfs:domain <" + targetClass + ">.?property rdf:type ?type.}\n" // type of class

				+ "}";

		System.out.println(q);
		QueryExecution qe = QueryExecutionFactory.create(q, sourceModel);
		ResultSet results = qe.execSelect();
		if (!results.hasNext()) {
			System.out.println("No predicateobject for " + iteratorPropertyUri);
		}
		while (results.hasNext()) {
			QuerySolution soln = results.next();
			Resource sourceResource = soln.getResource("source");
			Resource datatype = soln.getResource("type");
			Resource property = soln.getResource("?property");
			Resource path = soln.getResource("?path");
			Resource classRef = soln.getResource("classRef");
			Resource pom = m.createResource();

			Resource ref = m.createResource();
			if (classRef != null || (datatype != null && !datatype.getURI().equals(OWL.DatatypeProperty.getURI()))) {
				ref.addProperty(m.createProperty(nsRR + "termType"), m.createResource(nsRR + "IRI"));
			}

			// isAnon = does not exists <https://semopenalex.org/ontology/WorkShape>
			// <http://uri.suomi.fi/datamodel/ns/mscr#uri>
			// <subject:https://semopenalex.org/ontology/WorkShape>
			System.out.println("anon: " + "subject:" + targetClass);
			if (true) {
				String xpath = getXpathFromId(sourceResource.getURI());
				String reference = getJsonPathFromXPath(sourceModel, xpath, sourceLookup);
				if (classRef != null) {
					// need to find the shape that has the property that points to this shape
					// workShape property [ sh:class <https://semopenalex.org/ontology/Authorship> ]
					// authorshipShape sh:targetClass <https://semopenalex.org/ontology/Authorship>
					ResIterator ri = sourceModel.listResourcesWithProperty(SH.targetClass, classRef);
					if (ri.hasNext()) {

						Resource parentTripleMap = ri.next();
						boolean isAnon = !sourceModel.containsResource(
								ResourceFactory.createResource("subject:" + parentTripleMap.getURI()));
						if (isAnon) {
							ref.addProperty(m.createProperty(nsRR + "parentTriplesMap"),
									m.createResource(parentTripleMap.getURI() + ":triplesMap"));
							Resource join = m.createResource();
							join.addLiteral(m.createProperty(nsRR + "child"), reference.length() > iteratorReference.length()
									? reference.substring(iteratorReference.length() + 1)
											: reference);
							join.addLiteral(m.createProperty(nsRR + "parent"), "$");
							ref.addProperty(m.createProperty(nsRR + "joinCondition"), join);							
						}

					}
				}
				
				
				ref.addProperty(m.createProperty(nsRML + "reference"),
						reference.length() > iteratorReference.length()
								? reference.substring(iteratorReference.length() + 1)
								: reference);

			}

			pom.addProperty(m.createProperty(nsRR + "objectMap"), ref);
			if (path == null) {
				pom.addProperty(m.createProperty(nsRR + "predicate"), property);
			} else {
				pom.addProperty(m.createProperty(nsRR + "predicate"), path);
			}
			//

			triplesMap.addProperty(m.createProperty(nsRR + "predicateObjectMap"), pom);

		}
	}

	public Model generate(List<MappingInfoDTO> mappings, Model mappingsModel, Model sourceModel, String sourcePID,
			Model targetModel) throws Exception {

		Map<String, TreeNode> sourceLookup = new HashMap<String, TreeNode>();

		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		XPathFactory xpathFactory = XPathFactory.newInstance(); //
		XPath xpath = xpathFactory.newXPath();
		Map<String, List<MappingInfoDTO>> sourceInfo = new HashMap<String, List<MappingInfoDTO>>();
		for (MappingInfoDTO mapping : mappings) {
			String sourceId = mapping.getSource().get(0).getUri();
			String sourceXpath = getXpathFromId(sourceId);
			if (!sourceInfo.containsKey(sourceXpath)) {
				sourceInfo.put(sourceXpath, new ArrayList<MappingInfoDTO>());
			}
			sourceInfo.get(sourceXpath).add(mapping);
		}

		Document doc2 = docBuilder.newDocument();
		Element root2 = doc2.createElement("root2");
		doc2.appendChild(root2);

		for (String t : sourceInfo.keySet()) {
			addElementByPath(xpath, root2, t);
		}
		Model m = ModelFactory.createDefaultModel();

		Model tm = ModelFactory.createDefaultModel();
		tm.add(mappingsModel);
		tm.add(targetModel);
		tm.add(sourceModel);
		// Create another tree with extra info
		TreeNode _root = new TreeNode(new ArrayList<TreeNode>(), "/", root2.getNodeName(), null, "root");
		handleTree(_root, root2, sourceInfo, sourceLookup, sourcePID);

		// for each datatype property that is the source to class mappings
		String q1 = "PREFIX mscr: <http://uri.suomi.fi/datamodel/ns/mscr#>\n"
				+ "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + "select distinct ?target ?source \n"
				+ "where {\n" + " ?mappings rdf:type mscr:Mapping . \n"
				+ " ?mapping mscr:target/rdf:_1/mscr:uri ?target .\n"
				+ " ?mapping mscr:target/rdf:_1/mscr:label ?targetLabel .\n"
				+ " ?mapping mscr:source/rdf:_1/mscr:uri ?source .\n" + " FILTER(?targetLabel=\"iterator source\") \n"
				+ "}";
		System.out.println(q1);
		QueryExecution qe = QueryExecutionFactory.create(q1, tm);
		ResultSet it = qe.execSelect();
		while (it.hasNext()) {
			System.out.println("*");

			QuerySolution soln1 = it.next();

			Resource targetClass = null;
			String target = soln1.getResource("target").getURI();
			System.out.println(target);

			targetClass = tm.createResource(target.substring(9));
			Resource triplesMap = m.createResource(targetClass.getURI() + ":triplesMap");
			triplesMap.addProperty(RDF.type, m.createResource(nsRR + "TriplesMap"));

			System.out.println("TargetClass: " + targetClass);
			Resource sourcePropertyShape = soln1.getResource("source");

			System.out.println("Source property shape: " + sourcePropertyShape);
			String iteratorURI = sourcePropertyShape.getURI();
			String iteratorPath = getXpathFromId(iteratorURI);

			// add logical source
			// Resource logicalSource = createLogicalSourceJSON(tm, m, iteratorPath,
			// sourceLookup);
			Resource logicalSource = m.createResource();
			logicalSource.addProperty(RDF.type, m.createResource(nsRML + "BaseSource"));
			Resource formulation = m.createResource(nsQL + "JSONPath");
			logicalSource.addProperty(m.createProperty(nsRML + "referenceFormulation"), formulation);
			logicalSource.addProperty(m.createProperty(nsRML + "source"), m.createLiteral("data/openalex.json"));

			String iteratorJsonPath = getJsonPathFromXPath(sourceModel, iteratorPath, sourceLookup);
			logicalSource.addProperty(m.createProperty(nsRML + "iterator"), iteratorJsonPath);

			triplesMap.addProperty(m.createProperty(nsRML + "logicalSource"), logicalSource);

			// get source property
			String q2 = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
					+ "PREFIX mscr: <http://uri.suomi.fi/datamodel/ns/mscr#>\n"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + "select distinct ?source\n"
					+ "where {\n" + " ?mappings rdf:type mscr:Mapping . \n"
					+ " ?mapping mscr:target/rdf:_1/mscr:uri <subject:" + targetClass.getURI() + "> .\n"
					+ " ?mapping mscr:source/rdf:_1/mscr:uri ?source .\n" + "}";
			QueryExecution qe2 = QueryExecutionFactory.create(q2, tm);
			ResultSet it2 = qe2.execSelect();
			Resource subjectSourcePropertyShape = null;
			if (it2.hasNext()) {
				subjectSourcePropertyShape = it2.next().getResource("source");
			}

			// add subjectMap
			Resource subjectMap = createSubjectMap(tm, m, iteratorPath, subjectSourcePropertyShape, targetClass,
					sourceLookup);
			triplesMap.addProperty(m.createProperty(nsRR + "subjectMap"), subjectMap);

			// predicateObjectMap
			createPredicateObjectMap(m, tm, iteratorURI, iteratorJsonPath, targetClass.getURI(), triplesMap,
					sourceLookup);
			tm.write(new FileOutputStream(new File("model.ttl")), "TURTLE");
		}

		return m;
	}

	private Resource createSubjectMap(Model tm, Model m, String iteratorPath, Resource sourcePropertyShape,
			Resource targetClass, Map<String, TreeNode> sourceLookup) {
		Resource targetOntologyClass = targetClass.getPropertyResourceValue(SH.targetClass);
		if (sourcePropertyShape != null) {
			System.out.println("Subject map: " + sourcePropertyShape.getURI());
			String fullReferencePath = getXpathFromId(sourcePropertyShape.getURI());
			String referencePath = getJsonPathFromXPath(m, fullReferencePath.substring(iteratorPath.length()),
					sourceLookup).substring(2);
			Resource blank = m.createResource();

			blank.addProperty(m.createProperty(nsRML + "reference"), referencePath);
			blank.addProperty(m.createProperty(nsRR + "class"), targetOntologyClass);
			return blank;
		} else {
			Resource blank = m.createResource();

			blank.addProperty(m.createProperty(nsRR + "termType"), m.createResource(nsRR + "BlankNode"));
			blank.addProperty(m.createProperty(nsRR + "class"), targetOntologyClass);
			return blank;
		}
	}

}
