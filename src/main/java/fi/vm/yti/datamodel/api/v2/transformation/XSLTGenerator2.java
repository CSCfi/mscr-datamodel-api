package fi.vm.yti.datamodel.api.v2.transformation;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.graph.impl.LiteralLabelFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.VOID;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MappingInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.NodeInfo;
import fi.vm.yti.datamodel.api.v2.dto.ProcessingInfo;
import fi.vm.yti.datamodel.api.v2.mapper.MappingMapper;
import fi.vm.yti.datamodel.api.v2.transformation.RMLGenerator2.IteratorData;
import fi.vm.yti.datamodel.api.v2.transformation.XSLTGenerator.TreeNode2;

@Service
public class XSLTGenerator2 {

	public static final String xslNS = "http://www.w3.org/1999/XSL/Transform";
	public static final String funcNS = "http://www.w3.org/2005/xpath-functions";

	record TreeNode(Map<Integer, TreeNode> children, String targetPropertyURI, String mappingURI, String targetElementName,
			String targetElementNamespace, boolean isAttribute, String datatype) {
	};
	
	record IteratorData(String iteratorPropertyURI, String targetShapeURI, String sourcePropertyURI, String mappingURI) {}

	private TreeNode generateTreeMap(DocumentBuilder docBuilder, List<String> namespaces, Model crosswalkModel, Model targetSchemaModel, boolean isJSONOutput, boolean isJSONInput) throws Exception {
		Document targetDoc = docBuilder.newDocument();

		generateTargetTree(targetDoc, namespaces, ModelFactory.createUnion(crosswalkModel, targetSchemaModel), isJSONOutput, isJSONInput);
		System.out.println(toString(targetDoc));
		Element treeRoot = (Element)targetDoc.getDocumentElement().getFirstChild();
		return generateTargetMap(treeRoot, isJSONOutput); // make another version of the target tree with ordered elements
	}
	public String generateXMLtoXML(String sourceSchemaURI, Model sourceSchemaModel, Model crosswalkModel, Model targetSchemaModel) throws Exception {
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		Document doc = docBuilder.newDocument();

		final List<String> namespaces = getNamespaces(sourceSchemaModel);
		namespaces.addAll(getNamespaces(targetSchemaModel));
		
		TreeNode targetTreeMap = generateTreeMap(docBuilder, namespaces, crosswalkModel, targetSchemaModel, false, false); 

		Element stylesheet = doc.createElementNS(xslNS, "xsl:stylesheet");
		stylesheet.setAttribute("version", "3.0");
		doc.appendChild(stylesheet);
		Element rootElement = addTemplatesXMLtoXML(stylesheet, targetTreeMap, namespaces, sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel, null, false, false);
		addRootTemplate(geTemplateNameFromRootElement(rootElement), stylesheet);		
		return toString(doc);
	}
	
	public String generateJSONtoXML(String sourceSchemaURI, Model sourceSchemaModel, Model crosswalkModel, Model targetSchemaModel) throws Exception {
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		Document doc = docBuilder.newDocument();

		final List<String> namespaces = getNamespaces(sourceSchemaModel);
		namespaces.addAll(getNamespaces(targetSchemaModel));

		TreeNode targetTreeMap = generateTreeMap(docBuilder, namespaces, crosswalkModel, targetSchemaModel, false, true); 

		Element stylesheet = doc.createElementNS(xslNS, "xsl:stylesheet");
		stylesheet.setAttribute("version", "3.0");
		stylesheet.setAttribute("xmlns:f", funcNS);
		doc.appendChild(stylesheet);
		Element rootElement = addTemplatesXMLtoXML(stylesheet, targetTreeMap, namespaces, sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel, null, true, false);
		addRootJSONInputTemplate(geTemplateNameFromRootElement(rootElement), stylesheet);		
		return toString(doc);	
	}
	
	public String generateXMLtoJSON(String sourceSchemaURI, Model sourceSchemaModel, Model crosswalkModel, Model targetSchemaModel) throws Exception {
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		Document doc = docBuilder.newDocument();

		final List<String> namespaces = getNamespaces(sourceSchemaModel);
		namespaces.addAll(getNamespaces(targetSchemaModel));

		TreeNode targetTreeMap = generateTreeMap(docBuilder, namespaces, crosswalkModel, targetSchemaModel, true, false); 

		Element stylesheet = doc.createElementNS(xslNS, "xsl:stylesheet");
		stylesheet.setAttribute("version", "3.0");
		stylesheet.setAttribute("xmlns:f", funcNS);
		doc.appendChild(stylesheet);
		Element rootElement = addTemplatesXMLtoXML(stylesheet, targetTreeMap, namespaces, sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel, null, false, true);
		addRootJSONOutputTemplate(geTemplateNameFromRootElement(rootElement), stylesheet);		
		return toString(doc);	
	}	
	
	public String generateJSONtoJSON(String sourceSchemaURI, Model sourceSchemaModel, Model crosswalkModel, Model targetSchemaModel) throws Exception {
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		Document doc = docBuilder.newDocument();

		final List<String> namespaces = getNamespaces(sourceSchemaModel);
		namespaces.addAll(getNamespaces(targetSchemaModel));

		TreeNode targetTreeMap = generateTreeMap(docBuilder, namespaces, crosswalkModel, targetSchemaModel, true, true); 

		Element stylesheet = doc.createElementNS(xslNS, "xsl:stylesheet");
		stylesheet.setAttribute("version", "3.0");
		stylesheet.setAttribute("xmlns:f", funcNS);
		doc.appendChild(stylesheet);
		Element rootElement = addTemplatesXMLtoXML(stylesheet, targetTreeMap, namespaces, sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel, null, true, true);
		addRootJSONInputAndOutputTemplate(geTemplateNameFromRootElement(rootElement), stylesheet);		
		return toString(doc);	
	}	
	public String generateCSVtoCSV(String sourceSchemaURI, Model sourceSchemaModel, Model crosswalkModel, Model targetSchemaModel) throws Exception {
		return null;
	}
	public String generateXMLtoCSV(String sourceSchemaURI, Model sourceSchemaModel, Model crosswalkModel, Model targetSchemaModel, List<MappingInfoDTO> mappings) throws Exception {
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		Document doc = docBuilder.newDocument();

		final List<String> namespaces = getNamespaces(sourceSchemaModel);
		namespaces.addAll(getNamespaces(targetSchemaModel));

		TreeNode targetTreeMap = generateTreeMap(docBuilder, namespaces, crosswalkModel, targetSchemaModel, false, false); 

		Element stylesheet = doc.createElementNS(xslNS, "xsl:stylesheet");
		stylesheet.setAttribute("version", "2.0");
		doc.appendChild(stylesheet);
		
		String defaultSeparator = ";";
		String defaultNewline = "\n";
		Element output = doc.createElementNS(xslNS, "output");
		stylesheet.appendChild(output);
		output.setAttribute("method", "text");
		Element separatorVariable = doc.createElementNS(xslNS, "variable");
		stylesheet.appendChild(separatorVariable);
		separatorVariable.setAttribute("name", "separator");
		separatorVariable.setAttribute("select", "'" + defaultSeparator + "'");
		Element newlineVariable = doc.createElementNS(xslNS, "variable");
		stylesheet.appendChild(newlineVariable);
		newlineVariable.setAttribute("name", "newline");
		newlineVariable.setAttribute("select", "'" + defaultNewline + "'");
		
		Element rootTemplate = doc.createElementNS(xslNS, "template");
		stylesheet.appendChild(rootTemplate);
		rootTemplate.setAttribute("match", "/");
		
		// generate header row
		List<String> cols = new ArrayList<String>();
		for (MappingInfoDTO m : mappings) {
			String colLabel = m.getTarget().get(0).getLabel();
			if (!colLabel.startsWith("iterator source")) { // remove this!
				cols.add("'" + colLabel + "'");
				cols.add("$separator");
			}

		}
		Element headerValueOf = doc.createElementNS(xslNS, "value-of");
		headerValueOf.setAttribute("select", "concat(" + String.join(",", cols) + ")");
		rootTemplate.appendChild(headerValueOf);
		Element newLineValue = doc.createElementNS(xslNS, "value-of");
		newLineValue.setAttribute("select", "$newline");
		rootTemplate.appendChild(newLineValue);
		
		
		List<IteratorData> rowIterators = getIterators(crosswalkModel);
		for (IteratorData iteratorData : rowIterators) {
			Element forEach = doc.createElementNS(xslNS, "for-each");
			Resource iteratorSourceProperty = sourceSchemaModel.getResource(iteratorData.sourcePropertyURI);
			String iteratorPath = iteratorSourceProperty.getProperty(MSCR.instancePath).getString();
			rootTemplate.appendChild(forEach);
			forEach.setAttribute("select", "$data" + iteratorPath);
			for (MappingInfoDTO mappingInfo : mappings) {
				String sourceValuePath = null;
				if(mappingInfo.getProcessing() == null && 
						mappingInfo.getSource().stream().filter(NodeInfo::hasProcessing).collect(Collectors.toList()).isEmpty() &&
						mappingInfo.getTarget().stream().filter(NodeInfo::hasProcessing).collect(Collectors.toList()).isEmpty()) {
					
					NodeInfo sourceNode = mappingInfo.getSource().get(0);
					Resource sourceProperty = sourceSchemaModel.getResource(sourceNode.getUri());
					String instancePath = sourceProperty.getProperty(MSCR.instancePath).getString();
					sourceValuePath = instancePath.substring(iteratorPath.length());
				}
				Element valueOf = doc.createElementNS(xslNS, "value-of");
				valueOf.setAttribute("select", sourceValuePath);
				forEach.appendChild(valueOf);
				Element valueOf2 = doc.createElementNS(xslNS, "value-of");
				valueOf2.setAttribute("select", "$separator");
				forEach.appendChild(valueOf2);
			}
			Element newLineValue2 = doc.createElementNS(xslNS, "value-of");
			newLineValue2.setAttribute("select", "$newline");
			forEach.appendChild(newLineValue2);
		}		
		
		
		return toString(doc);	
	}	
	
	private List<IteratorData> getIterators(Model inputModel) {
		String q ="""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>				
PREFIX : <http://uri.suomi.fi/datamodel/ns/mscr#>				
select distinct ?targetShapeURI ?sourcePropertyURI ?mappingURI
where {
  ?mappingURI a :Mapping .
  ?mappingURI :target/rdf:_1 ?target .
  ?mappingURI :source/rdf:_1/:id ?sourcePropertyURI .
  ?target :id ?targetShapeURI .
  ?target :label ?targetLabel .
  FILTER(?targetLabel=\"iterator source\")
}						
				""";
		QueryExecution qe = QueryExecutionFactory.create(q, inputModel);
		ResultSet results = qe.execSelect();
		
		List<IteratorData> list = new ArrayList<IteratorData>();
		while(results.hasNext()) {
			QuerySolution soln = results.next();
			String iteratorPropertyURI = soln.get("targetShapeURI").asLiteral().getString();
			String targetShapeURI = iteratorPropertyURI.substring(9); 
			list.add(new IteratorData(
					iteratorPropertyURI,
					targetShapeURI,
					soln.get("sourcePropertyURI").asLiteral().getString(),
					soln.get("mappingURI").asResource().getURI()
			));
		}
		
		return list;
	}
	
	private void addRootJSONInputTemplate(String rootTemplateName, Element stylesheet) {
		Document doc = stylesheet.getOwnerDocument();
		Element rootTemplate = doc.createElementNS(xslNS, "xsl:template");
		rootTemplate.setAttribute("match", "/");
		Element callTemplate = doc.createElementNS(xslNS, "call-template");
		callTemplate.setAttribute("name", rootTemplateName);
		rootTemplate.appendChild(callTemplate);
		Element withParam = doc.createElementNS(xslNS, "with-param");
		withParam.setAttribute("name", "node");
		withParam.setAttribute("select", "json-to-xml(./*)");
		callTemplate.appendChild(withParam);
		stylesheet.appendChild(rootTemplate);
	}
	
	private void addRootJSONOutputTemplate(String rootTemplateName, Element stylesheet) {
		Document doc = stylesheet.getOwnerDocument();
		Element output = doc.createElementNS(xslNS, "xsl:output");
		output.setAttribute("method", "text");
		stylesheet.appendChild(output);
		
		Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");
		valueOf.setAttribute("select", "xml-to-json($jsonoutput)");
		Element variable = doc.createElementNS(xslNS, "xsl:variable");
		variable.setAttribute("name", "jsonoutput");
		
		Element rootTemplate = doc.createElementNS(xslNS, "xsl:template");
		rootTemplate.setAttribute("match", "/");
		Element callTemplate = doc.createElementNS(xslNS, "call-template");
		callTemplate.setAttribute("name", rootTemplateName);
		
		Element withParam = doc.createElementNS(xslNS, "with-param");
		withParam.setAttribute("name", "node");
		withParam.setAttribute("select", ".");		
		callTemplate.appendChild(withParam);
		
		variable.appendChild(callTemplate);
		rootTemplate.appendChild(variable);
		rootTemplate.appendChild(valueOf);
		stylesheet.appendChild(rootTemplate);
	}	
	
	private void addRootJSONInputAndOutputTemplate(String rootTemplateName, Element stylesheet) {
		Document doc = stylesheet.getOwnerDocument();
		Element output = doc.createElementNS(xslNS, "xsl:output");
		output.setAttribute("method", "text");
		stylesheet.appendChild(output);
		
		Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");
		valueOf.setAttribute("select", "xml-to-json($jsonoutput)");
		Element variable = doc.createElementNS(xslNS, "xsl:variable");
		variable.setAttribute("name", "jsonoutput");
		
		Element rootTemplate = doc.createElementNS(xslNS, "xsl:template");
		rootTemplate.setAttribute("match", "/");
		Element callTemplate = doc.createElementNS(xslNS, "call-template");
		callTemplate.setAttribute("name", rootTemplateName);
		
		Element withParam = doc.createElementNS(xslNS, "with-param");
		withParam.setAttribute("name", "node");
		withParam.setAttribute("select", "json-to-xml(./*)");		
		callTemplate.appendChild(withParam);
		
		variable.appendChild(callTemplate);
		rootTemplate.appendChild(variable);
		rootTemplate.appendChild(valueOf);
		stylesheet.appendChild(rootTemplate);
	}		
	private void addRootJSONtoXMLTemplate(String rootTemplateName, Element stylesheet) {
		Document doc = stylesheet.getOwnerDocument();
		Element rootTemplate = doc.createElementNS(xslNS, "xsl:template");
		rootTemplate.setAttribute("match", "/");
		
		Element rootVariable = doc.createElementNS(xslNS, "xsl:variable");
		rootVariable.setAttribute("name", "xml");
		rootTemplate.appendChild(rootVariable);

		Element rootValueOf = doc.createElementNS(xslNS, "xsl:value-of");
		rootValueOf.setAttribute("select", "xml-to-json($xml)");
		rootTemplate.appendChild(rootValueOf);
		
		Element callTemplate = doc.createElementNS(xslNS, "call-template");
		callTemplate.setAttribute("name", rootTemplateName);
		rootTemplate.appendChild(callTemplate);
		Element withParam = doc.createElementNS(xslNS, "with-param");
		withParam.setAttribute("name", "node");
		withParam.setAttribute("select", ".");
		callTemplate.appendChild(withParam);		
		
		stylesheet.appendChild(rootTemplate);
	}	
	
	public String getXPathFromJsonPath(String path, String schemaURI, Model inputModel) {
		// Use just key attribute values = name of the property 
		// add <map> when necessary
		
		// /*[@key = 'propertyname']
		
		// check whether root element is array or object <f:array> or <f:map>
		// $.vegetables[*].veggieLike -> /f:map/*[@key='vegetables']/*/*[@key='veggieLike']
		// $.vegetables[*] -> /f:map/*[@key = 'vegetables']/*
		Resource rootProperty = inputModel.getResource(schemaURI + "#root");
		String firstElement = "*";
		if(rootProperty.getProperty(SH.maxCount) != null && rootProperty.getProperty(SH.maxCount).getInt() == 1) {
			firstElement = "f:map";
		}
		if(path.equals("/") || path.equals("$")) {
			//if(firstElement.equals("f:array")) {
				// is it an array of objects? - assuming yes for now
				firstElement =  firstElement + "/*";
				
			//}
		}
		String[] parts = path.split("\\.");
		String xpath = "/" + firstElement;
		String schemaPath = "$";
		if(parts[0].equals("$[*]")) { // root element is an array, need extra "hop"
			xpath = "/*" + xpath; 
		}
		for(int i = 1; i < parts.length; i++) {
			String key = parts[i];
			boolean isArray = false;
			if(key.endsWith("[*]")) {
				isArray = true;
				key = key.substring(0, key.length()-3);
			}
			schemaPath = schemaPath + "." + key;
			ResIterator pi = inputModel.listResourcesWithProperty(MSCR.schemaPath, schemaPath);
			if(pi.hasNext()) {
				xpath = xpath + "/*[@key='" + key + "']";
				if(isArray) {
					xpath = xpath + "/*";
				}
			}
			else {
				throw new RuntimeException("Schema path " + schemaPath + " missing from source model");
			}			
		}
		
		return xpath;
	}
	
	
	private String geTemplateNameFromRootElement(Element e) {
		Node p = e.getParentNode();
		if(p.getLocalName().equals("template")) {
			return p.getAttributes().getNamedItem("name").getNodeValue();			
		}
		p = p.getParentNode();
		if(p.getLocalName().equals("template")) {
			return p.getAttributes().getNamedItem("name").getNodeValue();			
		}
		
		p = p.getParentNode();
		if(p.getLocalName().equals("template")) {
			return p.getAttributes().getNamedItem("name").getNodeValue();			
		}

		throw new RuntimeException("No root template name found");
	}
	
	private void addRootTemplate(String rootTemplateName, Element stylesheet) {
		Document doc = stylesheet.getOwnerDocument();
		Element rootTemplate = doc.createElementNS(xslNS, "xsl:template");
		rootTemplate.setAttribute("match", "/");
		Element callTemplate = doc.createElementNS(xslNS, "call-template");
		callTemplate.setAttribute("name", rootTemplateName);
		rootTemplate.appendChild(callTemplate);
		Element withParam = doc.createElementNS(xslNS, "with-param");
		withParam.setAttribute("name", "node");
		withParam.setAttribute("select", ".");
		callTemplate.appendChild(withParam);
		stylesheet.appendChild(rootTemplate);
	}	
	
	private TreeNode generateTargetMap(Element element, boolean isJSONOutput) {
		String propertyURI = element.getAttribute("propertyURI");
		String mappingURI = element.getAttribute("mappingURI");
		boolean isAttribute = Boolean.parseBoolean(element.getAttribute("isAttribute"));
		String datatype = element.getAttribute("datatype");
		TreeNode node = new TreeNode(new TreeMap<Integer, TreeNode>(), propertyURI, mappingURI, element.getNodeName(), element.getAttribute("namespace"), isAttribute, datatype);
		NodeList _list = element.getChildNodes();
		int order = 0;
		for (int i = 0; i < _list.getLength(); i++) {			
			Element childElement = (Element)_list.item(i);
			if(childElement.getParentNode() == element) {
				
				if(!isJSONOutput && childElement.hasAttribute("order")) {
					try {
						order = Integer.parseInt(childElement.getAttribute("order"));
					}catch(Exception e) {
						order = (-i);
					}
				}
				node.children.put(
						//(isJSONOutput || !childElement.hasAttribute("order")) ? order : Integer.parseInt(childElement.getAttribute("order")),
						order,
						generateTargetMap(childElement, isJSONOutput)
				);						
				order++;
			}
		}
		return node;
	}
	

	private Element addTemplatesXMLtoXML(Element stylesheet, TreeNode targetTreeMap, List<String> namespaces,
			String sourceSchemaURI, Model sourceSchemaModel, Model crosswalkModel, Model targetSchemaModel, TemplateInfo ti, boolean isJSONSource, boolean isJSONTarget) {
		ti = addTemplate(targetTreeMap, stylesheet, sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel, ti, isJSONSource, isJSONTarget);
		for(TreeNode child : targetTreeMap.children.values()) {
			
			addTemplatesXMLtoXML(stylesheet, child, namespaces, sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel, ti, isJSONSource, isJSONTarget);
		}
		return ti.contentElement;
	}

	private boolean isRepeatable(String propertyURI, Model model) {
		Resource prop = model.getResource(propertyURI);
		if(prop.hasProperty(SH.maxCount)) {
			int count = prop.getProperty(SH.maxCount).getObject().asLiteral().getInt();
			return count > 1;
		}
		return true;
	}
	
	MappingMapper mapper = new MappingMapper();

	record TemplateInfo(Element contentElement, String prevIteratorPath, boolean isRepeatable, boolean isObjectProperty) {}
	
	
	private String mapXSDtoJSONXMLDatatype(String uri) {
		switch (uri) {
		case "http://www.w3.org/2001/XMLSchema#integer":
		case "http://www.w3.org/2001/XMLSchema#number": {
			return "number";
		}
		case "http://www.w3.org/2001/XMLSchema#boolean": {
			return "boolean";
		}
		default:
			return "string";
		}
	}
	
	private String getNamespaceAgnosticInstancePath(String path, boolean isAttribute) {
		String newPath = "";
		if(path.startsWith("$node")) {
			path = path.substring(5);
			newPath = "$node";
		}
		String[] parts = path.split("/");
		
		for(int i = 1; i < parts.length; i++) {
			if((i == parts.length - 1) && isAttribute) {
				newPath = newPath + "/" + parts[i];
						
			}
			else {
				newPath = newPath + "/*[local-name() = '" + parts[i] + "']";	
			}
			
		}
		System.out.println(newPath);
		return newPath;
	}
	private TemplateInfo addTemplate(TreeNode target, Element stylesheet, String sourceSchemaURI, Model sourceSchemaModel, Model crosswalkModel, Model targetSchemaModel, TemplateInfo prevTi, boolean isJSONSource, boolean isJSONTarget) {
		Element parentElement = prevTi == null ? null : prevTi.contentElement;
		boolean isLeafElement = isLeafElement(target);
		boolean isLeafElementOrAttribute = isLeafElementOrAttribute(target);
		boolean isMapped = (target.mappingURI != null && !target.mappingURI.equals(""));
		boolean isParentRepeatable = prevTi == null ? false : prevTi.isRepeatable;
		
		Document doc = stylesheet.getOwnerDocument();
		String prevIteratorPath = (prevTi == null) ? "" : prevTi.prevIteratorPath;
		
		String templateName = "template_" + target.targetElementName.replaceAll(":", "-") + "_" + UUID.randomUUID().toString();
		Element templateElement = doc.createElementNS(xslNS, "xsl:template");
		templateElement.setAttribute("name", templateName);
		Element param = doc.createElementNS(xslNS, "xsl:param");
		param.setAttribute("name", "node");
		templateElement.appendChild(param);
		stylesheet.appendChild(templateElement);

		Element contentRoot = null;
		Element contentElement = null;
		if(isLeafElement) {
			if(isJSONTarget) {
				contentElement = doc.createElementNS(funcNS, mapXSDtoJSONXMLDatatype(target.datatype));
				Resource propertyResource = targetSchemaModel.getResource(target.targetPropertyURI).asResource();
				Resource propertyNodeShape = targetSchemaModel.listSubjectsWithProperty(SH.property, propertyResource).nextResource();
				ResIterator resi = targetSchemaModel.listSubjectsWithProperty(SH.node, propertyNodeShape);
				if(resi.hasNext()) {
					Resource propertyParentPropResource = resi.next();
					if(propertyParentPropResource.getProperty(SH.maxCount) == null || propertyParentPropResource.getProperty(SH.maxCount).getInt() > 1) {
						contentElement.setAttribute("key", target.targetElementName);	
					}
				}							
			}
			else {
				if(target.isAttribute) {
					contentElement = doc.createElementNS(xslNS, "xsl:attribute");
					contentElement.setAttribute("name", target.targetElementName);
				}
				else {
					contentElement = doc.createElementNS(target.targetElementNamespace, target.targetElementName);
				}
			}

		}
		else {

			contentElement = doc.createElementNS(target.targetElementNamespace, target.targetElementName);	
				
		}
		

		// add for-each if targetElement is repeatable
		boolean isTargetRepeatable = isRepeatable(target.targetPropertyURI, targetSchemaModel);
		if(isTargetRepeatable && !target.isAttribute) {
			contentRoot = doc.createElementNS(xslNS, "xsl:for-each");
			contentRoot.setAttribute("select", "$node");	
			
			contentRoot.appendChild(contentElement);
		}
		else {
			contentRoot = contentElement;
		}
		
		if(isMapped) {
			MappingInfoDTO mappingInfo = mapper.mapToMappingDTO(target.mappingURI, crosswalkModel);

			if(isLeafElementOrAttribute) {
				if(mappingInfo.getProcessing() == null && 
						mappingInfo.getSource().stream().filter(NodeInfo::hasProcessing).collect(Collectors.toList()).isEmpty() &&
						mappingInfo.getTarget().stream().filter(NodeInfo::hasProcessing).collect(Collectors.toList()).isEmpty()) {
					// just copy the value over 
					NodeInfo sourceNode = mappingInfo.getSource().get(0);
					Resource sourceProperty = sourceSchemaModel.getResource(sourceNode.getUri());
					Element contentValueOf = doc.createElementNS(xslNS, "value-of");
					//String iteratorPath = getNamespaceAgnosticInstancePath(sourceProperty.getProperty(MSCR.instancePath).getString(), target.isAttribute);
					String iteratorPath = sourceProperty.getProperty(MSCR.instancePath).getString();
					if(isJSONSource) {
						iteratorPath = getXPathFromJsonPath(iteratorPath, sourceSchemaURI, sourceSchemaModel);
						
					}
					String pathSuffix = iteratorPath;
					if(iteratorPath.startsWith(prevIteratorPath)) {
						pathSuffix = iteratorPath.substring(prevIteratorPath.length());
					}
					if(!isJSONSource) {
						pathSuffix = getNamespaceAgnosticInstancePath(pathSuffix, target.isAttribute);
					}
					
					if(isTargetRepeatable) {
						contentValueOf.setAttribute("select", ".");
						contentRoot.setAttribute("select", "$node" + pathSuffix);
						
					}
					else {
						contentValueOf.setAttribute("select", "$node" + pathSuffix);	
					}
					if((isLeafElement && !hasAttributes(target)) || !isLeafElement) {
						contentElement.appendChild(contentValueOf);	
					}
												
					templateElement.appendChild(contentRoot);
					if(isTargetRepeatable) {
						prevIteratorPath = iteratorPath;
					}
					
					
				}
				else {
					// add function elements
					int targetNodeInfoIndex = getTargetNodeInfo(mappingInfo.getTarget(), target.targetPropertyURI);
					contentElement = addProcessingTemplates(mappingInfo, sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel, templateElement, prevTi, targetNodeInfoIndex, target, isJSONSource, isJSONTarget);
					
				}
			

			}
			else {
				// mapped "middle" element 
				// modify $node context 
				// functions do not apply
				NodeInfo sourceNode = mappingInfo.getSource().get(0);
				Resource sourceProperty = sourceSchemaModel.getResource(sourceNode.getId());
				
				if(sourceProperty.hasProperty(MSCR.instancePath)) {
					prevIteratorPath = sourceProperty.getProperty(MSCR.instancePath).getString();	
				}
				else {
					prevIteratorPath = "/";
				}
				
				if(isJSONSource) {
					prevIteratorPath = getXPathFromJsonPath(prevIteratorPath, sourceSchemaURI, sourceSchemaModel);
				}
				
				String pathSuffix = prevIteratorPath.substring(prevTi == null ? 0 : prevTi.prevIteratorPath.length());	
				if(!isJSONSource) {
					pathSuffix = getNamespaceAgnosticInstancePath(pathSuffix, target.isAttribute);
				}
				
				contentRoot.setAttribute("select", "$node" + pathSuffix );
				templateElement.appendChild(contentRoot);
				//prevIteratorPath = pathSuffix; // ?
			}
			
			
			
		}
		else {
			// just the content element - no attributes
			templateElement.appendChild(contentRoot);
		}
		
		if(parentElement != null) {
			if(isJSONTarget) {
				if(isTargetRepeatable) {
					Element newContentRoot = doc.createElementNS(funcNS, "f:array");
					newContentRoot.setAttribute("key", target.targetElementName);
							
					newContentRoot.appendChild(contentRoot);
					templateElement.appendChild(newContentRoot);
				}
				else {
					contentRoot.setAttribute("key", target.targetElementName);
				}
				if(!isLeafElement) {
					doc.renameNode(contentElement, funcNS, "f:map");	
				}
				
			}
			
			// add call to the created template
			Element callTemplate = doc.createElementNS(xslNS, "xsl:call-template");
			callTemplate.setAttribute("name", templateName);

			Element withParam = doc.createElementNS(xslNS, "xsl:with-param");
			callTemplate.appendChild(withParam);
			withParam.setAttribute("name", "node");
			/*
			if(isParentRepeatable) {
				withParam.setAttribute("select", ".");
			}
			else {
				withParam.setAttribute("select", "$node");
			}*/			
				
			withParam.setAttribute("select", ".");	
			System.out.println(target.targetPropertyURI);
			System.out.println(prevTi);
			//if(isJSONTarget && isLeafElement && isParentRepeatable && ) {
			//	withParam.setAttribute("select", "./*");	
			//}

			if(target.isAttribute) {
				Element firstChild = (Element)parentElement.getFirstChild();
				parentElement.insertBefore(callTemplate, firstChild);
				
			}
			else {
				parentElement.appendChild(callTemplate);
				
			}
			
			
		}
		/*
		if(isLeafElement && hasAttributes(target)) {
			for(TreeNode c : target.children.values()) {
				System.out.println(target.targetElementName);
				System.out.println(c.targetElementName);
				System.out.println("---");
				
				//TemplateInfo tempTi = new TemplateInfo(contentElement, prevIteratorPath, isTargetRepeatable);
				//TemplateInfo attrTi = addTemplate(c, stylesheet, sourceSchemaURI, sourceSchemaModel, crosswalkModel, targetSchemaModel, tempTi, isJSONSource, isJSONTarget);
				Element callTemplate2 = doc.createElementNS(xslNS, "xsl:call-template");
				callTemplate2.setAttribute("name", "testing");

				Element withParam2 = doc.createElementNS(xslNS, "xsl:with-param");
				callTemplate2.appendChild(withParam2);
				withParam2.setAttribute("name", "node");
				
				withParam2.setAttribute("select", ".");
				
				contentElement.appendChild(callTemplate2);
			}	
		}		
*/
		boolean isObjectProp = false;
		if(!target.targetPropertyURI.equals("")) {
			Resource propertyResource = targetSchemaModel.getResource(target.targetPropertyURI).asResource();
			if(propertyResource != null) {
				System.out.println("*"+ propertyResource);
				isObjectProp = propertyResource.getPropertyResourceValue(DCTerms.type).getURI().equals(OWL.ObjectProperty.getURI());
			}
			
		}
		
		return new TemplateInfo(contentElement, prevIteratorPath, isTargetRepeatable, isObjectProp);
	}
	
	

	private int getTargetNodeInfo(List<NodeInfo> targets, String targetPropertyURI) {
		for(int i = 0; i< targets.size(); i++) {
			if(targets.get(i).getUri().equals(targetPropertyURI)) {
				return i;
			}
		}
		throw new RuntimeException("Could not get the target node info based on target property URI");	
	}

	private Element addProcessingTemplates(MappingInfoDTO mappingInfo, String sourceSchemaURI, Model sourceSchemaModel, Model crosswalkModel, Model targetSchemaModel, Element templateElement, TemplateInfo ti, int targetNodeIndex, TreeNode target, boolean isJSONSource, boolean isJSONTarget ) {
		Document doc = templateElement.getOwnerDocument();
		
		int sourceIndex = 0;
		for (NodeInfo sourceNode : mappingInfo.getSource()) {			
			Element variable = doc.createElementNS(xslNS, "xsl:variable");
			variable.setAttribute("name", "source_var_" + sourceIndex);
			String selectValue = getSourceFunctionSelect(templateElement, sourceSchemaURI, sourceSchemaModel, sourceNode, ti, isJSONSource, mappingInfo);
			variable.setAttribute("select", selectValue);
			templateElement.appendChild(variable);			
			sourceIndex++;
		}
		// add input sequence variable for mapping function
		Element mappingInputVariable = doc.createElementNS(xslNS, "xsl:variable");
		mappingInputVariable.setAttribute("name", "mapping_func_input");
		mappingInputVariable.setAttribute("select", "(" + IntStream.range(0, mappingInfo.getSource().size()).boxed().map(n -> "$source_var_" + n).collect(Collectors.joining(", ")) + ")");
		templateElement.appendChild(mappingInputVariable);

		Element mappingOutputVariable = doc.createElementNS(xslNS, "xsl:variable");
		mappingOutputVariable.setAttribute("name", "mapping_func_output");	
		if(mappingInfo.getProcessing() == null) { // default to join
			mappingOutputVariable.setAttribute("select", "$mapping_func_input[" + sourceIndex + "]");
		}
		else {
			mappingOutputVariable.setAttribute("select", getFunctionSelect(templateElement, "$mapping_func_input", mappingInfo.getProcessing(), sourceSchemaURI, sourceSchemaModel, mappingInfo, isJSONSource));
		}
		templateElement.appendChild(mappingOutputVariable);
		NodeInfo targetNodeInfo = mappingInfo.getTarget().get(targetNodeIndex);

		if(mappingInfo.getTarget().size() > 1) {
			// create specific element
			Element targetElement = null;
			if(isJSONTarget) {
				targetElement = doc.createElementNS(funcNS, mapXSDtoJSONXMLDatatype(target.datatype));
				Resource propertyResource = targetSchemaModel.getResource(target.targetPropertyURI).asResource();
				Resource propertyNodeShape = targetSchemaModel.listSubjectsWithProperty(SH.property, propertyResource).nextResource();
				ResIterator resi = targetSchemaModel.listSubjectsWithProperty(SH.node, propertyNodeShape);
				/*
				if(resi.hasNext()) {
					Resource propertyParentPropResource = resi.next();
//					if(propertyParentPropResource.getProperty(SH.maxCount) == null || propertyParentPropResource.getProperty(SH.maxCount).getInt() > 1) {
					if(propertyParentPropResource.getProperty(SH.maxCount) != null && propertyParentPropResource.getProperty(SH.maxCount).getInt()== 1) {					
						targetElement.setAttribute("key", target.targetElementName);	
					}
				}*/
				targetElement.setAttribute("key", target.targetElementName);
				
			}
			else {
				if(target.isAttribute) {
					targetElement = doc.createElementNS(xslNS, "xsl:attribute");
					targetElement.setAttribute("name", target.targetElementName);
						
				}
				else {
					targetElement = doc.createElementNS(target.targetElementNamespace, target.targetElementName);	
				}
					
			}
			 
			templateElement.appendChild(targetElement);
			
			Element targetValueOf = doc.createElementNS(xslNS, "xsl:value-of");

			if(targetNodeInfo.getProcessing() == null) {
				// get value of the mapping_func_output with the target index
				targetValueOf.setAttribute("select", "$mapping_func_output[" + (targetNodeIndex + 1) + "]");	
			}
			else {
				targetValueOf.setAttribute("select", getFunctionSelect(targetElement, "$mapping_func_output[" + (targetNodeIndex + 1) + "]", targetNodeInfo.getProcessing(), sourceSchemaURI, sourceSchemaModel, mappingInfo, isJSONSource));
			}
			targetElement.appendChild(targetValueOf);
			return targetElement;
		}
		else if(target.isAttribute) {
			Element attribute = doc.createElementNS(xslNS, "xsl:attribute");
			attribute.setAttribute("name", target.targetElementName);
			Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");
			valueOf.setAttribute("select", "$mapping_func_output");
			attribute.appendChild(valueOf);
			templateElement.appendChild(attribute);
			return attribute;
		}
		else {
			Element targetForEach = doc.createElementNS(xslNS, "xsl:for-each");
			targetForEach.setAttribute("select", "$mapping_func_output");
			templateElement.appendChild(targetForEach);
			
			// loop the same element
			Element targetElement = null;
			if(isJSONTarget) {
				targetElement = doc.createElementNS(funcNS, mapXSDtoJSONXMLDatatype(target.datatype));
				Resource propertyResource = targetSchemaModel.getResource(target.targetPropertyURI).asResource();
				System.out.println(propertyResource);
				targetElement.setAttribute("key", target.targetElementName);
				/*
				Resource propertyNodeShape = targetSchemaModel.listSubjectsWithProperty(SH.property, propertyResource).nextResource();
				ResIterator resi = targetSchemaModel.listSubjectsWithProperty(SH.node, propertyNodeShape);
				if(propertyResource.getPropertyResourceValue(DCTerms.type).getURI().equals(OWL.DatatypeProperty.getURI())) {
					targetElement.setAttribute("key", target.targetElementName);
				}				
				else if(resi.hasNext()) {
					Resource propertyParentPropResource = resi.next();

					//if(propertyParentPropResource.getProperty(SH.maxCount) == null || propertyParentPropResource.getProperty(SH.maxCount).getInt() > 1) {
					if(propertyParentPropResource.getProperty(SH.maxCount) != null && propertyParentPropResource.getProperty(SH.maxCount).getInt()== 1) {					
						targetElement.setAttribute("key", target.targetElementName);	
					}
				}
				*/	
			}
			else {
				targetElement = doc.createElementNS(target.targetElementNamespace, target.targetElementName);	
			}
			targetForEach.appendChild(targetElement);
			Element targetValueOf = doc.createElementNS(xslNS, "xsl:value-of");
			if(targetNodeInfo.getProcessing() == null) {
				targetValueOf.setAttribute("select", ".");	
			}
			else {
				targetValueOf.setAttribute("select", getFunctionSelect(targetElement, ".", targetNodeInfo.getProcessing(), sourceSchemaURI, sourceSchemaModel, mappingInfo, isJSONSource));
			}
			
			
			targetElement.appendChild(targetValueOf);
			return targetElement;
		}
		
		
		/*
		String targetSelect = "$mapping_" + targetNodeIndex;
		if (mappingInfo.getTarget().get(targetNodeIndex).getProcessing() != null) {
			targetSelect = getFunctionSelect("$mapping_var_" +  targetNodeIndex, mappingInfo.getTarget().get(targetNodeIndex).getProcessing());
		}
		Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");
		valueOf.setAttribute("select", targetSelect);
		contentElement.appendChild(valueOf);
		*/	
	}

	private String getFunctionSelect(Element e, String valueSource, ProcessingInfo pi, String sourceSchemaURI, Model sourceSchemaModel, MappingInfoDTO mapping, boolean isJSONSource) {
		String id = pi.getId();
		if(id.equals("http://uri.suomi.fi/datamodel/ns/mscr#toString")) {
			return "string(" + valueSource + ")";
		}
		if(id.equals("http://uri.suomi.fi/datamodel/ns/mscr#toNumber")) {
			return "number(" + valueSource + ")";
		}

		if(id.equals("http://uri.suomi.fi/datamodel/ns/mscr#addPrefixFunc")) {
			String prefix = pi.getParams().get("prefix").toString();
			return "concat('" + prefix +"', " + valueSource + ")";
		}
		if(id.equals("http://uri.suomi.fi/datamodel/ns/mscr#addSuffixFunc")) {
			String suffix = pi.getParams().get("suffix").toString();
			return "concat(" + valueSource +",'" + suffix +"')";
		}	
		if(id.equals("http://uri.suomi.fi/datamodel/ns/mscr#concatFunc")) {
			String delimiter = pi.getParams().get("delimiter").toString();
			return "string-join(" + valueSource + ", '" + delimiter + "')";

		}		
		if(id.equals("http://uri.suomi.fi/datamodel/ns/mscr#splitFunc")) {
			String delimiter = pi.getParams().get("delimiter").toString();
			return "tokenize(" + valueSource + ", '" + delimiter + "')";

		}
		if(id.equals("http://uri.suomi.fi/datamodel/ns/mscr#normalizeSpaceFunc")) {
			return "normalize-space(" + valueSource + ")";
		}		
		if(id.equals("http://uri.suomi.fi/datamodel/ns/mscr#constantFunc")) {
			String value = pi.getParams().get("value").toString();
			return "'" + value + "'";

		}	
		if(id.equals("http://uri.suomi.fi/datamodel/ns/mscr#regexGetGroupFunc")) {
			String regex = pi.getParams().get("regex").toString();
			Element analyzeString = e.getOwnerDocument().createElementNS(xslNS, "xsl:analyze-string");
			analyzeString.setAttribute("select", valueSource);
			analyzeString.setAttribute("regex", regex);
			Element match = e.getOwnerDocument().createElementNS(xslNS, "xsl:matching-substring");
			Element variable = e.getOwnerDocument().createElementNS(xslNS, "xsl:variable");
			String varName = "var_" + UUID.randomUUID().toString();
			variable.setAttribute("name", varName);
			Element valueOf = e.getOwnerDocument().createElementNS(xslNS, "xsl:value-of");
			valueOf.setAttribute("select", "regex-group(1)");
			match.appendChild(valueOf);
			
			analyzeString.appendChild(match);
			variable.appendChild(analyzeString);
			e.appendChild(variable);
			return "($" + varName + ")";
			
			
		}		
		if(id.equals("http://uri.suomi.fi/datamodel/ns/mscr#filterFunc")) {
			String valueParam = pi.getParams().get("value").toString();
			String propertyParam = pi.getParams().get("property").toString();
			int typePropIndex = -1;
			String filterPath = "";
			String targetPath = "";
			List<NodeInfo> sources = mapping.getSource();
			if(sources.size() != 2) {
				throw new RuntimeException("Filter function requires exactly two source properties");
			}
			for(int i = 0; i < sources.size(); i++) {
				String propertyURI = sources.get(i).getUri();
				Resource property = sourceSchemaModel.getResource(propertyURI);
				String instancePath = property.getProperty(MSCR.instancePath).getString();
				String propertyName = "";
				if(isJSONSource) {
					propertyName = instancePath.substring(instancePath.lastIndexOf(".") + 1);
				}
				else {
					propertyName = instancePath.substring(instancePath.lastIndexOf("/") + 1);
				}
				 
				if(propertyName.equals(propertyParam)) {
					filterPath = instancePath;
				}
				else {
					typePropIndex = i;
					targetPath = instancePath;
				}
				
			}			
			String pathDiff = "";
			if(isJSONSource) {
				pathDiff = calculateJSONPathDiff(
						getXPathFromJsonPath(targetPath, sourceSchemaURI, sourceSchemaModel),
						getXPathFromJsonPath(filterPath, sourceSchemaURI, sourceSchemaModel) );
				return "$source_var_" + typePropIndex + "[" + pathDiff + " and text()='" + valueParam + "']]";
			}
			else {
				pathDiff = calculatePathDiff(targetPath, filterPath);
				return "$source_var_" + typePropIndex + "[" + pathDiff + "='" + valueParam + "']";
			}
			
			


		}		
		
		throw new RuntimeException("No handler found for processing function " + pi.getId());

	}
	private String calculateJSONPathDiff(String targetPath, String filterPath) {
		System.out.println("targetPath: " + targetPath);
		System.out.println("filterPath: " + filterPath);
		String[] targetParts = targetPath.split("/");
		String[] filterParts = filterPath.split("/");
		int i = 0;
		for(; i < (targetParts.length > filterParts.length ? filterParts.length : targetParts.length); i++) {
			if(!targetParts[i].equals(filterParts[i])) {
				break;
			}
			
			
		}
		String commonPrefix = String.join("/", java.util.Arrays.copyOfRange(targetParts, 0, i));
		// how many steps to go from target to common prefix?
		String tail = targetPath.substring(commonPrefix.length());
		String[] downStepParts = tail.split("/");
		String suffix = filterPath.substring(commonPrefix.length());
		int repeatCount = downStepParts.length;
		if(suffix.startsWith("/")) {
			repeatCount--;
			suffix = suffix.substring(1);			
		}
		String r = "../".repeat(repeatCount) + suffix;
		return StringUtils.removeEnd(r, "]");
	}
	private String calculatePathDiff(String targetPath, String filterPath) {
		System.out.println("targetPath: " + targetPath);
		System.out.println("filterPath: " + filterPath);
		String commonPrefix = StringUtils.getCommonPrefix(targetPath, filterPath);
		// how many steps to go from target to common prefix?
		
		String[] downStepParts = targetPath.substring(commonPrefix.length()).split("/");
		String suffix = filterPath.substring(commonPrefix.length());
		int repeatCount = downStepParts.length;
		if(suffix.startsWith("/")) {
			repeatCount--;
			suffix = suffix.substring(1);			
		}
		String r = "../".repeat(repeatCount) + suffix;
		return r;
	}
	
	private String getSourceFunctionSelect(Element e, String sourceSchemaURI, Model sourceSchemaModel, NodeInfo sourceNode, TemplateInfo ti, boolean isJSONSource, MappingInfoDTO mappingInfo) {
		Resource sourceProperty = sourceSchemaModel.getProperty(sourceNode.getUri());
		
		String valuePath = sourceProperty.getProperty(MSCR.instancePath).getString();
		if(isJSONSource) {
			valuePath = getXPathFromJsonPath(valuePath, sourceSchemaURI, sourceSchemaModel);
		}
		
		if(valuePath.startsWith(ti.prevIteratorPath)) {
			String prevPath = ti.prevIteratorPath; 
			valuePath = "$node" + valuePath.substring(prevPath.length());
//			if(valuePath.startsWith("$node/f:map")) {
//				valuePath = "$node" + valuePath.substring(11);
//			}	
			
		}
		if(!isJSONSource) {
			valuePath = getNamespaceAgnosticInstancePath(valuePath, sourceProperty.hasProperty(MSCR.sourceType));
		}
		
		if(sourceNode.getProcessing() != null) {
			return getFunctionSelect(e, valuePath, sourceNode.getProcessing(), sourceSchemaURI, sourceSchemaModel, mappingInfo, isJSONSource);

		}

		return valuePath;
	}
	private int addMappingFunction(int sourceIndex, ProcessingInfo processing, Element contentElement) {
		Document doc = contentElement.getOwnerDocument();
		if(processing == null) { // default to concat
			Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");
			String selectValue = IntStream.range(0, sourceIndex).boxed().map(n -> "$source_var_" + n).collect(Collectors.joining(", "));
			valueOf.setAttribute("select", selectValue);
			valueOf.setAttribute("separator", "");
			
			Element variable = doc.createElementNS(xslNS, "xsl:variable");
			variable.setAttribute("name", "mapping_var_0");
			variable.appendChild(valueOf);
			contentElement.appendChild(variable);
			return 1;
		}
		else {
			if(processing.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#concatFunc")) {
				String delimiter = processing.getParams().get("delimiter").toString();
				Element variable = doc.createElementNS(xslNS, "xsl:variable");
				variable.setAttribute("name", "mapping_var_0");
				variable.setAttribute("select", "string-join($mapping_func_input, \"" + delimiter + "\")");
				contentElement.appendChild(variable);
				
				return 1;
			}
			if(processing.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#splitFunc")) {
				String delimiter = processing.getParams().get("delimiter").toString();
				Element variable = doc.createElementNS(xslNS, "xsl:variable");
				variable.setAttribute("name", "mapping_var_0");
				variable.setAttribute("select", "tokenize(" + "$source_var_" + sourceIndex + ", \"" + delimiter + "\")");
				contentElement.appendChild(variable);
				
				return 1;
			}			

		}
		return 0;
		
	}


	private boolean isPartOfObjectArray(String propertyShapeUri, Model inputModel) {
		String q ="""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>				
PREFIX : <http://uri.suomi.fi/datamodel/ns/mscr#>	
prefix sh:    <http://www.w3.org/ns/shacl#> 			
select ?prop
where {
  ?r sh:property <%s> .
  ?prop sh:node ?r
}						
				""".formatted(propertyShapeUri);
		QueryExecution qe = QueryExecutionFactory.create(q, inputModel);
		ResultSet results = qe.execSelect();
		if(results.hasNext()) {
			Resource r = results.next().getResource("prop");
			if(r.getProperty(SH.maxCount) != null && r.getProperty(SH.maxCount).getInt() == 1) {
				return false;
			}
			return true;
		}
		else {
			return false;
		}
		
	}
	

	private boolean isMapped(TreeNode target, Model inputModel) {
		String q ="""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>				
PREFIX : <http://uri.suomi.fi/datamodel/ns/mscr#>	
prefix sh:    <http://www.w3.org/ns/shacl#> 			
select ?xpath ?targetProperty ?mapping ?order ?type
where {
  ?target a :Target .
  ?target :uri <%s>
}						
				""".formatted(target.targetPropertyURI);
		QueryExecution qe = QueryExecutionFactory.create(q, inputModel);
		ResultSet results = qe.execSelect();
		return results.hasNext();
	}
	
	private String getPropertyURIforPath(String path, Model model) {
		String q ="""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>				
PREFIX : <http://uri.suomi.fi/datamodel/ns/mscr#>	
prefix sh:    <http://www.w3.org/ns/shacl#> 			
select ?property
where {
  ?property a sh:PropertyShape .
  ?property :instancePath <%s>
}						
				""".formatted(path);
		QueryExecution qe = QueryExecutionFactory.create(q, model);
		ResultSet results = qe.execSelect();
		if(results.hasNext()) {
			return results.next().get("property").asResource().getURI();
		}
		else {
			return null;
		}
	}	

	private boolean isLeafElementOrAttribute(TreeNode target) {
		if(target.isAttribute) {
			return true;
		}
		if(target.children.isEmpty()) {
			return true;
		}
		boolean hasOnlyAttrChildren = true;
		for(TreeNode c : target.children.values()) {
			if(!c.isAttribute) {
				hasOnlyAttrChildren = false;
			}
		}
		if(hasOnlyAttrChildren) {
			return true;
		}				
 		return false;
	}
	
	private boolean isLeafElement(TreeNode target) {
		boolean hasOnlyAttrChildren = true;
		for(TreeNode c : target.children.values()) {
			if(!c.isAttribute) {
				hasOnlyAttrChildren = false;
			}
		}		
		if(target.children.isEmpty() || hasOnlyAttrChildren) {
			return true;
		}
 		return false;
	}	
	
	private boolean hasAttributes(TreeNode target) {
		boolean hasOnlyAttrChildren = false;
		for(TreeNode c : target.children.values()) {
			if(c.isAttribute) {
				hasOnlyAttrChildren = true;
			}
		}		
		
 		return hasOnlyAttrChildren;
	}	

	private List<String> getNamespaces(Model inputModel) {
		final List<String> namespaces = new ArrayList<String>();
		inputModel.listObjectsOfProperty(MSCR.namespace).forEach(new Consumer<RDFNode>() {		
			@Override
			public void accept(RDFNode t) {				
				namespaces.add((t.asResource().getURI()));
			}
		});		
		return namespaces;
	}
	record TargetInfo(String instancePath, String schemaPath, Resource propertyURI, Resource mappingURI, int order, boolean isAttribute, String namespace, String datatype) {}
	
	private Element generateTargetTree(Document targetDoc, List<String> namespaces, Model inputModel, boolean isJSONOutput, boolean isJSONInput) throws Exception {		
		List<TargetInfo> targetInfos = getTargetInfos(inputModel);
		XPathFactory xpathFactory = XPathFactory.newInstance();
		XPath xpath = xpathFactory.newXPath();
		Element root = targetDoc.createElement("root");
		targetDoc.appendChild(root);
		if(isJSONOutput) {
			// determine "root" element
			Element jsonRoot = targetDoc.createElementNS(funcNS, "f:map");
			root.appendChild(jsonRoot);
			root = jsonRoot;
		}
		for(TargetInfo ti : targetInfos) {
			addElementByPath(xpath, root, ti, inputModel, isJSONOutput, isJSONInput);	
		}
		return root;
		
	}

	private Element addElementByPath(XPath xpath, Element parent, TargetInfo targetInfo, Model inputModel, boolean isJSONOutput, boolean isJSONInput) throws Exception {
		String path = targetInfo.schemaPath;
		if(isJSONOutput) {
			path = path.replaceAll("\\$", "").replaceAll("\\.", "/").replaceAll("\\[\\*\\]", "");
		}

		var node = parent;
		var parts = path.split("/");
		var dom = parent.getOwnerDocument();
		String candidatePath = "";
		for (String part : parts) {
			
			if (!part.equals("")) {
				candidatePath = candidatePath  + "/" + part;
				XPathExpression expr = xpath.compile(part);
				NodeList nodes = (NodeList) expr.evaluate(node, XPathConstants.NODESET);
				if (nodes.getLength() > 0) {
					node = (Element) nodes.item(0);
				} else {
					node = (Element) node.appendChild(dom.createElement(part));
				}
				String schemaPath = candidatePath;
				if(isJSONOutput) {
					// TODO: get rid of this hack and set the value correctly in the parsing phase
					schemaPath =  "$"  + candidatePath.replaceAll("/", ".");
				}
				ResIterator i  = inputModel.listSubjectsWithProperty(MSCR.schemaPath, inputModel.createLiteral(schemaPath));
				String order = "0";
				String namespace = null;
				Resource r = null;
				if(i.hasNext()) {
					r = i.next();
					if(r.hasProperty(MSCR.sourceType) && r.getRequiredProperty(MSCR.sourceType).getResource().getURI().equals(MSCR.sourceTypeAttribute.getURI())) {
						order = candidatePath;
					}
					else if(!isJSONOutput && !targetInfo.isAttribute) {
						order = r.getRequiredProperty(SH.order).getObject().asLiteral().getInt()+ "";
					}
					// special handling for attributes that do not have order 


					if(r.hasProperty(MSCR.namespace)) {
						namespace = r.getRequiredProperty(MSCR.namespace).getResource().getURI();	
					}
					
				}
				else {
					throw new RuntimeException("Schema path " + schemaPath + " not found in the target model");
				}
				node.setAttribute("namespace", namespace);
				node.setAttribute("order", order);
				if(candidatePath.equals(path)) {
					node.setAttribute("propertyURI", targetInfo.propertyURI.getURI());
					node.setAttribute("mappingURI", targetInfo.mappingURI.getURI());
					
					node.setAttribute("isAttribute", ""+targetInfo.isAttribute);
//					node.setAttribute("namespace", targetInfo.namespace);
					node.setAttribute("datatype", targetInfo.datatype);
				}
				else {
					boolean isAttribute = r.hasProperty(MSCR.sourceType) && r.getRequiredProperty(MSCR.sourceType).getResource().getURI().equals(MSCR.sourceTypeAttribute.getURI());
					node.setAttribute("isAttribute", ""+isAttribute);
				}
			}
		}
		return node;
	}
	

	private List<TargetInfo> getTargetInfos(Model inputModel) {
		String q ="""
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>				
PREFIX : <http://uri.suomi.fi/datamodel/ns/mscr#>	
prefix sh:    <http://www.w3.org/ns/shacl#> 			
select ?xpath ?schemaPath ?targetProperty ?mapping ?order ?type ?namespace ?datatype ?maxCount
where {
  ?mapping a :Mapping .
  ?mapping :target/(<>|!<>)*/:id ?targetPropertyURI .
  bind(URI(?targetPropertyURI) as ?targetProperty)
  ?targetProperty :instancePath ?xpath .
  ?targetProperty :schemaPath ?schemaPath .
  optional {
    ?targetProperty sh:order ?order
  }
  optional {
	?targetProperty :sourceType ?type
  }
  optional {
	?targetProperty :namespace ?namespace
  }  
  optional {
	?targetProperty sh:datatype ?datatype
  }  
  optional {
	?targetProperty sh:maxCount ?maxCount
  }    
}						
				""";
		QueryExecution qe = QueryExecutionFactory.create(q, inputModel);
		ResultSet results = qe.execSelect();		
		List<TargetInfo> list = new ArrayList<TargetInfo>();
		while(results.hasNext()) {
			QuerySolution soln = results.next();
			String xpath = soln.get("xpath").asLiteral().getString();
			String schemaPath = soln.get("schemaPath").asLiteral().getString();
			Resource mapping = soln.get("mapping").asResource();
			Resource property = soln.get("targetProperty").asResource();
			int order = -1;
			if(soln.get("order") != null) {
				order = soln.get("order").asLiteral().getInt();	
			}
			
			boolean isAttribute = false;
			if(soln.get("type") != null) {
				if(soln.get("type").asResource().equals(MSCR.sourceTypeAttribute)) {
					isAttribute = true;
				}
			}
			String namespace = null;
			if(soln.get("namespace") != null) {
				if(soln.get("namespace").isLiteral()) {
					namespace = soln.get("namespace").asLiteral().getString();	
				}
				else {
					namespace = soln.get("namespace").asResource().getURI();
				}
					
			}
			String datatype = "http://www.w3.org/2001/XMLSchema#string";
			if(soln.get("datatype") != null) {
				datatype = soln.get("datatype").asResource().getURI();	
			}
			else {
				if(soln.get("maxCount") != null && soln.get("maxCount").asLiteral().getInt() == 1) { 
					// map 
					datatype = "mscr:map";
				}
				else {
					// array
					datatype = "mscr:array";
				}
				
			}
			
			list.add(new TargetInfo(xpath, schemaPath, property, mapping, order, isAttribute, namespace, datatype));
		}
		
		return list;
	}



	private String toString(Document doc) throws Exception {

		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer trans = tf.newTransformer();
		trans.setOutputProperty(OutputKeys.INDENT, "yes");
		StringWriter sw = new StringWriter();
		trans.transform(new DOMSource(doc), new StreamResult(sw));

		return sw.toString();
	}

}
