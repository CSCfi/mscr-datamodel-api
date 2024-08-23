package fi.vm.yti.datamodel.api.v2.transformation;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import fi.vm.yti.datamodel.api.v2.dto.MappingInfoDTO;

@Service
public class XSLTGenerator {

	public static final String xslNS = "http://www.w3.org/1999/XSL/Transform";
	
	private String getXpathFromId(String id) {
		String temp = id.substring(id.indexOf("Root") + 4);
		// remove all parts with capital letter from the temp string
		String[] parts = temp.split("/");
		List<String> newParts = new ArrayList<String>();
		for(String part : parts) {
			if(part.length() > 0 && Character.isLowerCase(part.charAt(0))) {
				newParts.add(part);
			}
		}
		temp = "/" + StringUtils.join(newParts, "/");
		return temp;
	}
	
	private Element addElementByPath(XPath xpath, Element parent, String path) throws Exception {
		var node = parent;
		var parts = path.split("/");
		var dom = parent.getOwnerDocument();
		for(String part : parts) {
			if(!part.equals("")) {				
				XPathExpression expr = xpath.compile(part);
				NodeList nodes = (NodeList)expr.evaluate(node, XPathConstants.NODESET);
				if(nodes.getLength() > 0) {
					node = (Element)nodes.item(0);
				}
				else {
					node = (Element) node.appendChild(dom.createElement(part));						
				}				
			}

		}
		
		return node;
	}
	
	private String getXPath(Node node)
	{
	    Node parent = node.getParentNode();
	    if (parent == null)
	    {
	        return "";
	    }
	    return getXPath(parent) + "/" + node.getNodeName();
	}
	
	
	private void addTemplateFromPath(Map<String, List<MappingInfoDTO>> targetInfo, XPath xpath, Element stylesheet, Element node) throws Exception {
		var xsltDoc = stylesheet.getOwnerDocument();

		String currentNodePath = getXPath(node).substring(6);
		boolean isLeafNode = !node.hasChildNodes();
		
		boolean isMapped = targetInfo.containsKey(currentNodePath);
		List<Element> contentElements = new ArrayList<Element>();
		if(node.getNodeName().equals("root2")) {
			Element templateElement = xsltDoc.createElementNS(xslNS, "xsl:template");
			templateElement.setAttribute("match", "/");
			stylesheet.appendChild(templateElement);
			contentElements.add(templateElement);
		}
		else if(isMapped) {
			// create one template per mapping
			for(MappingInfoDTO mapping: targetInfo.get(currentNodePath)) {
				Element templateElement = xsltDoc.createElementNS(xslNS, "xsl:template");
				Element contentElement = xsltDoc.createElement(node.getNodeName());
				templateElement.setAttribute("name", "t_" + mapping.getPID().substring(mapping.getPID().indexOf("=")+1).replaceAll("-", ""));
				if(isLeafNode) {
					Element copyOf = xsltDoc.createElementNS(xslNS, "xsl:copy-of");				
					copyOf.setAttribute("select", "$node/node()");
					contentElement.appendChild(copyOf);		
				}
				Element paramElement = xsltDoc.createElementNS(xslNS, "xsl:param");
				paramElement.setAttribute("name", "node");
				templateElement.appendChild(paramElement);				
				templateElement.appendChild(contentElement);
				stylesheet.appendChild(templateElement);
				contentElements.add(contentElement);
			}
		}
		else {
			// create one template
			Element templateElement = xsltDoc.createElementNS(xslNS, "xsl:template");
			Element contentElement = xsltDoc.createElement(node.getNodeName());

			if(isLeafNode) {
				Element copyOf = xsltDoc.createElementNS(xslNS, "xsl:copy-of");				
				copyOf.setAttribute("select", ".");
				contentElement.appendChild(copyOf);
			}
			templateElement.setAttribute("name", "t_" + getXPath(node).replaceAll("/", "_"));
			templateElement.appendChild(contentElement);
			stylesheet.appendChild(templateElement);
			contentElements.add(contentElement);
		}
		List<Element> calls = new ArrayList<Element>();
		Node childNode = (Element)node.getFirstChild();   
		//NodeList children2 = node.getChildNodes();
		//for(int i = 0; i < children2.getLength(); i++) {
		while(childNode !=null) {
			//Element child = (Element)children2.item(i);
			if(childNode.getNodeType() == Node.ELEMENT_NODE) {
				Element child = (Element)childNode;  
				addTemplateFromPath(targetInfo, xpath, stylesheet, child);
				// create call-element
				String childNodePath = getXPath(child).substring(6);
				boolean isChildMapped = targetInfo.containsKey(childNodePath);
				if(isChildMapped) {				
					for(MappingInfoDTO childMapping : targetInfo.get(childNodePath)) {
						Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
						callTemplate.setAttribute("name", "t_" + childMapping.getPID().substring(childMapping.getPID().indexOf("=")+1).replaceAll("-", ""));
						Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
						withParam.setAttribute("name", "node");
						withParam.setAttribute("select", getXpathFromId(childMapping.getSource().get(0).getId()));
						callTemplate.appendChild(withParam);
						calls.add(callTemplate);
					}				
				}
				else {
					Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
					callTemplate.setAttribute("name", "t_" + getXPath(child).replaceAll("/", "_"));
					calls.add(callTemplate);
				}
			}
			
			childNode = childNode.getNextSibling();
		}
		for(Element contentElement : contentElements) {
			for(Element callElement : calls) {
				contentElement.appendChild(callElement);
			}			
		}		
	}

		

	public String generate(List<MappingInfoDTO> mappings) throws Exception {
		
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        // Create XPathFactory object
        XPathFactory xpathFactory = XPathFactory.newInstance();
        // Create XPath object
        XPath xpath = xpathFactory.newXPath();
        
        // generate helper data structures: targetxpath -> mappings and list of target
        Map<String, List<MappingInfoDTO>> targetInfo = new HashMap<String, List<MappingInfoDTO>>();
        for(MappingInfoDTO mapping : mappings) {
        	String targetId = mapping.getTarget().get(0).getId();
        	String targetXpath = getXpathFromId(targetId);
        	if(!targetInfo.containsKey(targetXpath)) {
        		targetInfo.put(targetXpath, new ArrayList<MappingInfoDTO>());
        	}
        	targetInfo.get(targetXpath).add(mapping);
        	
        }
        
        // root elements
        Document doc = docBuilder.newDocument();
        
		doc.createElement("root");
		Element root = doc.createElementNS(xslNS, "xsl:stylesheet");
		root.setAttribute("version", "2.0");
        doc.appendChild(root);
        
        // Create target tree based on mappings. 
        // This we used as the source for named templates in the next phase.
        Document doc2 = docBuilder.newDocument();
		Element root2 = doc2.createElement("root2");
		doc2.appendChild(root2);
        for(String t: targetInfo.keySet()) {
        	addElementByPath(xpath, root2, t);        	
        } 
        // generate XSLT templates.
        addTemplateFromPath(targetInfo, xpath, root, root2);

        
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer trans = tf.newTransformer();
        trans.setOutputProperty(OutputKeys.INDENT, "yes");
        StringWriter sw = new StringWriter();
        trans.transform(new DOMSource(doc), new StreamResult(sw));
        

        return sw.toString();
        
	}
}
