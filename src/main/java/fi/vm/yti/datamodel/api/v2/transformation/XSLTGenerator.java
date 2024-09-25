package fi.vm.yti.datamodel.api.v2.transformation;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
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
import fi.vm.yti.datamodel.api.v2.dto.NodeInfo;
import fi.vm.yti.datamodel.api.v2.dto.ProcessingInfo;

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
	
	
	private void addTemplates(Map<String, List<MappingInfoDTO>> targetInfo, Element stylesheet, TreeNode targetNode) {
		var xsltDoc = stylesheet.getOwnerDocument();
		boolean isLeafNode = targetNode.children.isEmpty();
		boolean hasMappings = targetNode.mappings != null;
		List<Element> contentElements = new ArrayList<Element>();
		List<Element> calls = new ArrayList<Element>();
		
		Element templateElement = xsltDoc.createElementNS(xslNS, "xsl:template");
		if(targetNode.targetElementName.equals("root2")) {
			templateElement.setAttribute("match", "/");					
			contentElements.add(templateElement);	
			for(TreeNode child : targetNode.children) {
				addTemplates(targetInfo, stylesheet, child);
				Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
				callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));
				calls.add(callTemplate);
			}			
		}		
		else {
			
			templateElement.setAttribute("name", "t_" + targetNode.targetXPath.replaceAll("/", "-"));
			Element contentElement = xsltDoc.createElement(targetNode.targetElementName);
			contentElements.add(contentElement);
			templateElement.appendChild(contentElement);
			if(hasMappings) {
				Element forEach = xsltDoc.createElementNS(xslNS, "xsl:for-each");
				if(isLeafNode) {
					forEach.setAttribute("select", "$node");
					Element paramElement = xsltDoc.createElementNS(xslNS, "xsl:param");
					paramElement.setAttribute("name", "node");
					if(targetNode.mappings.get(0).getProcessing() != null) {
						ProcessingInfo pi = targetNode.mappings.get(0).getProcessing();
						if(pi.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#splitFunc")) {
							String delimiter = pi.getParams().get("delimiter").toString();
							//Element forEachToken = xsltDoc.createElementNS(xslNS, "xsl:for-each");
							List<NodeInfo> splitTargets = targetNode.mappings.get(0).getTarget();
							templateElement.removeChild(contentElement);
							for(int i = 1; i < splitTargets.size()+1; i++) {
								Element t = xsltDoc.createElement(splitTargets.get(i-1).getLabel());
								Element seq = xsltDoc.createElementNS(xslNS, "sequence");
								seq.setAttribute("select" , "tokenize(.,'" + delimiter + "')["+ i + "]");
								t.appendChild(seq);
								forEach.appendChild(t);
							}
						}
						else if(pi.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#concatFunc")) {
							String delimiter = pi.getParams().get("delimiter").toString();
							List<NodeInfo> sources = targetNode.mappings.get(0).getSource();
							String s = "";							
							for(int i = 0; i < sources.size(); i++) {
								String sourcePath = getXpathFromId(sources.get(i).getUri());

								s = s + sourcePath;
								if( i < sources.size()-1) {
									s = s  + ",'" + delimiter + "',";
								}
								
							}
							Element valueOf = xsltDoc.createElementNS(xslNS, "xsl:value-of");
							valueOf.setAttribute("select", "concat(" + s + ")");
							contentElement.appendChild(valueOf);
							forEach.appendChild(contentElement);
						}						
						else if(pi.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#constantFunc")) {
							String value = pi.getParams().get("value").toString();
							contentElement.setTextContent(value);
							forEach.appendChild(contentElement);

						}						
					}
					
					else {
						Element copyOf = xsltDoc.createElementNS(xslNS, "xsl:copy-of");				
						copyOf.setAttribute("select", "node()");
						contentElement.appendChild(copyOf);
						forEach.appendChild(contentElement);

					}
					
					templateElement.appendChild(paramElement);
					templateElement.appendChild(forEach);
				}
				else {
					String sourceUri = targetNode.mappings.get(0).getSource().get(0).getUri();
					String sourceXPath = getXpathFromId(sourceUri);
					forEach.setAttribute("select", "$node");				
					for(TreeNode child : targetNode.children) {
						addTemplates(targetInfo, stylesheet, child);
						
						Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
						callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));
						
						if(child.mappings != null) {
							Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
							String childSourceUri = child.mappings.get(0).getSource().get(0).getUri();
							
							String childSourceXPath = getXpathFromId(childSourceUri);
							if(childSourceXPath.equals(sourceXPath)) {
								childSourceXPath = ".";
							}
							else {
								childSourceXPath = childSourceXPath.substring(sourceXPath.length()+1);
							}
							withParam.setAttribute("name", "node");
							withParam.setAttribute("select", childSourceXPath);
							callTemplate.appendChild(withParam);
						}
						
						calls.add(callTemplate);
						
					}
					Element paramElement = xsltDoc.createElementNS(xslNS, "xsl:param");
					paramElement.setAttribute("name", "node");
					
					forEach.appendChild(contentElement);
					templateElement.appendChild(paramElement);
					templateElement.appendChild(forEach);
				}
			}
			else {
				for(TreeNode child : targetNode.children) {
					addTemplates(targetInfo, stylesheet, child);
					Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
					callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));
					if(child.mappings != null) {
						Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
						String childSourceUri = child.mappings.get(0).getSource().get(0).getUri();
						String childSourceXPath = getXpathFromId(childSourceUri);
						withParam.setAttribute("name", "node");
						withParam.setAttribute("select", childSourceXPath);
						callTemplate.appendChild(withParam);
					}
					calls.add(callTemplate);
				}
			}
		}		
		for(Element contentElement : contentElements) {
			for(Element callElement : calls) {
				contentElement.appendChild(callElement);
			}			
		}			
		stylesheet.appendChild(templateElement);		
	}

	public void handleTree(TreeNode targetTree, Node node, Map<String, List<MappingInfoDTO>> infos) {
        NodeList _list = node.getChildNodes();
        for(int i = 0; i < _list.getLength(); i++) {
        	Node _node = _list.item(i);
        	String cxpath = getXPath(_node).substring(6);        	
    		TreeNode child = new TreeNode(new ArrayList<TreeNode>(), cxpath, _node.getNodeName(), infos.get(cxpath));
    		targetTree.children.add(child);
    		handleTree(child, _node, infos);
        	
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
        	String targetId = mapping.getTarget().get(0).getUri();
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
        // Create another tree with extra info
		TreeNode _root = new TreeNode(new ArrayList<TreeNode>(), "/", root2.getNodeName(), null);
		handleTree(_root, root2, targetInfo);
        
        // generate XSLT templates.
        addTemplates(targetInfo, root, _root);

        
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer trans = tf.newTransformer();
        trans.setOutputProperty(OutputKeys.INDENT, "yes");
        StringWriter sw = new StringWriter();
        trans.transform(new DOMSource(doc), new StreamResult(sw));
        
        return sw.toString();
        
	}
	

	record TreeNode(List<TreeNode> children, String targetXPath, String targetElementName, List<MappingInfoDTO> mappings) {}
}
