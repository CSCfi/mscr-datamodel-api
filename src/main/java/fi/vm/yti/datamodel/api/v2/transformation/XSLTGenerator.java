package fi.vm.yti.datamodel.api.v2.transformation;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import fi.vm.yti.datamodel.api.v2.dto.MappingInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.NodeInfo;
import fi.vm.yti.datamodel.api.v2.dto.ProcessingInfo;
import fi.vm.yti.datamodel.api.v2.transformation.RMLGenerator.TreeNode;

@Service
public class XSLTGenerator {

	public static final String xslNS = "http://www.w3.org/1999/XSL/Transform";
	public static final String funcNS = "http://www.w3.org/2005/xpath-functions";
	
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
	
	private Element addElementByPath2(XPath xpath, Element parent, String path, String pid, Model targetModel) throws Exception {
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
					String pathToNode = getXPath(node);
					String propURI = getPropertyNodeIdFromXpath(pid, pathToNode);
					propURI = propURI.replace("root2/Root2/", "");
  					Resource prop = ResourceFactory.createResource(propURI);
  					if(targetModel.containsResource(prop)) {
						System.out.println(prop);	
						prop = targetModel.getResource(propURI);
						node.setAttribute("order", ""+prop.getProperty(SH.order).getInt());
					}
					
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
	
	
	private void addTemplates(Map<String, List<MappingInfoDTO>> targetInfo, Element stylesheet, TreeNode2 targetNode, int pathSegment, String templateContext) {
		var xsltDoc = stylesheet.getOwnerDocument();
		boolean isLeafNode = targetNode.children.isEmpty();
		boolean hasMappings = targetNode.mappings != null;
		List<Element> contentElements = new ArrayList<Element>();
		//List<Element> calls = new ArrayList<Element>();
		Map<Element, List<Element>> calls = new LinkedHashMap<Element, List<Element>>();
		
		Element templateElement = xsltDoc.createElementNS(xslNS, "xsl:template");
		if(targetNode.targetElementName.equals("root2")) {
			templateElement.setAttribute("match", "/");					
			contentElements.add(templateElement);	
			for(TreeNode2 child : targetNode.children.values()) {
				addTemplates(targetInfo, stylesheet, child, pathSegment +1, templateContext);
				Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
				callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));
				//calls.add(callTemplate);
				calls.put(templateElement, List.of(callTemplate));
				
				Element withParam = xsltDoc.createElementNS(xslNS, "with-param");
				withParam.setAttribute("name", "node");
				withParam.setAttribute("select", ".");
				callTemplate.appendChild(withParam);
				
			}			
		}		
		else {
			
			
			if(hasMappings) {
				Element contentElement = xsltDoc.createElement(targetNode.targetElementName);
				contentElements.add(contentElement);
				templateElement.appendChild(contentElement);

				Element forEach = xsltDoc.createElementNS(xslNS, "xsl:for-each");
				if(isLeafNode) {
					/*
					System.out.println("Leaf node " + targetNode.targetXPath);
					templateElement.setAttribute("name", "t_" + targetNode.targetXPath.replaceAll("/", "-") + "-from-.");
					forEach.setAttribute("select", "$node");
					Element paramElement = xsltDoc.createElementNS(xslNS, "xsl:param");
					paramElement.setAttribute("name", "node");
					
					if(targetNode.mappings.get(0).getProcessing() != null) {
					
					}
					
					else {
						Element copyOf = xsltDoc.createElementNS(xslNS, "xsl:copy-of");				
						copyOf.setAttribute("select", "node()");
						contentElement.appendChild(copyOf);
						forEach.appendChild(contentElement);

					}
					
					templateElement.appendChild(paramElement);
					templateElement.appendChild(forEach);
					*/
				}
				else {
					templateElement.setAttribute("name", "t_" + targetNode.targetXPath.replaceAll("/", "-") + templateContext);
					forEach.setAttribute("select", "$node");	
					System.out.println("target node " + targetNode.targetElementName);
				
					
					for(MappingInfoDTO tcmap : targetNode.mappings) {
						// if target element is not leaf -> iterator target 
						
						String sourceUri = tcmap.getSource().get(0).getUri();
						String sourceXPath = getXpathFromId(sourceUri);
						System.out.println("sourceXPath: " + sourceXPath);
						calls.put(contentElement, new ArrayList<Element>());
						for(TreeNode2 child : targetNode.children.values()) {
							
						
							if(child.mappings != null) {
								for(MappingInfoDTO cmap : child.mappings) {
									System.out.println("child " +child.id + " mappings " + cmap.getSource().get(0).getUri());
								}
								/*
								for(MappingInfoDTO cmap : child.mappings) {
									System.out.println(templateElement.getAttribute("name"));
									Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
									Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
									String childSourceUri = cmap.getSource().get(0).getUri();
									
									String childSourceXPath = getXpathFromId(childSourceUri);
									
									if(childSourceXPath.startsWith(sourceXPath)) {
	 									if(childSourceXPath.equals(sourceXPath)) {
											childSourceXPath = "";
										}
										else {										
											String[] xparts = childSourceXPath.split("/");
											String temp = "";
											for(int i = pathSegment; i < xparts.length; i++) {
												if(!xparts[i].equals("")) {
													temp = temp + "/" + xparts[i];	
												}
												
											}
											childSourceXPath = temp;
											
										}
										withParam.setAttribute("name", "node");
										withParam.setAttribute("select", "$node" + childSourceXPath);

										String newTemplateContext = "-from-" + childSourceXPath.replaceAll("/", "-");
										callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-")+newTemplateContext);

										//addTemplates(targetInfo, stylesheet, child, pathSegment + 1, newTemplateContext);

										callTemplate.appendChild(withParam);
									
										//calls.add(callTemplate);
										calls.get(contentElement).add(callTemplate);										
									}


								}
								*/
							}
							else {
								//addTemplates(targetInfo, stylesheet, child, pathSegment + 1, templateContext);

								Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
								callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-") + templateContext);

								Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
								withParam.setAttribute("name", "node");
								withParam.setAttribute("select", ".");
								callTemplate.appendChild(withParam);

								//calls.add(callTemplate);
								calls.get(contentElement).add(callTemplate);

							}
							
							
						}				
					}

					Element paramElement = xsltDoc.createElementNS(xslNS, "xsl:param");
					paramElement.setAttribute("name", "node");
					
					forEach.appendChild(contentElement);
					templateElement.appendChild(paramElement);
					templateElement.appendChild(forEach);
				}
				
			}
			else {
				templateElement.setAttribute("name", "t_" + targetNode.targetXPath.replaceAll("/", "-"));
				Element param = xsltDoc.createElementNS(xslNS, "param");
				param.setAttribute("name", "node");
				templateElement.appendChild(param);
				
				// figure out how many description elements need to be created 
				// i.e. how many 
				
				Element targetNodeParent = (Element)targetNode.node.getParentNode();
				// TODO: check if targetNodeParent is repeatable
				boolean isRepeatableParent = true;
				
				System.out.println("Does not have mappings: " + targetNode.id);
				for(TreeNode2 child : targetNode.children.values()) {
					
					List<Element> elementCalls = new ArrayList<Element>();
					

					if(child.mappings != null && !child.mappings.isEmpty()) {

						
						for(MappingInfoDTO cmap : child.mappings) {
							Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
							String childSourceUri = cmap.getSource().get(0).getUri();
							
							String childSourceXPath = getXpathFromId(childSourceUri);

							Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");

							
							String[] xparts = childSourceXPath.split("/");
							String temp = "";
							for(int i = pathSegment; i < xparts.length; i++) {
								if(!xparts[i].equals("")) {
									temp = temp + "/" + xparts[i];	
								}
								
							}
							String newTemplateContext = "-from-" + temp.replaceAll("/", "-");
							String templateName = "t_" + child.targetXPath.replaceAll("/", "-") + newTemplateContext;
							addTemplates(targetInfo, stylesheet, child, pathSegment + 1, newTemplateContext);	
							
							callTemplate.setAttribute("name", templateName);

							withParam.setAttribute("name", "node");
							withParam.setAttribute("select", "$node"+temp );
							callTemplate.appendChild(withParam);
							
							//calls.add(callTemplate);

							elementCalls.add(callTemplate);
						}
						if(isRepeatableParent) {
							Element contentElement = xsltDoc.createElement(targetNode.targetElementName);
							contentElements.add(contentElement);
							calls.put(contentElement, elementCalls);
							templateElement.appendChild(contentElement);
							
						}
						

					}
					else {
						Element contentElement = xsltDoc.createElement(targetNode.targetElementName);
						contentElements.add(contentElement);
						templateElement.appendChild(contentElement);

						Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
						callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));

						addTemplates(targetInfo, stylesheet, child, pathSegment, templateContext);
						Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
						withParam.setAttribute("name", "node");
						withParam.setAttribute("select", "$node");
						callTemplate.appendChild(withParam);
						//calls.add(callTemplate);
						calls.put(contentElement, List.of(callTemplate));
					}
					
				}
			}
		}		
		for(Element contentElement : contentElements) {
			if(calls.containsKey(contentElement)) {
				
				for(Element callElement : calls.get(contentElement)) {
					contentElement.appendChild(callElement);
				}			
			}
		}
		
		boolean templateExists = false;
		
		for(int i = 0; i < stylesheet.getChildNodes().getLength(); i++) {
			Element e = (Element)stylesheet.getChildNodes().item(i);
			if(templateElement.getAttribute("name").equals(e.getAttribute("name"))) {
				templateExists = true;
			
			}
			System.out.println(e.getAttribute("name"));
		}
		if(!templateExists) {
			stylesheet.appendChild(templateElement);
		}

				
	}

	private void addTemplatesFromJSON(Map<String, List<MappingInfoDTO>> targetInfo, Element stylesheet, TreeNode targetNode, Model sourceModel) {
		var xsltDoc = stylesheet.getOwnerDocument();
		boolean isLeafNode = targetNode.children.isEmpty();
		boolean hasMappings = targetNode.mappings != null;
		List<Element> contentElements = new ArrayList<Element>();
		List<Element> calls = new ArrayList<Element>();		
		Element templateElement = xsltDoc.createElementNS(xslNS, "xsl:template");
		if(targetNode.targetElementName.equals("root2")) {
			templateElement.setAttribute("match", "/");					
			contentElements.add(templateElement);	
			
	        Element dataVariable = xsltDoc.createElementNS(xslNS, "variable");
	    	dataVariable.setAttribute("name", "data");
	    	dataVariable.setAttribute("select", "json-to-xml(./*)");
	    	templateElement.appendChild(dataVariable);
	    	
			for(TreeNode child : targetNode.children) {
				// first level templates need data param
				addTemplatesFromJSON(targetInfo, stylesheet, child, sourceModel);
				Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
				callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));

				Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
				withParam.setAttribute("name", "node");
				withParam.setAttribute("select", "$data/*");
				callTemplate.appendChild(withParam);
				calls.add(callTemplate);
			}			
			
		}		
		else {
			templateElement.setAttribute("name", "t_" + targetNode.targetXPath.replaceAll("/", "-"));

			Element paramElement = xsltDoc.createElementNS(xslNS, "xsl:param");
			paramElement.setAttribute("name", "node");
			templateElement.appendChild(paramElement);

			
			Element contentElement = xsltDoc.createElement(targetNode.targetElementName);
			contentElements.add(contentElement);
			templateElement.appendChild(contentElement);

			if(hasMappings) {
				Element forEach = xsltDoc.createElementNS(xslNS, "xsl:for-each");
				if(isLeafNode) {
					forEach.setAttribute("select", "$node");
					
					Element copyOf = xsltDoc.createElementNS(xslNS, "xsl:copy-of");				
					copyOf.setAttribute("select", "node()");
					contentElement.appendChild(copyOf);
					forEach.appendChild(contentElement);

					templateElement.appendChild(forEach);
				}
				else {
					String sourceUri = targetNode.mappings.get(0).getSource().get(0).getUri();
					String sourceXPath = getXpathFromId(sourceUri);
					forEach.setAttribute("select", "$node");				
					for(TreeNode child : targetNode.children) {
						addTemplatesFromJSON(targetInfo, stylesheet, child, sourceModel);
						
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
							String jsonXmlPath = getJsonXMLPath(childSourceUri, childSourceXPath, sourceModel);
							withParam.setAttribute("name", "node");
							withParam.setAttribute("select", jsonXmlPath);
							
														
							callTemplate.appendChild(withParam);
						}
						
						calls.add(callTemplate);
						
					}
					
					forEach.appendChild(contentElement);
					templateElement.appendChild(forEach);
				}
			}
			else {
				Element forEach = xsltDoc.createElementNS(xslNS, "xsl:for-each");
				forEach.setAttribute("select", "$node");
				for(TreeNode child : targetNode.children) {
					addTemplatesFromJSON(targetInfo, stylesheet, child, sourceModel);
					Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
					callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));
					if(child.mappings != null) {
						Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
						String childSourceUri = child.mappings.get(0).getSource().get(0).getUri();
						String childSourceXPath = getXpathFromId(childSourceUri);
						String childJsonXmlPath = getJsonXMLPath(childSourceUri, childSourceXPath, sourceModel);
						withParam.setAttribute("name", "node");
						withParam.setAttribute("select", childJsonXmlPath);
						callTemplate.appendChild(withParam);
					}
					calls.add(callTemplate);
				}
				forEach.appendChild(contentElement);
				templateElement.appendChild(forEach);
			}
		}		
		for(Element contentElement : contentElements) {
			for(Element callElement : calls) {
				contentElement.appendChild(callElement);
			}			
		}			
		stylesheet.appendChild(templateElement);		
	}
	
	private void addTemplatesXMLtoJSON(Map<String, List<MappingInfoDTO>> targetInfo, Element stylesheet, TreeNode targetNode, Model sourceModel, Model targetModel) { 
		var xsltDoc = stylesheet.getOwnerDocument();
		boolean isLeafNode = targetNode.children.isEmpty();
		boolean hasMappings = targetNode.mappings != null;
		List<Element> contentElements = new ArrayList<Element>();
		List<Element> calls = new ArrayList<Element>();		
		Element templateElement = xsltDoc.createElementNS(xslNS, "xsl:template");
		if(targetNode.targetElementName.equals("root2")) {
			templateElement.setAttribute("name", "root");					
			Element map = xsltDoc.createElementNS(funcNS, "map");
			templateElement.appendChild(map);
			contentElements.add(map);

			for(TreeNode child : targetNode.children) {
				addTemplatesXMLtoJSON(targetInfo, stylesheet, child, sourceModel, targetModel);
				Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
				callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));
				Element withParam = xsltDoc.createElementNS(xslNS, "with-param");
				withParam.setAttribute("name", "node");
				String sourceUri = child.mappings.get(0).getSource().get(0).getUri();
				String sourceXPath = getXpathFromId(sourceUri);

				withParam.setAttribute("select", sourceXPath);
				callTemplate.appendChild(withParam);
				map.appendChild(callTemplate);
				calls.add(callTemplate);
			}			
		}
		else {
			
			templateElement.setAttribute("name", "t_" + targetNode.targetXPath.replaceAll("/", "-"));
			if(hasMappings) {
				String sourceURI = targetNode.mappings.get(0).getSource().get(0).getUri();
				Resource sourceProperty = sourceModel.getResource(sourceURI);
				Statement maxStmt = sourceModel.getProperty(sourceProperty, SH.maxCount);
				boolean isArray = false;
				if (maxStmt == null || maxStmt.getObject().asLiteral().getInt() > 1) {
					isArray = true;
				}
				
				
				if(isLeafNode) {
					Element contentElement = xsltDoc.createElementNS(funcNS, "string");
					contentElement.setAttribute("key", targetNode.targetElementName);
					contentElements.add(contentElement);

					Element paramElement = xsltDoc.createElementNS(xslNS, "xsl:param");
					paramElement.setAttribute("name", "node");

					Element copyOf = xsltDoc.createElementNS(xslNS, "xsl:value-of");				
					copyOf.setAttribute("select", "$node/.");
					contentElement.appendChild(copyOf);
					
					
					templateElement.appendChild(paramElement);
					templateElement.appendChild(contentElement);
				}
				else {
					Element forEach = xsltDoc.createElementNS(xslNS, "xsl:for-each");
					Element contentElement = xsltDoc.createElementNS(funcNS, "map");
					contentElements.add(contentElement);


					
					String sourceUri = targetNode.mappings.get(0).getSource().get(0).getUri();
					String sourceXPath = getXpathFromId(sourceUri);
					forEach.setAttribute("select", "$node");		
					System.out.println(sourceXPath);
					for(TreeNode child : targetNode.children) {
						addTemplatesXMLtoJSON(targetInfo, stylesheet, child, sourceModel, targetModel);
						
						Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
						callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));
						System.out.println(callTemplate.getAttribute("name"));
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
						else {
							Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
							withParam.setAttribute("name", "node");
							withParam.setAttribute("select", ".");
							callTemplate.appendChild(withParam);
						}
						
						calls.add(callTemplate);
						
					}
					Element paramElement = xsltDoc.createElementNS(xslNS, "xsl:param");
					paramElement.setAttribute("name", "node");
					
					forEach.appendChild(contentElement);
					templateElement.appendChild(paramElement);
					if(isArray) {
						Element arrayElement = xsltDoc.createElementNS(funcNS, "array");
						arrayElement.setAttribute("key", targetNode.targetElementName);
						arrayElement.appendChild(contentElement);
							
						templateElement.appendChild(arrayElement);
						arrayElement.appendChild(forEach);
					}
					else {
						contentElement.setAttribute("key", targetNode.targetElementName);
							
					}					
					forEach.appendChild(contentElement);
					
				}
			}
			else {	
				Element param = xsltDoc.createElementNS(xslNS, "param");
				param.setAttribute("name", "node");
				templateElement.appendChild(param);
						
				
				Resource targetProperty = targetModel.getResource(targetNode.id);
				System.out.println(targetProperty);
				Statement maxStmt = targetModel.getProperty(targetProperty, SH.maxCount);
				boolean isArray = false;
				if (maxStmt == null || maxStmt.getObject().asLiteral().getInt() > 1) {
					isArray = true;
				}				
				if(isArray) {
					Element arrayElement = xsltDoc.createElementNS(funcNS, "array");
					arrayElement.setAttribute("key", targetNode.targetElementName);
					templateElement.appendChild(arrayElement);
					Element mapElement = xsltDoc.createElementNS(funcNS, "map");
					arrayElement.appendChild(mapElement);
					contentElements.add(mapElement);
				}
				else {
					// if target node is an object
					Element mapElement = xsltDoc.createElementNS(funcNS, "map");
					mapElement.setAttribute("key", targetNode.targetElementName);
					templateElement.appendChild(mapElement);
					contentElements.add(mapElement);
				}
				for(TreeNode child : targetNode.children) {
					addTemplatesXMLtoJSON(targetInfo, stylesheet, child, sourceModel, targetModel);
					Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
					callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));
					
					if(child.mappings != null) {
						Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
						String childSourceUri = child.mappings.get(0).getSource().get(0).getUri();
						
						String childSourceXPath = getXpathFromId(childSourceUri);
						childSourceXPath = childSourceXPath.substring(childSourceXPath.lastIndexOf("/")+1);
						withParam.setAttribute("name", "node");
						withParam.setAttribute("select", childSourceXPath);
						callTemplate.appendChild(withParam);						
						
						/*
						Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
						String childSourceUri = child.mappings.get(0).getSource().get(0).getUri();
						String childSourceXPath = getXpathFromId(childSourceUri);
						withParam.setAttribute("name", "node");
						withParam.setAttribute("select", childSourceXPath);
						callTemplate.appendChild(withParam);
						*/
					}
					else {
						Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
						withParam.setAttribute("name", "node");
						withParam.setAttribute("select", "$node");
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
	
	public void handleTree(TreeNode targetTree, Node node, Map<String, List<MappingInfoDTO>> infos, String pid) {
        NodeList _list = node.getChildNodes();
        for(int i = 0; i < _list.getLength(); i++) {
        	Node _node = _list.item(i);
        	String cxpath = getXPath(_node).substring(6);
        	String id = getPropertyNodeIdFromXpath(pid, cxpath);
    		TreeNode child = new TreeNode(new ArrayList<TreeNode>(), cxpath, _node.getNodeName(), infos.get(cxpath), id);
    		targetTree.children.add(child);
    		handleTree(child, _node, infos, pid);
        	
        }		
	}
	
	private List<String> getCSVRowIteratorSourceIDs(Model m) {
		String q = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX mscr: <http://uri.suomi.fi/datamodel/ns/mscr#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
				+ "PREFIX dcterms: <http://purl.org/dc/terms/>\n"
				+ "PREFIX sh: <http://www.w3.org/ns/shacl#>\n"
				+ "select ?source \n"
				+ "where {\n"
				+ "?mapping rdf:type mscr:Mapping . \n"
				+ "?mapping mscr:source/rdf:_1/mscr:uri ?source.\n"
				+ "?mapping mscr:target/rdf:_1/mscr:id ?targetID.\n"
				+ "FILTER(strstarts( STR(?targetID), \"iterator\"))\n"
				+ "}";
		
		System.out.println(q);
		List<String> iterators = new ArrayList<String>();
		QueryExecution qe = QueryExecutionFactory.create(q, m);
		ResultSet results = qe.execSelect();
		if (!results.hasNext()) {
			System.out.println("No row iterator mapped. No rows will be generated");
		}
		while (results.hasNext()) {
			QuerySolution soln = results.next();
			String sourceID = soln.get("source").asResource().getURI();			
			iterators.add(sourceID);
		}
		
		return iterators;
		
	}
	
	private String getJsonXMLPath(String uri, String xpath, Model sourceModel) {
		String x = "";
		
		Resource sourceProperty = sourceModel.getResource(uri);
		Statement maxCountStmt = sourceProperty.getProperty(SH.maxCount);
		if(maxCountStmt != null) {
			System.out.println(maxCountStmt.getInt());
		}
		else {
			System.out.println("null");
		}
		boolean isArrayOfObject = (maxCountStmt == null || (maxCountStmt != null && maxCountStmt.getInt() > 1));
		System.out.println(sourceProperty.getURI());
		List<String> newParts = new ArrayList<String>();
		String[] parts = xpath.split("/");
		for(String part : parts) {
			if(!part.equals("")) {
				newParts.add("*[@key='"+ part + "']");	
			}
			
		}
		x = String.join("/", newParts);
		if(isArrayOfObject) {
			// add extra /*
			x = x + "/*";			
		}
		
		return x;
	}
	public String generateXMLtoJSON(List<MappingInfoDTO> mappings, Model sourceModel, Model targetModel, String targetPID) throws Exception {
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        
        XPathFactory xpathFactory = XPathFactory.newInstance();
        XPath xpath = xpathFactory.newXPath();
        
        Document doc = docBuilder.newDocument();        
		Element root = doc.createElementNS(xslNS, "xsl:stylesheet");
		root.setAttribute("version", "3.0");		
        doc.appendChild(root);
        
        Element output = doc.createElementNS(xslNS, "xsl:output");
        output.setAttribute("method", "text");
        output.setAttribute("omit-xml-declaration", "yes");
        output.setAttribute("indent", "yes");
        root.appendChild(output);
        
        Element rootTemplate = doc.createElementNS(xslNS, "xsl:template");
        rootTemplate.setAttribute("match", "/");
        root.appendChild(rootTemplate);

        Element rootVariable = doc.createElementNS(xslNS, "xsl:variable");
        rootVariable.setAttribute("name", "xml");
        rootTemplate.appendChild(rootVariable);
        // all generated content must be added under rootVariable 
        
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
        // Create target tree based on mappings. 
        // This we used as the source for named templates in the next phase.
        Document doc2 = docBuilder.newDocument();
		Element root2 = doc2.createElement("root2");
		doc2.appendChild(root2);
		
        for(String t: targetInfo.keySet()) {
        	addElementByPath(xpath, root2, t);        	
        } 
        // Create another tree with extra info
		TreeNode _root = new TreeNode(new ArrayList<TreeNode>(), "/", root2.getNodeName(), null, targetPID);
		handleTree(_root, root2, targetInfo, targetPID);
		
        // generate XSLT templates.
		
        addTemplatesXMLtoJSON(targetInfo, root, _root, sourceModel, targetModel);
        
        Element callTemplate = doc.createElementNS(xslNS, "call-template");
        callTemplate.setAttribute("name", "root");
        rootVariable.appendChild(callTemplate);
        
        
        Element rootValueOf = doc.createElementNS(xslNS, "xsl:value-of");
        rootValueOf.setAttribute("select", "xml-to-json($xml)");
        rootTemplate.appendChild(rootValueOf);
        
        return toString(doc);
		
	}
	
	public String generateJSONtoXML(List<MappingInfoDTO> mappings, Model sourceModel) throws Exception {
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        Document doc = docBuilder.newDocument();        
		Element root = doc.createElementNS(xslNS, "xsl:stylesheet");
		root.setAttribute("version", "3.0");		
        doc.appendChild(root);	       
    	
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
        
        // 
        // Create target tree based on mappings. 
        // This we used as the source for named templates in the next phase.
        Document doc2 = docBuilder.newDocument();
		Element root2 = doc2.createElement("root2");
		doc2.appendChild(root2);
		
        for(String t: targetInfo.keySet()) {
        	addElementByPath(xpath, root2, t);        	
        } 
        // Create another tree with extra info
		TreeNode _root = new TreeNode(new ArrayList<TreeNode>(), "/", root2.getNodeName(), null, "");
		handleTree(_root, root2, targetInfo, "");
        
        // generate XSLT templates.
        addTemplatesFromJSON(targetInfo, root, _root, sourceModel);

        return toString(doc);
	}
	
	public String generateJSONtoCSV(List<MappingInfoDTO> mappings, Model mappingsModel, Model sourceModel) throws Exception {
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        Document doc = docBuilder.newDocument();        
		Element root = doc.createElementNS(xslNS, "xsl:stylesheet");
		root.setAttribute("version", "3.0");		
        doc.appendChild(root);		
        
        
        String defaultSeparator = ";";
        String defaultNewline = "\n";
        Element output = doc.createElementNS(xslNS, "output");
        root.appendChild(output);
        output.setAttribute("method", "text");
        Element separatorVariable = doc.createElementNS(xslNS, "variable");
        root.appendChild(separatorVariable);
        separatorVariable.setAttribute("name", "separator");
        separatorVariable.setAttribute("select", "'"+defaultSeparator+"'");
        Element newlineVariable = doc.createElementNS(xslNS, "variable");
        root.appendChild(newlineVariable);
        newlineVariable.setAttribute("name", "newline");
        newlineVariable.setAttribute("select", "'"+defaultNewline+"'");
        
    	Element dataVariable = doc.createElementNS(xslNS, "variable");
    	dataVariable.setAttribute("name", "data");
    	dataVariable.setAttribute("select", "json-to-xml(./*)");
    	root.appendChild(dataVariable);

    	
        Element rootTemplate = doc.createElementNS(xslNS, "template");
        root.appendChild(rootTemplate);
        rootTemplate.setAttribute("match", "/");
    	List<String> cols = new ArrayList<String>(); 
    	for(MappingInfoDTO m : mappings) {
    		String colLabel = m.getTarget().get(0).getLabel();
    		if(!colLabel.startsWith("iterator source")) { // remove this!
    			cols.add("'" + colLabel + "'");
    			cols.add("$separator");
    		}
    		
    	}
    	
    	Element headerValueOf = doc.createElementNS(xslNS, "value-of");
    	headerValueOf.setAttribute("select", "concat("+ String.join(",", cols) + ")");
    	rootTemplate.appendChild(headerValueOf);
    	
    	Element newLineValue = doc.createElementNS(xslNS, "value-of");
    	newLineValue.setAttribute("select", "$newline");
    	rootTemplate.appendChild(newLineValue);

    	// find the row iterator 
        List<String> rowIterators = getCSVRowIteratorSourceIDs(mappingsModel);
        for(String iteratorID : rowIterators) {        	
        	Element forEach = doc.createElementNS(xslNS, "for-each");
        	String iterator = getXpathFromId(iteratorID);
        	String iteratorXMLJsonPath = getJsonXMLPath(iteratorID, iterator, sourceModel);
        	rootTemplate.appendChild(forEach);
        	forEach.setAttribute("select", "$data/*/" + iteratorXMLJsonPath);
        	for(MappingInfoDTO m : mappings) {
        		String sourceId = m.getSource().get(0).getUri();
        		String sourceXpath = getXpathFromId(sourceId);
        		if(!sourceXpath.equals(iterator)) {
            		sourceXpath = sourceXpath.substring(iterator.length()+1);
            		sourceXpath = getJsonXMLPath(sourceId, sourceXpath, sourceModel);
                	Element valueOf = doc.createElementNS(xslNS, "value-of");
                	valueOf.setAttribute("select", sourceXpath);
                	forEach.appendChild(valueOf);
                	Element valueOf2 = doc.createElementNS(xslNS, "value-of");
                	valueOf2.setAttribute("select", "$separator");
                	forEach.appendChild(valueOf2);
        			
        		}
            	
        	}
        	Element newLineValue2 = doc.createElementNS(xslNS, "value-of");
        	newLineValue2.setAttribute("select", "$newline");
        	forEach.appendChild(newLineValue2);
        }
		
        return toString(doc);
	
	}
	
	public String generateXMLtoCSV(List<MappingInfoDTO> mappings, Model mappingsModel) throws Exception {
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        XPathFactory xpathFactory = XPathFactory.newInstance();
        XPath xpath = xpathFactory.newXPath();
        Document doc = docBuilder.newDocument();        
		Element root = doc.createElementNS(xslNS, "xsl:stylesheet");
		root.setAttribute("version", "2.0");
        doc.appendChild(root);
        
        String defaultSeparator = ";";
        String defaultNewline = "\n";
        Element output = doc.createElementNS(xslNS, "output");
        root.appendChild(output);
        output.setAttribute("method", "text");
        Element separatorVariable = doc.createElementNS(xslNS, "variable");
        root.appendChild(separatorVariable);
        separatorVariable.setAttribute("name", "separator");
        separatorVariable.setAttribute("select", "'"+defaultSeparator+"'");
        Element newlineVariable = doc.createElementNS(xslNS, "variable");
        root.appendChild(newlineVariable);
        newlineVariable.setAttribute("name", "newline");
        newlineVariable.setAttribute("select", "'"+defaultNewline+"'");
        
        Element rootTemplate = doc.createElementNS(xslNS, "template");
        root.appendChild(rootTemplate);
        rootTemplate.setAttribute("match", "/");
    	List<String> cols = new ArrayList<String>(); 
    	for(MappingInfoDTO m : mappings) {
    		String colLabel = m.getTarget().get(0).getLabel();
    		if(!colLabel.startsWith("iterator source")) { // remove this!
    			cols.add("'" + colLabel + "'");
    			cols.add("$separator");
    		}
    		
    	}
    	Element headerValueOf = doc.createElementNS(xslNS, "value-of");
    	headerValueOf.setAttribute("select", "concat("+ String.join(",", cols) + ")");
    	rootTemplate.appendChild(headerValueOf);
    	
    	Element newLineValue = doc.createElementNS(xslNS, "value-of");
    	newLineValue.setAttribute("select", "$newline");
    	rootTemplate.appendChild(newLineValue);

    	// find the row iterator 
        List<String> rowIterators = getCSVRowIteratorSourceIDs(mappingsModel);
        for(String iteratorID : rowIterators) {        	
        	Element forEach = doc.createElementNS(xslNS, "for-each");
        	rootTemplate.appendChild(forEach);
        	String iterator = getXpathFromId(iteratorID);
        	forEach.setAttribute("select", iterator);
        	for(MappingInfoDTO m : mappings) {
        		String sourceId = m.getSource().get(0).getUri();
        		String sourceXpath = getXpathFromId(sourceId);
        		if(!sourceXpath.equals(iterator)) {
            		sourceXpath = sourceXpath.substring(iterator.length()+1);
            		
                	Element valueOf = doc.createElementNS(xslNS, "value-of");
                	valueOf.setAttribute("select", sourceXpath);
                	forEach.appendChild(valueOf);
                	Element valueOf2 = doc.createElementNS(xslNS, "value-of");
                	valueOf2.setAttribute("select", "$separator");
                	forEach.appendChild(valueOf2);
        			
        		}
            	
        	}
        	Element newLineValue2 = doc.createElementNS(xslNS, "value-of");
        	newLineValue2.setAttribute("select", "$newline");
        	forEach.appendChild(newLineValue2);
        }
		
        return toString(doc);
	}
	
	public String generate(List<MappingInfoDTO> mappings, Model sourceModel, String sourcePID, Model targetModel, String targetPID) throws Exception {
		
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
        // Create XPathFactory object
        XPathFactory xpathFactory = XPathFactory.newInstance();
        // Create XPath object
        XPath xpath = xpathFactory.newXPath();
        
        // generate helper data structures: targetxpath -> mappings and list of target
        Map<String, List<MappingInfoDTO>> targetInfo = new LinkedHashMap<String, List<MappingInfoDTO>>();
        Map<String, List<MappingInfoDTO>> sourceInfo = new LinkedHashMap<String, List<MappingInfoDTO>>();
        Map<String, List<String>> targetToSource = new LinkedHashMap<String, List<String>>();
        Map<String, List<String>> sourceToTarget = new LinkedHashMap<String, List<String>>();
        
        for(MappingInfoDTO mapping : mappings) {
        	String targetId = mapping.getTarget().get(0).getUri();
        	String targetXpath = getXpathFromId(targetId);

        	String sourceId = mapping.getSource().get(0).getUri();
        	String sourceXpath = getXpathFromId(sourceId);

        	if(!targetInfo.containsKey(targetXpath)) {
        		targetInfo.put(targetXpath, new ArrayList<MappingInfoDTO>());
        	}
        	targetInfo.get(targetXpath).add(mapping);
        	
        	if(!sourceInfo.containsKey(sourceXpath)) {
        		sourceInfo.put(sourceXpath, new ArrayList<MappingInfoDTO>());
        	}
        	sourceInfo.get(sourceXpath).add(mapping);
        	
        	if(!targetToSource.containsKey(targetXpath)) {
        		targetToSource.put(targetXpath, new ArrayList<String>());
        	}
        	targetToSource.get(targetXpath).add(sourceXpath);

        	if(!sourceToTarget.containsKey(sourceXpath)) {
        		sourceToTarget.put(sourceXpath, new ArrayList<String>());
        	}
        	sourceToTarget.get(sourceXpath).add(targetXpath);

        }
        
        // root elements
        Document doc = docBuilder.newDocument();
        
		Element root = doc.createElementNS(xslNS, "xsl:stylesheet");
		root.setAttribute("version", "2.0");
        doc.appendChild(root);
        
        // Create target tree based on mappings. 
        // This we used as the source for named templates in the next phase.
        Document doc2 = docBuilder.newDocument();
		Element root2 = doc2.createElement("root2");
		doc2.appendChild(root2);
		
        for(String t: targetInfo.keySet()) {
        	System.out.println(t);
        	addElementByPath2(xpath, root2, t, targetPID, targetModel);        	
        }    
        // order path elements according to the target schema 
        TreeNode2 treeNode = new TreeNode2(new TreeMap<Integer, TreeNode2>(), root2, "/", root2.getNodeName(), null, targetPID);
		handleTree(treeNode, targetInfo, targetPID);
        
        // Create another tree with extra info
		//TreeNode _root = new TreeNode(new ArrayList<TreeNode>(), "/", root2.getNodeName(), null, targetPID);
		//handleTree(_root, root2, targetInfo, targetPID);
        
        // generate XSLT templates.
        //addTemplates(targetInfo, root, treeNode, 0, "-");

        addTemplateXMLtoXML(root, treeNode.children, 0, "root", targetToSource, sourceToTarget, sourceInfo, targetInfo, "/");
        Element rootTemplate = doc.createElementNS(xslNS, "xsl:template");
        rootTemplate.setAttribute("match", "/");
        Element callTemplate = doc.createElementNS(xslNS, "call-template");
        callTemplate.setAttribute("name", "root");
        rootTemplate.appendChild(callTemplate);
        Element withParam = doc.createElementNS(xslNS, "with-param");
        withParam.setAttribute("name", "node");
        withParam.setAttribute("select", ".");
        callTemplate.appendChild(withParam);
        root.appendChild(rootTemplate);
        return toString(doc);
        
	}
	
	private Element getRootTemplate(Element stylesheet, String name) {
		for(int i = 0; i < stylesheet.getChildNodes().getLength(); i++) {
			Element e = (Element)stylesheet.getChildNodes().item(i);
			if(name.equals(e.getAttribute("name"))) {
				return e;
			
			}
		}
		return null;
	}

	private void handleFunctions(Document doc, Element e, MappingInfoDTO m) {
		ProcessingInfo pi = m.getProcessing();
		if(pi.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#splitFunc")) {
			String delimiter = pi.getParams().get("delimiter").toString();
			//Element forEachToken = xsltDoc.createElementNS(xslNS, "xsl:for-each");
			List<NodeInfo> splitTargets = m.getTarget();
			
			for(int i = 1; i < splitTargets.size()+1; i++) {
				Element t = doc.createElement(splitTargets.get(i-1).getLabel());
				Element seq = doc.createElementNS(xslNS, "sequence");
				seq.setAttribute("select" , "tokenize(.,'" + delimiter + "')["+ i + "]");
				t.appendChild(seq);
				e.appendChild(t);
			}
		}
		else if(pi.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#concatFunc")) {
			String delimiter = pi.getParams().get("delimiter").toString();
			List<NodeInfo> sources = m.getSource();
			String s = "";							
			for(int i = 0; i < sources.size(); i++) {
				String sourcePath = getXpathFromId(sources.get(i).getUri());

				s = s + sourcePath;
				if( i < sources.size()-1) {
					s = s  + ",'" + delimiter + "',";
				}
				
			}
			Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");
			valueOf.setAttribute("select", "concat(" + s + ")");
			e.appendChild(valueOf);
			
		}						
		else if(pi.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#constantFunc")) {
			String value = pi.getParams().get("value").toString();
			e.setTextContent(value);
			

		}		
		else if(pi.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#filterFunc")) {
			
			String value = "Accepted";
			String property = "dateType";
			Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");				
			valueOf.setAttribute("select", "$node/resource/dates/date/dateValue[../dateType='Accepted']");

			e.appendChild(valueOf);

			
			

		}
	}
	private void addTemplateXMLtoXML(Element stylesheet, Map<Integer, TreeNode2> children, int depth, String parentTemplateName, Map<String, List<String>> targetToSource, Map<String, List<String>> sourceToTarget, Map<String, List<MappingInfoDTO>> sourceInfo, Map<String, List<MappingInfoDTO>> targetInfo, String parentSourceXPath) {
		Document doc = stylesheet.getOwnerDocument();
		if(parentTemplateName.equals("t--resource-titles")) {
			System.out.println("ADs");
		}
		for(TreeNode2 targetNode : children.values()) {
			System.out.println("*: " + targetNode.targetXPath);
			boolean isLeafNode = targetNode.children.isEmpty();
			boolean isMapped = (targetNode.mappings != null && !targetNode.mappings.isEmpty());
			
			if(isMapped) {

				
				
				if(isLeafNode) {
					Element templateElement = getRootTemplate(stylesheet, parentTemplateName);
					if(templateElement == null) {
						templateElement = doc.createElementNS(xslNS, "xsl:template");
						templateElement.setAttribute("name", parentTemplateName);
						Element param = doc.createElementNS(xslNS, "xsl:param");
						param.setAttribute("name", "node");
						templateElement.appendChild(param);
						stylesheet.appendChild(templateElement);
					}
					String templateSourceXpath = parentTemplateName.substring(parentTemplateName.lastIndexOf("--")+1).replaceAll("-", "/");
					for(String sourceXpath : targetToSource.get(targetNode.targetXPath)) {
						if(sourceXpath.startsWith(templateSourceXpath)) {
							Element contentElement = doc.createElement(targetNode.targetElementName);
							templateElement.appendChild(contentElement);
							
							// which mapping is this?
							boolean hasFunction = false;
							for(MappingInfoDTO m : sourceInfo.get(sourceXpath)) {
								for(NodeInfo ni : m.getTarget()) {
									if(targetNode.id.equals(ni.getUri())) {
										if(m.getProcessing() != null) {
											handleFunctions(doc, contentElement, m);
											hasFunction = true;
										}
									}
								}
							}
							if(!hasFunction) {
								Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");
								if(parentSourceXPath.equals("/")) {
									valueOf.setAttribute("select", "$node"+sourceXpath+ "/text()");	
								}
								else {
									valueOf.setAttribute("select", "$node"+sourceXpath.substring(templateSourceXpath.length())+ "/text()");	
								}
								
								//
								contentElement.appendChild(valueOf);
								
							}
						}
						else {
							System.out.println("*---: " + targetNode.targetElementName);
							if(targetNode.targetXPath.startsWith(templateSourceXpath)) {
								Element contentElement = doc.createElement(targetNode.targetElementName);
								templateElement.appendChild(contentElement);
								Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");				
								valueOf.setAttribute("select", "$node"+sourceXpath+ "/text()");
								contentElement.appendChild(valueOf);
								
							}

							
						}
												
					}


				}
				else {
					Element templateElement = doc.createElementNS(xslNS, "xsl:template");
					templateElement.setAttribute("name", parentTemplateName);
					Element param = doc.createElementNS(xslNS, "xsl:param");
					param.setAttribute("name", "node");
					templateElement.appendChild(param);
					stylesheet.appendChild(templateElement);
					for(String sourceXpath : targetToSource.get(targetNode.targetXPath)) {
						Element contentElement = doc.createElement(targetNode.targetElementName);
						templateElement.appendChild(contentElement);
						
						Element callTemplate = doc.createElementNS(xslNS, "xsl:call-template");
						contentElement.appendChild(callTemplate);
						String targetTemplateName = "t-" + targetNode.targetXPath.replaceAll("/", "-") + "-children-source--" + sourceXpath.replaceAll("/", "-");
						callTemplate.setAttribute("name", targetTemplateName);
						
						Element withParam = doc.createElementNS(xslNS, "xsl:with-param");
						callTemplate.appendChild(withParam);
						withParam.setAttribute("name", "node");
						withParam.setAttribute("select", "$node/" + sourceXpath.substring(parentSourceXPath.length()));
						
						addTemplateXMLtoXML(stylesheet, targetNode.children, depth + 1, targetTemplateName, targetToSource, sourceToTarget, sourceInfo, targetInfo, sourceXpath);

						
					}
					
				}
				
			}
			else {
				Element templateElement = getRootTemplate(stylesheet, parentTemplateName);
				if(templateElement == null) {
					templateElement = doc.createElementNS(xslNS, "xsl:template");
					templateElement.setAttribute("name", parentTemplateName);
					Element param = doc.createElementNS(xslNS, "xsl:param");
					param.setAttribute("name", "node");
					templateElement.appendChild(param);
					stylesheet.appendChild(templateElement);
				}
				/*
				Element templateElement = doc.createElementNS(xslNS, "xsl:template");
				templateElement.setAttribute("name", parentTemplateName);
				Element param = doc.createElementNS(xslNS, "xsl:param");
				param.setAttribute("name", "node");
				templateElement.appendChild(param);
				*/
				Element contentElement = doc.createElement(targetNode.targetElementName);
				templateElement.appendChild(contentElement);
				
				Element callTemplate = doc.createElementNS(xslNS, "xsl:call-template");
				contentElement.appendChild(callTemplate);
				String targetTemplateName = "t-" + targetNode.targetXPath.replaceAll("/", "-");
				callTemplate.setAttribute("name", targetTemplateName);
				
				Element withParam = doc.createElementNS(xslNS, "xsl:with-param");
				callTemplate.appendChild(withParam);
				withParam.setAttribute("name", "node");
				withParam.setAttribute("select", "$node");
				
				stylesheet.appendChild(templateElement);
				
				addTemplateXMLtoXML(stylesheet, targetNode.children, depth + 1, targetTemplateName, targetToSource, sourceToTarget, sourceInfo, targetInfo, parentSourceXPath);
			}
			
		}
		
	}


	record TreeNode2(Map<Integer,TreeNode2> children, Element node, String targetXPath, String targetElementName, List<MappingInfoDTO> mappings, String id) {};
	
	private void handleTree(TreeNode2 treeNode, Map<String, List<MappingInfoDTO>> infos, String targetPID) {
		NodeList _list = treeNode.node.getChildNodes();
		for (int i = 0; i < _list.getLength(); i++) {
			Element _node = (Element)_list.item(i);
        	String cxpath = getXPath(_node).substring(6);
        	String id = getPropertyNodeIdFromXpath(targetPID, cxpath);
			
			TreeNode2 child = new TreeNode2(new TreeMap<Integer, TreeNode2>(), _node, cxpath, _node.getNodeName(), infos.get(cxpath), id);			
			treeNode.children.put(Integer.parseInt(_node.getAttribute("order")), child);			
			handleTree(child, infos, targetPID);
		}
		
		
	}
	private String toString(Document doc) throws Exception {

        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer trans = tf.newTransformer();
        trans.setOutputProperty(OutputKeys.INDENT, "yes");
        StringWriter sw = new StringWriter();
        trans.transform(new DOMSource(doc), new StreamResult(sw));
        
        return sw.toString();
	}
	

	record TreeNode(List<TreeNode> children, String targetXPath, String targetElementName, List<MappingInfoDTO> mappings, String id) {}
}
