package fi.vm.yti.datamodel.api.v2.transformation;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

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
import org.apache.jena.vocabulary.RDF;
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
		for (String part : parts) {
			if (part.length() > 0 && Character.isLowerCase(part.charAt(0))) {
				newParts.add(part);
			}
		}
		temp = "/" + StringUtils.join(newParts, "/");
		return temp;
	}

	private String getXpathFromUri(String uri, Model sourceModel, List<String> namespaces) {
		String temp = uri.substring(uri.indexOf("Root") + 4);
		String prefix = uri.substring(0, uri.indexOf("#") + 1) + "root/Root";
		String[] parts = temp.split("/");
		List<String> newParts = new ArrayList<String>();
		List<String> allParts = new ArrayList<String>();
		int i = 0;
		Resource r = null;
		for (String part : parts) {
			allParts.add(part);
			if (part.length() > 0 && i % 2 != 0) {
				// add possible namespace
				String partURI = prefix + StringUtils.join(allParts, "/");
				r = sourceModel.getResource(partURI);

				if (r.getProperty(MSCR.namespace) != null) {
					String namespace = r.getPropertyResourceValue(MSCR.namespace).getURI();
					/*
					 * String tempPart = "*[local-name() = '" + part + "' and namespace-uri() = '" +
					 * namespace + "']";
					 */
					String ns = "ns" + namespaces.indexOf(namespace);
					String tempPart = ns + ":" + part;
					newParts.add(tempPart);
				} else {
					newParts.add(part);
				}

			}
			i++;
		}
		temp = "/" + StringUtils.join(newParts, "/");

		System.out.println("getXpathFromUri: " + temp);
		return temp;
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

	private Element addElementByPath2(XPath xpath, Element parent, String path, String pid, Model targetModel,
			List<String> namespaces) throws Exception {
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
					String pathToNode = getXPath(node);
					String propURI = getPropertyNodeIdFromXpath(pid, pathToNode);
					propURI = propURI.replace("root2/Root2/", "");
					Resource prop = ResourceFactory.createResource(propURI);
					System.out.println(prop);
					if (targetModel.containsResource(prop)) {
						System.out.println("Found");
						prop = targetModel.getResource(propURI);
						if (prop.getProperty(SH.order) != null) {
							node.setAttribute("order", "" + prop.getProperty(SH.order).getInt());
						}
						if (prop.getProperty(MSCR.namespace) != null) {
							String namespace = prop.getProperty(MSCR.namespace).getObject().asResource().getURI();
							if (namespace != null && !"".equals(namespace) && !namespaces.contains(namespace)) {
								namespaces.add(namespace);
							}

							node.setAttribute("namespace", namespace);
						}
						if (prop.getProperty(MSCR.sourceType) != null) {
							node.setAttribute("sourceType",
									"" + prop.getProperty(MSCR.sourceType).getObject().asResource().getURI());
						}
						
						
					}

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

	private void addTemplates(Map<String, List<MappingInfoDTO>> targetInfo, Element stylesheet, TreeNode2 targetNode,
			int pathSegment, String templateContext) {
		var xsltDoc = stylesheet.getOwnerDocument();
		boolean isLeafNode = targetNode.children.isEmpty();
		boolean hasMappings = targetNode.mappings != null;
		List<Element> contentElements = new ArrayList<Element>();
		// List<Element> calls = new ArrayList<Element>();
		Map<Element, List<Element>> calls = new LinkedHashMap<Element, List<Element>>();

		Element templateElement = xsltDoc.createElementNS(xslNS, "xsl:template");
		if (targetNode.targetElementName.equals("root2")) {
			templateElement.setAttribute("match", "/");
			contentElements.add(templateElement);
			for (TreeNode2 child : targetNode.children.values()) {
				addTemplates(targetInfo, stylesheet, child, pathSegment + 1, templateContext);
				Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
				callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));
				// calls.add(callTemplate);
				calls.put(templateElement, List.of(callTemplate));

				Element withParam = xsltDoc.createElementNS(xslNS, "with-param");
				withParam.setAttribute("name", "node");
				withParam.setAttribute("select", ".");
				callTemplate.appendChild(withParam);

			}
		} else {

			if (hasMappings) {
				Element contentElement = xsltDoc.createElement(targetNode.targetElementName);
				contentElements.add(contentElement);
				templateElement.appendChild(contentElement);

				Element forEach = xsltDoc.createElementNS(xslNS, "xsl:for-each");
				if (isLeafNode) {
					/*
					 * System.out.println("Leaf node " + targetNode.targetXPath);
					 * templateElement.setAttribute("name", "t_" +
					 * targetNode.targetXPath.replaceAll("/", "-") + "-from-.");
					 * forEach.setAttribute("select", "$node"); Element paramElement =
					 * xsltDoc.createElementNS(xslNS, "xsl:param");
					 * paramElement.setAttribute("name", "node");
					 * 
					 * if(targetNode.mappings.get(0).getProcessing() != null) {
					 * 
					 * }
					 * 
					 * else { Element copyOf = xsltDoc.createElementNS(xslNS, "xsl:copy-of");
					 * copyOf.setAttribute("select", "node()"); contentElement.appendChild(copyOf);
					 * forEach.appendChild(contentElement);
					 * 
					 * }
					 * 
					 * templateElement.appendChild(paramElement);
					 * templateElement.appendChild(forEach);
					 */
				} else {
					templateElement.setAttribute("name",
							"t_" + targetNode.targetXPath.replaceAll("/", "-") + templateContext);
					forEach.setAttribute("select", "$node");
					System.out.println("target node " + targetNode.targetElementName);

					for (MappingInfoDTO tcmap : targetNode.mappings) {
						// if target element is not leaf -> iterator target

						String sourceUri = tcmap.getSource().get(0).getUri();
						String sourceXPath = getXpathFromId(sourceUri);
						System.out.println("sourceXPath: " + sourceXPath);
						calls.put(contentElement, new ArrayList<Element>());
						for (TreeNode2 child : targetNode.children.values()) {

							if (child.mappings != null) {
								for (MappingInfoDTO cmap : child.mappings) {
									System.out.println(
											"child " + child.id + " mappings " + cmap.getSource().get(0).getUri());
								}
								/*
								 * for(MappingInfoDTO cmap : child.mappings) {
								 * System.out.println(templateElement.getAttribute("name")); Element
								 * callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template"); Element
								 * withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param"); String
								 * childSourceUri = cmap.getSource().get(0).getUri();
								 * 
								 * String childSourceXPath = getXpathFromId(childSourceUri);
								 * 
								 * if(childSourceXPath.startsWith(sourceXPath)) {
								 * if(childSourceXPath.equals(sourceXPath)) { childSourceXPath = ""; } else {
								 * String[] xparts = childSourceXPath.split("/"); String temp = ""; for(int i =
								 * pathSegment; i < xparts.length; i++) { if(!xparts[i].equals("")) { temp =
								 * temp + "/" + xparts[i]; }
								 * 
								 * } childSourceXPath = temp;
								 * 
								 * } withParam.setAttribute("name", "node"); withParam.setAttribute("select",
								 * "$node" + childSourceXPath);
								 * 
								 * String newTemplateContext = "-from-" + childSourceXPath.replaceAll("/", "-");
								 * callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/",
								 * "-")+newTemplateContext);
								 * 
								 * //addTemplates(targetInfo, stylesheet, child, pathSegment + 1,
								 * newTemplateContext);
								 * 
								 * callTemplate.appendChild(withParam);
								 * 
								 * //calls.add(callTemplate); calls.get(contentElement).add(callTemplate); }
								 * 
								 * 
								 * }
								 */
							} else {
								// addTemplates(targetInfo, stylesheet, child, pathSegment + 1,
								// templateContext);

								Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
								callTemplate.setAttribute("name",
										"t_" + child.targetXPath.replaceAll("/", "-") + templateContext);

								Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
								withParam.setAttribute("name", "node");
								withParam.setAttribute("select", ".");
								callTemplate.appendChild(withParam);

								// calls.add(callTemplate);
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

			} else {
				templateElement.setAttribute("name", "t_" + targetNode.targetXPath.replaceAll("/", "-"));
				Element param = xsltDoc.createElementNS(xslNS, "param");
				param.setAttribute("name", "node");
				templateElement.appendChild(param);

				// figure out how many description elements need to be created
				// i.e. how many

				Element targetNodeParent = (Element) targetNode.node.getParentNode();
				// TODO: check if targetNodeParent is repeatable
				boolean isRepeatableParent = true;

				System.out.println("Does not have mappings: " + targetNode.id);
				for (TreeNode2 child : targetNode.children.values()) {

					List<Element> elementCalls = new ArrayList<Element>();

					if (child.mappings != null && !child.mappings.isEmpty()) {

						for (MappingInfoDTO cmap : child.mappings) {
							Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
							String childSourceUri = cmap.getSource().get(0).getUri();

							String childSourceXPath = getXpathFromId(childSourceUri);

							Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");

							String[] xparts = childSourceXPath.split("/");
							String temp = "";
							for (int i = pathSegment; i < xparts.length; i++) {
								if (!xparts[i].equals("")) {
									temp = temp + "/" + xparts[i];
								}

							}
							String newTemplateContext = "-from-" + temp.replaceAll("/", "-");
							String templateName = "t_" + child.targetXPath.replaceAll("/", "-") + newTemplateContext;
							addTemplates(targetInfo, stylesheet, child, pathSegment + 1, newTemplateContext);

							callTemplate.setAttribute("name", templateName);

							withParam.setAttribute("name", "node");
							withParam.setAttribute("select", "$node" + temp);
							callTemplate.appendChild(withParam);

							// calls.add(callTemplate);

							elementCalls.add(callTemplate);
						}
						if (isRepeatableParent) {
							Element contentElement = xsltDoc.createElement(targetNode.targetElementName);
							contentElements.add(contentElement);
							calls.put(contentElement, elementCalls);
							templateElement.appendChild(contentElement);

						}

					} else {
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
						// calls.add(callTemplate);
						calls.put(contentElement, List.of(callTemplate));
					}

				}
			}
		}
		for (Element contentElement : contentElements) {
			if (calls.containsKey(contentElement)) {

				for (Element callElement : calls.get(contentElement)) {
					contentElement.appendChild(callElement);
				}
			}
		}

		boolean templateExists = false;

		for (int i = 0; i < stylesheet.getChildNodes().getLength(); i++) {
			Element e = (Element) stylesheet.getChildNodes().item(i);
			if (templateElement.getAttribute("name").equals(e.getAttribute("name"))) {
				templateExists = true;

			}
			System.out.println(e.getAttribute("name"));
		}
		if (!templateExists) {
			stylesheet.appendChild(templateElement);
		}

	}

	private void addTemplatesFromJSON(Map<String, List<MappingInfoDTO>> targetInfo, Element stylesheet,
			TreeNode targetNode, Model sourceModel) {
		var xsltDoc = stylesheet.getOwnerDocument();
		boolean isLeafNode = targetNode.children.isEmpty();
		boolean hasMappings = targetNode.mappings != null;
		List<Element> contentElements = new ArrayList<Element>();
		List<Element> calls = new ArrayList<Element>();
		Element templateElement = xsltDoc.createElementNS(xslNS, "xsl:template");
		if (targetNode.targetElementName.equals("root2")) {
			templateElement.setAttribute("match", "/");
			contentElements.add(templateElement);

			Element dataVariable = xsltDoc.createElementNS(xslNS, "variable");
			dataVariable.setAttribute("name", "data");
			dataVariable.setAttribute("select", "json-to-xml(./*)");
			templateElement.appendChild(dataVariable);

			for (TreeNode child : targetNode.children) {
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

		} else {
			templateElement.setAttribute("name", "t_" + targetNode.targetXPath.replaceAll("/", "-"));

			Element paramElement = xsltDoc.createElementNS(xslNS, "xsl:param");
			paramElement.setAttribute("name", "node");
			templateElement.appendChild(paramElement);

			Element contentElement = xsltDoc.createElement(targetNode.targetElementName);
			contentElements.add(contentElement);
			templateElement.appendChild(contentElement);

			if (hasMappings) {
				Element forEach = xsltDoc.createElementNS(xslNS, "xsl:for-each");
				if (isLeafNode) {
					forEach.setAttribute("select", "$node");

					Element copyOf = xsltDoc.createElementNS(xslNS, "xsl:copy-of");
					copyOf.setAttribute("select", "node()");
					contentElement.appendChild(copyOf);
					forEach.appendChild(contentElement);

					templateElement.appendChild(forEach);
				} else {
					String sourceUri = targetNode.mappings.get(0).getSource().get(0).getUri();
					String sourceXPath = getXpathFromId(sourceUri);
					forEach.setAttribute("select", "$node");
					for (TreeNode child : targetNode.children) {
						addTemplatesFromJSON(targetInfo, stylesheet, child, sourceModel);

						Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
						callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));

						if (child.mappings != null) {
							Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
							String childSourceUri = child.mappings.get(0).getSource().get(0).getUri();

							String childSourceXPath = getXpathFromId(childSourceUri);
							if (childSourceXPath.equals(sourceXPath)) {
								childSourceXPath = ".";
							} else {
								childSourceXPath = childSourceXPath.substring(sourceXPath.length() + 1);
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
			} else {
				Element forEach = xsltDoc.createElementNS(xslNS, "xsl:for-each");
				forEach.setAttribute("select", "$node");
				for (TreeNode child : targetNode.children) {
					addTemplatesFromJSON(targetInfo, stylesheet, child, sourceModel);
					Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
					callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));
					if (child.mappings != null) {
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
		for (Element contentElement : contentElements) {
			for (Element callElement : calls) {
				contentElement.appendChild(callElement);
			}
		}
		stylesheet.appendChild(templateElement);
	}

	private void addTemplatesXMLtoJSON(Map<String, List<MappingInfoDTO>> targetInfo, Element stylesheet,
			TreeNode targetNode, Model sourceModel, Model targetModel) {
		var xsltDoc = stylesheet.getOwnerDocument();
		boolean isLeafNode = targetNode.children.isEmpty();
		boolean hasMappings = targetNode.mappings != null;
		List<Element> contentElements = new ArrayList<Element>();
		List<Element> calls = new ArrayList<Element>();
		Element templateElement = xsltDoc.createElementNS(xslNS, "xsl:template");
		if (targetNode.targetElementName.equals("root2")) {
			templateElement.setAttribute("name", "root");
			Element map = xsltDoc.createElementNS(funcNS, "map");
			templateElement.appendChild(map);
			contentElements.add(map);

			for (TreeNode child : targetNode.children) {
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
		} else {

			templateElement.setAttribute("name", "t_" + targetNode.targetXPath.replaceAll("/", "-"));
			if (hasMappings) {
				String sourceURI = targetNode.mappings.get(0).getSource().get(0).getUri();
				Resource sourceProperty = sourceModel.getResource(sourceURI);
				Statement maxStmt = sourceModel.getProperty(sourceProperty, SH.maxCount);
				boolean isArray = false;
				if (maxStmt == null || maxStmt.getObject().asLiteral().getInt() > 1) {
					isArray = true;
				}

				if (isLeafNode) {
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
				} else {
					Element forEach = xsltDoc.createElementNS(xslNS, "xsl:for-each");
					Element contentElement = xsltDoc.createElementNS(funcNS, "map");
					contentElements.add(contentElement);

					String sourceUri = targetNode.mappings.get(0).getSource().get(0).getUri();
					String sourceXPath = getXpathFromId(sourceUri);
					forEach.setAttribute("select", "$node");
					System.out.println(sourceXPath);
					for (TreeNode child : targetNode.children) {
						addTemplatesXMLtoJSON(targetInfo, stylesheet, child, sourceModel, targetModel);

						Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
						callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));
						System.out.println(callTemplate.getAttribute("name"));
						if (child.mappings != null) {
							Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
							String childSourceUri = child.mappings.get(0).getSource().get(0).getUri();

							String childSourceXPath = getXpathFromId(childSourceUri);
							if (childSourceXPath.equals(sourceXPath)) {
								childSourceXPath = ".";
							} else {
								childSourceXPath = childSourceXPath.substring(sourceXPath.length() + 1);
							}
							withParam.setAttribute("name", "node");
							withParam.setAttribute("select", childSourceXPath);
							callTemplate.appendChild(withParam);
						} else {
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
					if (isArray) {
						Element arrayElement = xsltDoc.createElementNS(funcNS, "array");
						arrayElement.setAttribute("key", targetNode.targetElementName);
						arrayElement.appendChild(contentElement);

						templateElement.appendChild(arrayElement);
						arrayElement.appendChild(forEach);
					} else {
						contentElement.setAttribute("key", targetNode.targetElementName);

					}
					forEach.appendChild(contentElement);

				}
			} else {
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
				if (isArray) {
					Element arrayElement = xsltDoc.createElementNS(funcNS, "array");
					arrayElement.setAttribute("key", targetNode.targetElementName);
					templateElement.appendChild(arrayElement);
					Element mapElement = xsltDoc.createElementNS(funcNS, "map");
					arrayElement.appendChild(mapElement);
					contentElements.add(mapElement);
				} else {
					// if target node is an object
					Element mapElement = xsltDoc.createElementNS(funcNS, "map");
					mapElement.setAttribute("key", targetNode.targetElementName);
					templateElement.appendChild(mapElement);
					contentElements.add(mapElement);
				}
				for (TreeNode child : targetNode.children) {
					addTemplatesXMLtoJSON(targetInfo, stylesheet, child, sourceModel, targetModel);
					Element callTemplate = xsltDoc.createElementNS(xslNS, "xsl:call-template");
					callTemplate.setAttribute("name", "t_" + child.targetXPath.replaceAll("/", "-"));

					if (child.mappings != null) {
						Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
						String childSourceUri = child.mappings.get(0).getSource().get(0).getUri();

						String childSourceXPath = getXpathFromId(childSourceUri);
						childSourceXPath = childSourceXPath.substring(childSourceXPath.lastIndexOf("/") + 1);
						withParam.setAttribute("name", "node");
						withParam.setAttribute("select", childSourceXPath);
						callTemplate.appendChild(withParam);

						/*
						 * Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param"); String
						 * childSourceUri = child.mappings.get(0).getSource().get(0).getUri(); String
						 * childSourceXPath = getXpathFromId(childSourceUri);
						 * withParam.setAttribute("name", "node"); withParam.setAttribute("select",
						 * childSourceXPath); callTemplate.appendChild(withParam);
						 */
					} else {
						Element withParam = xsltDoc.createElementNS(xslNS, "xsl:with-param");
						withParam.setAttribute("name", "node");
						withParam.setAttribute("select", "$node");
						callTemplate.appendChild(withParam);

					}
					calls.add(callTemplate);
				}
			}
		}

		for (Element contentElement : contentElements) {
			for (Element callElement : calls) {
				contentElement.appendChild(callElement);
			}
		}
		stylesheet.appendChild(templateElement);
	}

	public void handleTree(TreeNode targetTree, Node node, Map<String, List<MappingInfoDTO>> infos, String pid) {
		NodeList _list = node.getChildNodes();
		for (int i = 0; i < _list.getLength(); i++) {
			Node _node = _list.item(i);
			String cxpath = getXPath(_node).substring(6);
			String id = getPropertyNodeIdFromXpath(pid, cxpath);
			TreeNode child = new TreeNode(new ArrayList<TreeNode>(), cxpath, _node.getNodeName(), infos.get(cxpath),
					id);
			targetTree.children.add(child);
			handleTree(child, _node, infos, pid);

		}
	}

	private List<String> getCSVRowIteratorSourceIDs(Model m) {
		String q = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
				+ "PREFIX mscr: <http://uri.suomi.fi/datamodel/ns/mscr#>\n"
				+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
				+ "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" + "PREFIX dcterms: <http://purl.org/dc/terms/>\n"
				+ "PREFIX sh: <http://www.w3.org/ns/shacl#>\n" + "select ?source \n" + "where {\n"
				+ "?mapping rdf:type mscr:Mapping . \n" + "?mapping mscr:source/rdf:_1/mscr:uri ?source.\n"
				+ "?mapping mscr:target/rdf:_1/mscr:id ?targetID.\n"
				+ "FILTER(strstarts( STR(?targetID), \"iterator\"))\n" + "}";

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
		if (maxCountStmt != null) {
			System.out.println(maxCountStmt.getInt());
		} else {
			System.out.println("null");
		}
		boolean isArrayOfObject = (maxCountStmt == null || (maxCountStmt != null && maxCountStmt.getInt() > 1));
		System.out.println(sourceProperty.getURI());
		List<String> newParts = new ArrayList<String>();
		String[] parts = xpath.split("/");
		for (String part : parts) {
			if (!part.equals("")) {
				newParts.add("*[@key='" + part + "']");
			}

		}
		x = String.join("/", newParts);
		if (isArrayOfObject) {
			// add extra /*
			x = x + "/*";
		}

		return x;
	}

	public String generateXMLtoJSON(List<MappingInfoDTO> mappings, Model sourceModel, Model targetModel,
			String targetPID) throws Exception {
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
		for (MappingInfoDTO mapping : mappings) {
			String targetId = mapping.getTarget().get(0).getUri();
			String targetXpath = getXpathFromId(targetId);
			if (!targetInfo.containsKey(targetXpath)) {
				targetInfo.put(targetXpath, new ArrayList<MappingInfoDTO>());
			}
			targetInfo.get(targetXpath).add(mapping);
		}
		// Create target tree based on mappings.
		// This we used as the source for named templates in the next phase.
		Document doc2 = docBuilder.newDocument();
		Element root2 = doc2.createElement("root2");
		doc2.appendChild(root2);

		for (String t : targetInfo.keySet()) {
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
		for (MappingInfoDTO mapping : mappings) {
			String targetId = mapping.getTarget().get(0).getUri();
			String targetXpath = getXpathFromId(targetId);
			if (!targetInfo.containsKey(targetXpath)) {
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

		for (String t : targetInfo.keySet()) {
			addElementByPath(xpath, root2, t);
		}
		// Create another tree with extra info
		TreeNode _root = new TreeNode(new ArrayList<TreeNode>(), "/", root2.getNodeName(), null, "");
		handleTree(_root, root2, targetInfo, "");

		// generate XSLT templates.
		addTemplatesFromJSON(targetInfo, root, _root, sourceModel);

		return toString(doc);
	}

	public String generateJSONtoCSV(List<MappingInfoDTO> mappings, Model mappingsModel, Model sourceModel)
			throws Exception {
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
		separatorVariable.setAttribute("select", "'" + defaultSeparator + "'");
		Element newlineVariable = doc.createElementNS(xslNS, "variable");
		root.appendChild(newlineVariable);
		newlineVariable.setAttribute("name", "newline");
		newlineVariable.setAttribute("select", "'" + defaultNewline + "'");

		Element dataVariable = doc.createElementNS(xslNS, "variable");
		dataVariable.setAttribute("name", "data");
		dataVariable.setAttribute("select", "json-to-xml(./*)");
		root.appendChild(dataVariable);

		Element rootTemplate = doc.createElementNS(xslNS, "template");
		root.appendChild(rootTemplate);
		rootTemplate.setAttribute("match", "/");
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

		// find the row iterator
		List<String> rowIterators = getCSVRowIteratorSourceIDs(mappingsModel);
		for (String iteratorID : rowIterators) {
			Element forEach = doc.createElementNS(xslNS, "for-each");
			String iterator = getXpathFromId(iteratorID);
			String iteratorXMLJsonPath = getJsonXMLPath(iteratorID, iterator, sourceModel);
			rootTemplate.appendChild(forEach);
			forEach.setAttribute("select", "$data/*/" + iteratorXMLJsonPath);
			for (MappingInfoDTO m : mappings) {
				String sourceId = m.getSource().get(0).getUri();
				String sourceXpath = getXpathFromId(sourceId);
				if (!sourceXpath.equals(iterator)) {
					sourceXpath = sourceXpath.substring(iterator.length() + 1);
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
		separatorVariable.setAttribute("select", "'" + defaultSeparator + "'");
		Element newlineVariable = doc.createElementNS(xslNS, "variable");
		root.appendChild(newlineVariable);
		newlineVariable.setAttribute("name", "newline");
		newlineVariable.setAttribute("select", "'" + defaultNewline + "'");

		Element rootTemplate = doc.createElementNS(xslNS, "template");
		root.appendChild(rootTemplate);
		rootTemplate.setAttribute("match", "/");
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

		// find the row iterator
		List<String> rowIterators = getCSVRowIteratorSourceIDs(mappingsModel);
		for (String iteratorID : rowIterators) {
			Element forEach = doc.createElementNS(xslNS, "for-each");
			rootTemplate.appendChild(forEach);
			String iterator = getXpathFromId(iteratorID);
			forEach.setAttribute("select", iterator);
			for (MappingInfoDTO m : mappings) {
				String sourceId = m.getSource().get(0).getUri();
				String sourceXpath = getXpathFromId(sourceId);
				if (!sourceXpath.equals(iterator)) {
					sourceXpath = sourceXpath.substring(iterator.length() + 1);

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

	public record TreeNodeInfo(boolean isRepeatable, boolean isAttribute) {}
	
	public String generateXMLtoXML(List<MappingInfoDTO> mappings, Model sourceModel, String sourcePID,
			Model targetModel, String targetPID) throws Exception {

		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

		// generate helper data structures: targetxpath -> mappings and list of target
		Map<String, List<MappingInfoDTO>> targetInfo = new LinkedHashMap<String, List<MappingInfoDTO>>();
		Map<String, List<MappingInfoDTO>> sourceInfo = new LinkedHashMap<String, List<MappingInfoDTO>>();
		Map<String, List<String>> targetToSource = new LinkedHashMap<String, List<String>>();
		Map<String, List<String>> sourceToTarget = new LinkedHashMap<String, List<String>>();

		// Create XPathFactory object
		XPathFactory xpathFactory = XPathFactory.newInstance();
		// Create XPath object
		XPath xpath = xpathFactory.newXPath();

		List<String> namespaces = new ArrayList<String>();

		Document doc2 = docBuilder.newDocument();
		Element root2 = doc2.createElement("root2");
		doc2.appendChild(root2);
		initTargetInfo(mappings, targetInfo);
		
		for (String t : targetInfo.keySet()) {
			addElementByPath2(xpath, root2, t, targetPID, targetModel, namespaces);
		}

		initTargetAndSourceInfos(mappings, targetInfo, sourceInfo, targetToSource, sourceToTarget, sourceModel,
				targetModel, namespaces);
		// order path elements according to the target schema
		
		
		
		TreeNode2 treeNode = new TreeNode2(new TreeMap<Integer, TreeNode2>(), root2, "/", root2.getNodeName(), null,
				false, null, targetPID);
		handleTree(treeNode, targetInfo, targetPID);

		Map<String, TreeNodeInfo> uriToNode = new HashMap<String, TreeNodeInfo>();
		sourceModel.listSubjectsWithProperty(RDF.type, SH.PropertyShape).forEach(new Consumer<Resource>() {
			
			@Override
			public void accept(Resource t) {
				uriToNode.put(t.getURI(), new TreeNodeInfo(
						false,
						t.hasProperty(MSCR.sourceType) ? true : false
						
						));
			}
		});
		
		// TreeNode2 treeNode = initTree(docBuilder.newDocument(), targetPID,
		// targetModel, targetInfo, namespaces);
		Document doc = docBuilder.newDocument();

		Element root = doc.createElementNS(xslNS, "xsl:stylesheet");
		root.setAttribute("version", "2.0");
		doc.appendChild(root);

		for (int nsi = 0; nsi < namespaces.size(); nsi++) {
			root.setAttribute("xmlns:ns" + nsi, namespaces.get(nsi));
		}

		addTemplateXMLtoXML(uriToNode, root, treeNode.children, 0, "root", targetToSource, sourceToTarget, sourceInfo, targetInfo,
				"", namespaces, sourceModel);
		addRootTemplate(root);

		return toString(doc);

	}
	/*
	 * private TreeNode2 initTree(Document doc2, String targetPID, Model
	 * targetModel, Map<String, List<MappingInfoDTO>> targetInfo, List<String>
	 * namespaces) throws Exception { // Create XPathFactory object XPathFactory
	 * xpathFactory = XPathFactory.newInstance(); // Create XPath object XPath xpath
	 * = xpathFactory.newXPath();
	 * 
	 * 
	 * Element root2 = doc2.createElement("root2"); doc2.appendChild(root2);
	 * for(String t: targetInfo.keySet()) { addElementByPath2(xpath, root2, t,
	 * targetPID, targetModel); } // order path elements according to the target
	 * schema TreeNode2 treeNode = new TreeNode2(new TreeMap<Integer, TreeNode2>(),
	 * root2, "/", root2.getNodeName(), null, false, null, targetPID);
	 * handleTree(treeNode, targetInfo, targetPID, namespaces); return treeNode;
	 * 
	 * }
	 */

	private void initTargetInfo(List<MappingInfoDTO> mappings, Map<String, List<MappingInfoDTO>> targetInfo) {
		for (MappingInfoDTO mapping : mappings) {
			for(NodeInfo ni : mapping.getTarget()) {
				String targetId = ni.getUri();
				String targetXpath = getXpathFromId(targetId);
				if (!targetInfo.containsKey(targetXpath)) {
					targetInfo.put(targetXpath, new ArrayList<MappingInfoDTO>());
				}
				targetInfo.get(targetXpath).add(mapping);
				
			}

		}

	}

	private void initTargetAndSourceInfos(List<MappingInfoDTO> mappings, Map<String, List<MappingInfoDTO>> targetInfo,
			Map<String, List<MappingInfoDTO>> sourceInfo, Map<String, List<String>> targetToSource,
			Map<String, List<String>> sourceToTarget, Model sourceModel, Model targetModel, List<String> namespaces) {
		for (MappingInfoDTO mapping : mappings) {
			String targetId = mapping.getTarget().get(0).getUri();
			String targetXpath = getXpathFromId(targetId);

			String sourceId = mapping.getSource().get(0).getUri();
			String sourceXpath = getXpathFromUri(sourceId, sourceModel, namespaces);

			/*
			 * if(!targetInfo.containsKey(targetXpath)) { targetInfo.put(targetXpath, new
			 * ArrayList<MappingInfoDTO>()); } targetInfo.get(targetXpath).add(mapping);
			 */
			if (!sourceInfo.containsKey(sourceXpath)) {
				sourceInfo.put(sourceXpath, new ArrayList<MappingInfoDTO>());
			}
			sourceInfo.get(sourceXpath).add(mapping);

			if (!targetToSource.containsKey(targetXpath)) {
				targetToSource.put(targetXpath, new ArrayList<String>());
			}
			targetToSource.get(targetXpath).add(sourceXpath);

			if (!sourceToTarget.containsKey(sourceXpath)) {
				sourceToTarget.put(sourceXpath, new ArrayList<String>());
			}
			sourceToTarget.get(sourceXpath).add(targetXpath);

		}

	}

	private void addRootTemplate(Element root) {
		Document doc = root.getOwnerDocument();
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
	}

	private Element getRootTemplate(Element stylesheet, String name) {
		for (int i = 0; i < stylesheet.getChildNodes().getLength(); i++) {
			Element e = (Element) stylesheet.getChildNodes().item(i);
			if (name.equals(e.getAttribute("name"))) {
				return e;

			}
		}
		return null;
	}

	private void handleFunctions(Document doc, Element e, MappingInfoDTO m) {
		ProcessingInfo pi = m.getProcessing();
		if (pi.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#splitFunc")) {
			String delimiter = pi.getParams().get("delimiter").toString();
			// Element forEachToken = xsltDoc.createElementNS(xslNS, "xsl:for-each");
			List<NodeInfo> splitTargets = m.getTarget();

			for (int i = 1; i < splitTargets.size() + 1; i++) {
				Element t = doc.createElement(splitTargets.get(i - 1).getLabel());
				Element seq = doc.createElementNS(xslNS, "sequence");
				seq.setAttribute("select", "tokenize(.,'" + delimiter + "')[" + i + "]");
				t.appendChild(seq);
				e.appendChild(t);
			}
		} else if (pi.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#concatFunc")) {
			String delimiter = pi.getParams().get("delimiter").toString();
			List<NodeInfo> sources = m.getSource();
			String s = "";
			for (int i = 0; i < sources.size(); i++) {
				String sourcePath = getXpathFromId(sources.get(i).getUri());

				s = s + sourcePath;
				if (i < sources.size() - 1) {
					s = s + ",'" + delimiter + "',";
				}

			}
			Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");
			valueOf.setAttribute("select", "concat(" + s + ")");
			e.appendChild(valueOf);

		} else if (pi.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#constantFunc")) {
			String value = pi.getParams().get("value").toString();
			e.setTextContent(value);

		} else if (pi.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#filterFunc")) {

			String value = pi.getParams().get("value").toString();
			String property = pi.getParams().get("property").toString();
			Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");
			valueOf.setAttribute("select", "$node/resource/dates/date/dateValue[../" + property + "='" + value + "']");

			e.appendChild(valueOf);
		} else if (pi.getId().equals("http://uri.suomi.fi/datamodel/ns/mscr#normalizeSpaceFunc")) {
			List<NodeInfo> sources = m.getSource();
			if (sources.size() > 0) {
				throw new RuntimeException(
						"Normalize space function must have only one source. See mapping with target "
								+ m.getTarget().get(0).getLabel());
			}
			Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");
			valueOf.setAttribute("select", "normalize-space(.)");
			e.appendChild(valueOf);

		}
	}

	private Element createContentElement(Document doc, TreeNode2 targetNode, int mappingIndex, List<String> namespaces, boolean addForEach) {
		if(targetNode.isAttribute) {
			Element contentElement = doc.createElementNS(xslNS, "attribute");
			if (targetNode.targetElementNamespace != null
					&& !targetNode.targetElementNamespace.equals("")) {
				contentElement.setAttribute("namespace", targetNode.targetElementNamespace);
			}
			contentElement.setAttribute("name", targetNode.targetElementName);
			
			Element valueOf = doc.createElementNS(xslNS, "xsl:value-of");
			valueOf.setAttribute("select", "$value_" + mappingIndex);
			contentElement.appendChild(valueOf);
			return contentElement;
		}
		else {
			if(addForEach) {
				Element forEach = doc.createElementNS(xslNS, "xsl:for-each");
				forEach.setAttribute("select", "$value_" + mappingIndex);
				
				Element contentElement = null;
				if (targetNode.targetElementNamespace != null
						&& !targetNode.targetElementNamespace.equals("")) {
					contentElement = doc.createElementNS(targetNode.targetElementNamespace,
							"ns" + namespaces.indexOf(targetNode.targetElementNamespace) + ":"
									+ targetNode.targetElementName);
				} else {
					contentElement = doc.createElement(targetNode.targetElementName);
				}
											 
				Element contentValueOf = doc.createElementNS(xslNS, "value-of");						
				contentValueOf.setAttribute("select", "$value_" + mappingIndex);
				contentElement.appendChild(contentValueOf);
				forEach.appendChild(contentElement);			
				return forEach;
				
			}
			else {
				Element contentElement = null;
				if (targetNode.targetElementNamespace != null
						&& !targetNode.targetElementNamespace.equals("")) {
					contentElement = doc.createElementNS(targetNode.targetElementNamespace,
							"ns" + namespaces.indexOf(targetNode.targetElementNamespace) + ":"
									+ targetNode.targetElementName);
				} else {
					contentElement = doc.createElement(targetNode.targetElementName);
				}
											 
				Element contentValueOf = doc.createElementNS(xslNS, "value-of");						
				contentValueOf.setAttribute("select", "$value_" + mappingIndex);
				contentElement.appendChild(contentValueOf);
				return contentElement;
				
			}
		}
	}
	private void addTemplateXMLtoXML(Map<String, TreeNodeInfo> sourceUriToNode, Element stylesheet, Map<Integer, TreeNode2> children, int depth,
			String parentTemplateName, Map<String, List<String>> targetToSource,
			Map<String, List<String>> sourceToTarget, Map<String, List<MappingInfoDTO>> sourceInfo,
			Map<String, List<MappingInfoDTO>> targetInfo, String parentSourceXPath, List<String> namespaces, Model sourceModel) {
		Document doc = stylesheet.getOwnerDocument();
		for (TreeNode2 targetNode : children.values()) {
			boolean isLeafNode = targetNode.children.isEmpty();
			boolean isMapped = (targetNode.mappings != null && !targetNode.mappings.isEmpty());
			if (isMapped) {
				if (isLeafNode) {
					Element templateElement = getRootTemplate(stylesheet, parentTemplateName);
					if (templateElement == null) {
						templateElement = doc.createElementNS(xslNS, "xsl:template");
						templateElement.setAttribute("name", parentTemplateName);
						Element param = doc.createElementNS(xslNS, "xsl:param");
						param.setAttribute("name", "node");
						templateElement.appendChild(param);
						stylesheet.appendChild(templateElement);
					}

					String templateSourceXpath = parentTemplateName.substring(parentTemplateName.lastIndexOf("--") + 1)
							.replaceAll("-", "/").replace("_", ":");
					

					
					List<MappingInfoDTO> mappings = targetInfo.get(targetNode.targetXPath);
					int mappingIndex = 0;
					
					for(MappingInfoDTO mapping : mappings) {
						List<Element> contentElements = new ArrayList<Element>();
						// each mapping produces one or more leaf elements or exactly one attribute
						

						// for each of the mappings sources - do pre processing
						int sourceIndex = 0;
						for(NodeInfo ni : mapping.getSource()) {
							Element preProcessingVar = doc.createElementNS(xslNS, "xsl:variable");
							templateElement.appendChild(preProcessingVar);
							preProcessingVar.setAttribute("name", "preprocessed_" + sourceIndex);
							//String mappingSourceXPath = getXpathFromId(ni.getUri());
							String mappingSourceXPath = getXpathFromUri(ni.getUri(), sourceModel, namespaces);
							if(!parentSourceXPath.equals("/")) {
								mappingSourceXPath =  "$node"+mappingSourceXPath.substring(templateSourceXpath.length());
							}
							TreeNodeInfo node = sourceUriToNode.get(ni.getUri());
							if(node.isAttribute) {
								mappingSourceXPath = mappingSourceXPath.substring(0, mappingSourceXPath.lastIndexOf("/")+1)
										+ "@"
										+ mappingSourceXPath.substring(mappingSourceXPath.lastIndexOf("/")+1);
							}
							
							String preProcessingSelect = getProcessingSelect(mappingSourceXPath, ni.getProcessing());
							preProcessingVar.setAttribute("select", preProcessingSelect);
							
							sourceIndex++;
						}

						
						// run post processing for target elements
						int targetIndex = 0;
						
						for(NodeInfo ni : mapping.getTarget()) {
							if(mapping.getProcessing() != null) {
								String funcID = mapping.getProcessing().getId();
								if (funcID.equals("http://uri.suomi.fi/datamodel/ns/mscr#concatFunc")) {								
									Element processingVar = doc.createElementNS(xslNS, "xsl:variable");
									templateElement.appendChild(processingVar);
									String delimiter = mapping.getProcessing().getParams().get("delimiter").toString();
									List<NodeInfo> sources = mapping.getSource();
									String s = "";
									for (int i = 0; i < sources.size(); i++) {										
										s = s + "$preprocessed_" + i;
										if (i < sources.size() - 1) {
											s = s + ",'" + delimiter + "',";
										}
									}
									processingVar.setAttribute("name", "processed_0"); // always only one output
									processingVar.setAttribute("select", "concat(" + s + ")");
									
									Element postProcessingVar = doc.createElementNS(xslNS, "xsl:variable");
									templateElement.appendChild(postProcessingVar);
									postProcessingVar.setAttribute("name", "value_" + targetIndex);							
									String preProcessingSelect = getProcessingSelect("$processed_" + targetIndex, ni.getProcessing());
									postProcessingVar.setAttribute("select", preProcessingSelect);
									
									Element contentElement = createContentElement(doc, targetNode, mappingIndex, namespaces, false);
									contentElements.add(contentElement);
									

								}
								else if (funcID.equals("http://uri.suomi.fi/datamodel/ns/mscr#splitFunc")) {
									
									if(targetNode.id.equals(ni.getUri())) {
										Element processingVar = doc.createElementNS(xslNS, "xsl:variable");
										templateElement.appendChild(processingVar);
										
										String delimiter = mapping.getProcessing().getParams().get("delimiter").toString();
										processingVar.setAttribute("name", "processed_" + targetIndex);
										processingVar.setAttribute("select", "tokenize($preprocessed_" + mappingIndex +",'" + delimiter +"')[" + (targetIndex + 1) + "]");
										
										
										Element postProcessingVar = doc.createElementNS(xslNS, "xsl:variable");
										templateElement.appendChild(postProcessingVar);
										postProcessingVar.setAttribute("name", "value_" + targetIndex);							
										String preProcessingSelect = getProcessingSelect("$processed_" + targetIndex, ni.getProcessing());
										postProcessingVar.setAttribute("select", preProcessingSelect);

										
										Element contentElement = createContentElement(doc, targetNode, targetIndex, namespaces, false);
										contentElements.add(contentElement);	
										
										
									}
									
								}	
								else if (funcID.equals("http://uri.suomi.fi/datamodel/ns/mscr#addPrefixFunc")) {
									Element processingVar = doc.createElementNS(xslNS, "xsl:variable");
									templateElement.appendChild(processingVar);
									
									String prefix = mapping.getProcessing().getParams().get("prefix").toString();
									processingVar.setAttribute("name", "processed_" + targetIndex);
									processingVar.setAttribute("select", "concat('" + prefix +"', $preprocessed_" + mappingIndex +")");
									
									
									Element postProcessingVar = doc.createElementNS(xslNS, "xsl:variable");
									templateElement.appendChild(postProcessingVar);
									postProcessingVar.setAttribute("name", "value_" + targetIndex);							
									String preProcessingSelect = getProcessingSelect("$processed_" + targetIndex, ni.getProcessing());
									postProcessingVar.setAttribute("select", preProcessingSelect);

									
									Element contentElement = createContentElement(doc, targetNode, targetIndex, namespaces, false);
									contentElements.add(contentElement);	
								}
								else if (funcID.equals("http://uri.suomi.fi/datamodel/ns/mscr#addSuffixFunc")) {
									Element processingVar = doc.createElementNS(xslNS, "xsl:variable");
									templateElement.appendChild(processingVar);
									
									String prefix = mapping.getProcessing().getParams().get("prefix").toString();
									processingVar.setAttribute("name", "processed_" + targetIndex);
									processingVar.setAttribute("select", "concat($preprocessed_" + mappingIndex +",'" + prefix +"')");
									
									
									Element postProcessingVar = doc.createElementNS(xslNS, "xsl:variable");
									templateElement.appendChild(postProcessingVar);
									postProcessingVar.setAttribute("name", "value_" + targetIndex);							
									String preProcessingSelect = getProcessingSelect("$processed_" + targetIndex, ni.getProcessing());
									postProcessingVar.setAttribute("select", preProcessingSelect);

									
									Element contentElement = createContentElement(doc, targetNode, targetIndex, namespaces, false);
									contentElements.add(contentElement);	
								}	
								else if (funcID.equals("http://uri.suomi.fi/datamodel/ns/mscr#constantFunc")) {
									Element processingVar = doc.createElementNS(xslNS, "xsl:variable");
									templateElement.appendChild(processingVar);
									
									String value = mapping.getProcessing().getParams().get("value").toString();
									processingVar.setAttribute("name", "processed_" + targetIndex);
									processingVar.setAttribute("select", "'" + value + "'");
									
									
									Element postProcessingVar = doc.createElementNS(xslNS, "xsl:variable");
									templateElement.appendChild(postProcessingVar);
									postProcessingVar.setAttribute("name", "value_" + targetIndex);							
									String preProcessingSelect = getProcessingSelect("$processed_" + targetIndex, ni.getProcessing());
									postProcessingVar.setAttribute("select", preProcessingSelect);

									
									Element contentElement = createContentElement(doc, targetNode, targetIndex, namespaces, false);
									contentElements.add(contentElement);	
								}
								else if (funcID.equals("http://uri.suomi.fi/datamodel/ns/mscr#replaceFunc")) {
									Element processingVar = doc.createElementNS(xslNS, "xsl:variable");
									templateElement.appendChild(processingVar);
									
									String pattern = mapping.getProcessing().getParams().get("pattern").toString();
									String rep = mapping.getProcessing().getParams().get("replacement").toString();
									
									processingVar.setAttribute("name", "processed_" + targetIndex);
									processingVar.setAttribute("select", "replace($preprocessed_" + mappingIndex +", '" + pattern + "', '" + rep + "')");
									
									
									Element postProcessingVar = doc.createElementNS(xslNS, "xsl:variable");
									templateElement.appendChild(postProcessingVar);
									postProcessingVar.setAttribute("name", "value_" + targetIndex);							
									String preProcessingSelect = getProcessingSelect("$processed_" + targetIndex, ni.getProcessing());
									postProcessingVar.setAttribute("select", preProcessingSelect);

									
									Element contentElement = createContentElement(doc, targetNode, targetIndex, namespaces, false);
									contentElements.add(contentElement);	
								}									

							}
							else {	
								
								Element processingVar = doc.createElementNS(xslNS, "xsl:variable");
								templateElement.appendChild(processingVar);
								processingVar.setAttribute("name", "processed_" + targetIndex);
								processingVar.setAttribute("select", "$preprocessed_" + mappingIndex);
								
								Element postProcessingVar = doc.createElementNS(xslNS, "xsl:variable");
								templateElement.appendChild(postProcessingVar);
								postProcessingVar.setAttribute("name", "value_" + targetIndex);							
								String preProcessingSelect = getProcessingSelect("$processed_" + targetIndex, ni.getProcessing());
								postProcessingVar.setAttribute("select", preProcessingSelect);

								Element contentElement = createContentElement(doc, targetNode, mappingIndex, namespaces, true);
								contentElements.add(contentElement);

							}
							targetIndex++;
						}						
						
						for(Element ce : contentElements) {
							templateElement.appendChild(ce);
						}

						mappingIndex++;
					}
				} else {
					Element templateElement = getRootTemplate(stylesheet, parentTemplateName);
					if (templateElement == null) {
						templateElement = doc.createElementNS(xslNS, "xsl:template");
						templateElement.setAttribute("name", parentTemplateName);
						Element param = doc.createElementNS(xslNS, "xsl:param");
						param.setAttribute("name", "node");
						templateElement.appendChild(param);
						stylesheet.appendChild(templateElement);
					}

					for (String sourceXpath : targetToSource.get(targetNode.targetXPath)) {
						Element forEach = doc.createElementNS(xslNS, "xsl:for-each");
						forEach.setAttribute("select", "$node" + sourceXpath.substring(parentSourceXPath.length()));
						templateElement.appendChild(forEach);

						Element contentElement = null;
						if (targetNode.targetElementNamespace != null
								&& !targetNode.targetElementNamespace.equals("")) {
							contentElement = doc.createElementNS(targetNode.targetElementNamespace,
									"ns" + namespaces.indexOf(targetNode.targetElementNamespace) + ":"
											+ targetNode.targetElementName);
						} else {
							contentElement = doc.createElement(targetNode.targetElementName);
						}

						forEach.appendChild(contentElement);

						Element callTemplate = doc.createElementNS(xslNS, "xsl:call-template");
						contentElement.appendChild(callTemplate);
						String targetTemplateName = "t-" + targetNode.targetXPath.replaceAll("/", "-")
								+ "-children-source--" + sourceXpath.replaceAll("/", "-").replaceAll(":", "_");
						callTemplate.setAttribute("name", targetTemplateName);

						Element withParam = doc.createElementNS(xslNS, "xsl:with-param");
						callTemplate.appendChild(withParam);
						withParam.setAttribute("name", "node");
						withParam.setAttribute("select", ".");

						addTemplateXMLtoXML(sourceUriToNode, stylesheet, targetNode.children, depth + 1, targetTemplateName,
								targetToSource, sourceToTarget, sourceInfo, targetInfo, sourceXpath, namespaces, sourceModel);
					}
				}
			} else {
				Element templateElement = getRootTemplate(stylesheet, parentTemplateName);
				if (templateElement == null) {
					templateElement = doc.createElementNS(xslNS, "xsl:template");
					templateElement.setAttribute("name", parentTemplateName);
					Element param = doc.createElementNS(xslNS, "xsl:param");
					param.setAttribute("name", "node");
					templateElement.appendChild(param);
					stylesheet.appendChild(templateElement);
				}
				Element contentElement = null;
				if (targetNode.targetElementNamespace != null
						&& !targetNode.targetElementNamespace.equals("")) {
					contentElement = doc.createElementNS(targetNode.targetElementNamespace,
							"ns" + namespaces.indexOf(targetNode.targetElementNamespace) + ":"
									+ targetNode.targetElementName);
				} else {
					contentElement = doc.createElement(targetNode.targetElementName);
				}

				templateElement.appendChild(contentElement);

				Element callTemplate = doc.createElementNS(xslNS, "xsl:call-template");
				contentElement.appendChild(callTemplate);
				String templateNamePrefix = "";
				if(!parentTemplateName.equals("root")) {
					templateNamePrefix = parentTemplateName
						.substring(parentTemplateName.indexOf("-children-source"));
				}
				String targetTemplateName = "t-" + targetNode.targetXPath.replaceAll("/", "-").replaceAll(":", "_")
						+ templateNamePrefix;
				callTemplate.setAttribute("name", targetTemplateName);

				Element withParam = doc.createElementNS(xslNS, "xsl:with-param");
				callTemplate.appendChild(withParam);
				withParam.setAttribute("name", "node");
				withParam.setAttribute("select", "$node");

				stylesheet.appendChild(templateElement);

				addTemplateXMLtoXML(sourceUriToNode, stylesheet, targetNode.children, depth + 1, targetTemplateName, targetToSource,
						sourceToTarget, sourceInfo, targetInfo, parentSourceXPath, namespaces, sourceModel);
			}
		}
	}

	private String getProcessingSelect(String sourcePath, ProcessingInfo processing) {
		if(processing == null) {
			return sourcePath;
		}
		String funcID = processing.getId();
		if (funcID.equals("http://uri.suomi.fi/datamodel/ns/mscr#normalizeSpaceFunc")) {
			return "normalize-space(" + sourcePath + ")";
		}
		if (funcID.equals("http://uri.suomi.fi/datamodel/ns/mscr#addSuffixFunc")) {
			String param = processing.getParams().get("suffix").toString();			
			return "concat(" + sourcePath + ", '" + param + "')";
		}
		if (funcID.equals("http://uri.suomi.fi/datamodel/ns/mscr#addPrefixFunc")) {
			String param = processing.getParams().get("prefix").toString();			
			return "concat('" + param + "'," + sourcePath +")";
		}
		if (funcID.equals("http://uri.suomi.fi/datamodel/ns/mscr#constantFunc")) {
			String param = processing.getParams().get("value").toString();			
			return "'" + param + "'";
		}
		if (funcID.equals("http://uri.suomi.fi/datamodel/ns/mscr#replaceFunc")) {
			String pattern = processing.getParams().get("pattern").toString();
			String rep = processing.getParams().get("replacement").toString();
			return "replace(" + sourcePath + ", '" + pattern + "', '" + rep + "')";
		}		
		return sourcePath;
	}

	record TreeNode2(Map<Integer, TreeNode2> children, Element node, String targetXPath, String targetElementName,
			String targetElementNamespace, boolean isAttribute, List<MappingInfoDTO> mappings, String id) {
	};

	private void handleTree(TreeNode2 treeNode, Map<String, List<MappingInfoDTO>> infos, String targetPID) {
		NodeList _list = treeNode.node.getChildNodes();
		for (int i = 0; i < _list.getLength(); i++) {
			Element _node = (Element) _list.item(i);
			String cxpath = getXPath(_node).substring(6);
			String id = getPropertyNodeIdFromXpath(targetPID, cxpath);
			String namespace = _node.getAttribute("namespace");
			TreeNode2 child = new TreeNode2(new TreeMap<Integer, TreeNode2>(), _node, cxpath, _node.getNodeName(),
					namespace,
					_node.getAttribute("sourceType").equals(MSCR.sourceTypeAttribute.getURI()) ? true : false,
					infos.get(cxpath), id);
			treeNode.children.put(
					_node.getAttribute("order").equals("") ? -1 : Integer.parseInt(_node.getAttribute("order")), child);			
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

	record TreeNode(List<TreeNode> children, String targetXPath, String targetElementName,
			List<MappingInfoDTO> mappings, String id) {
	}

	public String generateJSONtoJSON(List<MappingInfoDTO> mappings, Model sourceModel, String sourcePID,
			Model targetModel, String targetPID) throws Exception {
		return "";
	}
}
