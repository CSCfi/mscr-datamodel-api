package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.xmlet.xsdparser.core.XsdParser;
import org.xmlet.xsdparser.xsdelements.XsdAbstractElement;
import org.xmlet.xsdparser.xsdelements.XsdAll;
import org.xmlet.xsdparser.xsdelements.XsdBuiltInDataType;
import org.xmlet.xsdparser.xsdelements.XsdChoice;
import org.xmlet.xsdparser.xsdelements.XsdComplexContent;
import org.xmlet.xsdparser.xsdelements.XsdComplexType;
import org.xmlet.xsdparser.xsdelements.XsdElement;
import org.xmlet.xsdparser.xsdelements.XsdExtension;
import org.xmlet.xsdparser.xsdelements.XsdMultipleElements;
import org.xmlet.xsdparser.xsdelements.XsdRestriction;
import org.xmlet.xsdparser.xsdelements.XsdSchema;
import org.xmlet.xsdparser.xsdelements.XsdSequence;
import org.xmlet.xsdparser.xsdelements.XsdSimpleContent;
import org.xmlet.xsdparser.xsdelements.XsdSimpleType;
import org.xmlet.xsdparser.xsdelements.xsdrestrictions.XsdPattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;

import fi.vm.yti.datamodel.api.v2.dto.SchemaParserResultDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaPart;

@Service
public class XSDMapper {

	private static final ObjectMapper m = new ObjectMapper();

	private void processSchema(SchemaPart part, String schemaLocation, Map<String, XsdSchema> map, Set<String> added) {
		SchemaPart newPart = new SchemaPart(schemaLocation, true);
		part.getHasPart().add(newPart);
		XsdSchema newSchema = map
				.get(map.keySet().stream().filter(key -> key.endsWith(schemaLocation)).findFirst().get());
		traverseSchemaTree(newPart, newSchema, map, added);

	}

	private void traverseSchemaTree(SchemaPart part, XsdSchema schema, Map<String, XsdSchema> map, Set<String> added) {
		if (schema.getChildrenImports() != null) {
			schema.getChildrenImports().forEach(i -> {
				if (!added.contains(i.getSchemaLocation())) {
					added.add(i.getSchemaLocation());
					processSchema(part, i.getSchemaLocation(), map, added);
				}

			});
		}
		if (schema.getChildrenIncludes() != null) {
			schema.getChildrenIncludes().forEach(i -> {
				if (!added.contains(i.getSchemaLocation())) {
					added.add(i.getSchemaLocation());
					processSchema(part, i.getSchemaLocation(), map, added);
				}

			});
		}

	}

	public SchemaParserResultDTO loadSchema(String filePath) {
		SchemaParserResultDTO r = new SchemaParserResultDTO();

		Properties systemProperties = System.getProperties();
		systemProperties.remove("javax.xml.parsers.DocumentBuilderFactory");
		System.setProperties(systemProperties);
		try {
			XsdParser p = new XsdParser(filePath);
			SchemaPart root = new SchemaPart(filePath, true);
			Map<String, XsdSchema> schemaMap = new HashMap<String, XsdSchema>();
			p.getResultXsdSchemas().forEach(schema -> schemaMap.put(schema.getFilePath(), schema));
			//schemaMap.keySet().stream().forEach(key -> System.out.println(key));
			//System.out.println("***");
			XsdSchema rootSchema = p.getResultXsdSchemas().filter(e -> e.getFilePath().equals(filePath)).findFirst()
					.get(); // since parsing was successful this should work
			//System.out.println(rootSchema.getTargetNamespace());
			//System.out.println("***");
			Set<String> added = new HashSet<String>();
			traverseSchemaTree(root, rootSchema, schemaMap, added);
			r.setTree(root);
			r.setOk(true);
		} catch (Exception ex) {
			r.setOk(false);
			if (ex.getCause() instanceof FileNotFoundException) {
				r.setMessage("File not found.");
			} else {
				r.setMessage(ex.getMessage());
			}

		}
		return r;
	}

	public ObjectNode mapToInternalJson(String filePath) {
		Properties systemProperties = System.getProperties();
		systemProperties.remove("javax.xml.parsers.DocumentBuilderFactory");
		System.setProperties(systemProperties);
		XsdParser p = new XsdParser(filePath);
		
		ObjectNode jroot = m.createObjectNode();
		ObjectNode rootProperties = m.createObjectNode();
		ObjectNode props = m.createObjectNode();
		jroot.put("type", "object");
		jroot.set("properties", rootProperties);
		jroot.put("$schema", "http://json-schema.org/draft-04/schema#");
		/*
		 * XsdElement root = getRootElement(p, filePath); if(root != null) {
		 * System.out.println("Root element: " + root.getName());
		 * rootProperties.set(root.getName(), props);
		 * 
		 * if(root.getXsdComplexType() != null) {
		 * handleComplexType(root.getXsdComplexType(), props, null); } } else {
		 * List<XsdElement> list = p.getResultXsdElements().toList(); list.forEach(e ->
		 * { handleElement(e, jroot, rootProperties); }); }
		 */
		
		List<XsdElement> list = p.getResultXsdElements().toList();
		list.forEach(e -> {
			Set<String> handledTypes = new HashSet<String>();
			handleElement(e, findSchema(e), jroot, rootProperties, handledTypes);
		});

		return jroot;
	}

	void handleDesc(XsdElement e, ObjectNode props) {
		String desc = getAnnotationStr(e);
		if (desc.length() > 0) {
			props.put("description", desc);
		} else {
			if (e.getCloneOf() != null && e instanceof XsdElement) {
				desc = getAnnotationStr((XsdElement) e.getCloneOf());
				if (desc.length() > 0) {
					props.put("description", desc);
				}
			}
		}
	}

	void handleElement(XsdElement e, XsdSchema schema, ObjectNode props, ObjectNode newProps, Set<String> handledTypes) {
		if(!handledTypes.contains(e.toString())) {
			handledTypes.add(e.toString());
		String elementId = null;
		String elementName = e.getName();
		if (schema != null ) {
			/*
			if(schema.getTargetNamespace().endsWith("#") || 
					schema.getTargetNamespace().endsWith("/")
					) {
				elementId = schema.getTargetNamespace() + e.getName();
			}
			else {
			*/
			if(schema.getTargetNamespace() != null) {
				elementId = schema.getTargetNamespace() + e.getName();
			}
			else {
				elementId = e.getName();
			}
			
				
		
		}
		/*
			*/
		
		
		//System.out.println(elementId);
		if (e.getTypeAsBuiltInDataType() != null) {
			//System.out.println("Builtin type");
			//System.out.println(e.getTypeAsBuiltInDataType().getName());
			ObjectNode prop = m.createObjectNode();
			// prop.put("type", e.getTypeAsBuiltInDataType().getName());
			handleBuiltInType(e.getTypeAsBuiltInDataType(), prop);
			handleDesc(e, prop);
			prop.put("title", elementName);
			if (elementId != null) {
				prop.put("@id", elementId);
			}

			if (e.getMaxOccurs() != null && e.getMaxOccurs().equals("unbounded")) {
				// newObj.put("mscr_repeatable", true);
				if (e.getParent() instanceof XsdMultipleElements
						&& ((XsdMultipleElements) e.getParent()).getXsdElements().count() == 1) {
					// array of these elements
					/*
					 * XsdElement arrayParent = findNextParentElement(e); if(arrayParent != null) {
					 * System.out.println(arrayParent.getName()); ObjectNode newObj =
					 * m.createObjectNode(); ObjectNode newProps2 = m.createObjectNode(); ObjectNode
					 * newProps3 = m.createObjectNode();
					 * 
					 * 
					 * newProps.set(arrayParent.getName(), newObj); newObj.put("type", "array");
					 * newObj.set("items", newProps2);
					 * 
					 * newProps2.put("type", "object"); newProps2.put("title", e.getName());
					 * newProps2.set("properties", newProps3); newProps3.set(e.getName(), prop); }
					 */
					ObjectNode newObj = m.createObjectNode();
					ObjectNode newProps2 = m.createObjectNode();
					ObjectNode newProps3 = m.createObjectNode();

					props.put("type", "array");
					props.set("items", newObj);
					props.remove("properties");

					newObj.put("type", "object");
					newObj.set("properties", newProps2);

					newProps2.set(e.getName(), prop);

				} else {
					newProps.set(elementName, prop);
				}
			} else {
				newProps.set(elementName, prop);
			}

			// newProps.set(elementName, prop);
		} else if (e.getXsdSimpleType() != null) {
			

			//System.out.println("Simple type");
			//System.out.println(e.getXsdSimpleType().getName());
			ObjectNode prop = handleSimpleType(e.getXsdSimpleType(), newProps);
			handleDesc(e, prop);
			prop.put("title", elementName);
			if (elementId != null) {
				prop.put("@id", elementId);
			}
			newProps.set(elementName, prop);
		} else if (e.getXsdComplexType() != null) {
			
			
			//System.out.println("Complex type: " + e.getXsdComplexType().getName());
				ObjectNode newObj = m.createObjectNode();
				//if (elementName.equals("FieldtripMethod")) {
				//	System.out.println("test");
				//}
	
				// special handling of repeatable element with the same name
				if(handleComplexType(schema, e.getXsdComplexType(), newObj, null, handledTypes)) {
					handleDesc(e, newObj);
					newObj.put("title", elementName);
					// additional conditions - multiple element the e belongs to only has one
					// element (this one)
					if (elementId != null) {
						newObj.put("@id", elementId);
					}
					if (e.getMaxOccurs() != null && e.getMaxOccurs().equals("unbounded")) {
						// newObj.put("mscr_repeatable", true);
						if (e.getParent() instanceof XsdMultipleElements
								&& ((XsdMultipleElements) e.getParent()).getXsdElements().count() == 1) {
							// array of these elements
							ObjectNode newObj2 = m.createObjectNode();
							ObjectNode newProps2 = m.createObjectNode();
							ObjectNode newProps3 = m.createObjectNode();
							/*
							 * XsdElement arrayParent = findNextParentElement(e); if(arrayParent != null) {
							 * System.out.println(arrayParent.getName());
							 * newProps.set(arrayParent.getName(), newObj); props.put("type", "array");
							 * props.set("items", newObj2); props.remove("properties");
							 * 
							 * newObj2.put("type", "object"); newObj2.set("properties", newProps2);
							 * 
							 * newProps2.set(e.getName(), newObj);
							 * 
							 * }
							 */
							props.put("type", "array");
							props.set("items", newObj2);
							props.remove("properties");
		
							newObj2.put("type", "object");
							newObj2.set("properties", newProps2);
		
							newProps2.set(e.getName(), newObj);
		
						} else {
							newProps.set(elementName, newObj);
						}
		
					} else {
						newProps.set(elementName, newObj);
					}	
				}
				
				
			

		}

		else {
			// just an element
			ObjectNode prop = m.createObjectNode();	
			if (elementId != null) {
				prop.put("@id", elementId);
			}			
			prop.put("type", "string");
			prop.put("title", e.getName());
			newProps.set(elementName, prop);
		}
		}

	}

	private void convertAndAddBuiltInType(XsdBuiltInDataType datatype, ArrayNode a, String value) {
		if (datatype != null) {

			String rawName = datatype.getRawName();
			if(rawName.indexOf(":") > 0) {
				rawName = rawName.substring(rawName.indexOf(":")+1);
			}
			switch (rawName) {
			case "string": {
				a.add(value);
				break;
			}
			case "integer": {
				a.add(Integer.parseInt(value));
				break;
			}
			case "positiveInteger": {
				a.add(Integer.parseInt(value));
				break;
			}
			case "negativeInteger": {
				a.add(Integer.parseInt(value));
				break;
			}
			case "nonNegativeInteger": {
				a.add(Integer.parseInt(value));
				break;
			}
			case "nonPositiveInteger": {
				a.add(Integer.parseInt(value));
				break;
			}
			case "long": {
				a.add(Long.parseLong(value));
				break;
			}
			case "unsignedLong": {
				a.add(Long.parseLong(value));
				break;
			}
			case "int": {
				a.add(Integer.parseInt(value));
				break;
			}
			case "unsignedInt": {
				a.add(Integer.parseInt(value));
				break;
			}
			case "short": {
				a.add(Integer.parseInt(value));
				break;
			}
			case "unsignedShort": {
				a.add(Integer.parseInt(value));
				break;
			}
			case "decimal": {
				a.add(Double.parseDouble(value));
				break;
			}
			case "float": {
				a.add(Float.parseFloat(value));
				break;
			}
			case "double": {
				a.add(Double.parseDouble(value));
				break;
			}
			default:
				a.add(value);

			}
		}
	}

	private void handleBuiltInType(XsdBuiltInDataType datatype, ObjectNode props) {
		//System.out.println(datatype.getRawName());
		String rawName = datatype.getRawName();
		if (rawName.indexOf(":") >= 0) {
			rawName = rawName.substring(rawName.indexOf(":") + 1);
		}
		//System.out.println(rawName);
		props.remove("properties"); // TODO: Do not add this in the first place
		switch (rawName) {
		case "string": {
			props.put("type", "string");
			break;
		}
		case "normalizedString": {
			props.put("type", "string");
			break;
		}
		case "token": {
			props.put("type", "string");
			break;
		}
		case "base64Binary": {
			props.put("type", "string");
			// 'pattern': '^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$'
			break;
		}
		case "hexBinary": {
			props.put("type", "string");
			// 'pattern': '^([0-9a-fA-F]{2})*$'
			break;
		}
		case "integer": {
			props.put("type", "integer");
			break;
		}
		case "positiveInteger": {
			props.put("type", "integer");
			props.put("minimum", 0);
			props.put("exclusiveMinimum", true);
			break;
		}
		case "negativeInteger": {
			props.put("type", "integer");
			props.put("maximum", 0);
			props.put("exclusiveMaximum", true);
			break;
		}
		case "nonNegativeInteger": {
			props.put("type", "integer");
			props.put("minimum", 0);
			props.put("exclusiveMinimum", false);
			break;
		}
		case "nonPositiveInteger": {
			props.put("type", "integer");
			props.put("maximum", 0);
			props.put("exclusiveMaximum", false);
			break;
		}
		case "long": {
			props.put("type", "integer");
			props.put("minimum", Long.MIN_VALUE);
			props.put("maximum", Long.MAX_VALUE);
			props.put("exclusiveMaximum", false);
			props.put("exclusiveMinimum", false);
			break;
		}
		case "unsignedLong": {
			props.put("type", "integer");
			props.put("minimum", 0);
			props.put("maximum", UnsignedLong.MAX_VALUE.longValue());
			props.put("exclusiveMaximum", false);
			props.put("exclusiveMinimum", false);
			break;
		}
		case "int": {
			props.put("type", "integer");
			props.put("minimum", Integer.MIN_VALUE);
			props.put("maximum", Integer.MAX_VALUE);
			props.put("exclusiveMaximum", false);
			props.put("exclusiveMinimum", false);
			break;
		}
		case "unsignedInt": {
			props.put("type", "integer");
			props.put("minimum", 0);
			props.put("maximum", UnsignedInteger.MAX_VALUE.intValue());
			props.put("exclusiveMaximum", false);
			props.put("exclusiveMinimum", false);
			break;
		}
		case "short": {
			props.put("type", "integer");
			props.put("exclusiveMaximum", false);
			props.put("exclusiveMinimum", false);
			break;
		}
		case "unsignedShort": {
			props.put("type", "integer");
			props.put("minimum", 0);
			props.put("exclusiveMaximum", false);
			props.put("exclusiveMinimum", false);
			break;
		}
		case "byte": {
			props.put("type", "integer");
			props.put("minimum", -128);
			props.put("maximum", 127);
			props.put("exclusiveMaximum", false);
			props.put("exclusiveMinimum", false);
			break;
		}
		case "unsignedByte": {
			props.put("type", "integer");
			props.put("minimum", 0);
			props.put("maximum", 255);
			props.put("exclusiveMaximum", false);
			props.put("exclusiveMinimum", false);
			break;
		}
		case "decimal": {
			props.put("type", "number");
			break;
		}
		case "float": {
			props.put("type", "number");
			break;
		}
		case "double": {
			props.put("type", "number");
			break;
		}
		case "duration": {
			props.put("type", "string");
			props.put("pattern", "^P(?!$)(\\d+Y)?(\\d+M)?(\\d+W)?(\\d+D)?(T(?=\\d+[HMS])(\\d+H)?(\\d+M)?(\\d+S)?)?$");
			break;
		}
		case "dateTime": {
			props.put("type", "string");
			props.put("pattern",
					"^([\\+-]?\\d{4}(?!\\d{2}\\b))((-?)((0[1-9]|1[0-2])(\\3([12]\\d|0[1-9]|3[01]))?|W([0-4]\\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\\d|[12]\\d{2}|3([0-5]\\d|6[1-6])))(T((([01]\\d|2[0-3])((:?)[0-5]\\d)?|24\\:?00)([\\.,]\\d+(?!:))?)?(\\17[0-5]\\d([\\.,]\\d+)?)?([zZ]|([\\+-])([01]\\d|2[0-3]):?([0-5]\\d)?)?)?)?$");
			break;
		}
		case "date": {
			props.put("type", "string");
			props.put("pattern", "^\\d{4}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$");
			break;
		}
		case "time": {
			props.put("type", "string");
			props.put("pattern", "^([01]\\d|2[0-3]):([0-5]\\d)(?::([0-5]\\d)(.(\\d{3}))?)?$");
			break;
		}
		case "gYear": {
			props.put("type", "integer");
			props.put("minimum", 1);
			props.put("maximum", 9999);
			props.put("exclusiveMaximum", false);
			props.put("exclusiveMinimum", false);
			break;

		}
		case "gYearMonth": {
			props.put("type", "string");
			props.put("pattern", "^(19|20)\\d\\d-(0[1-9]|1[012])$");
			break;

		}
		case "gMonth": {
			props.put("type", "integer");
			props.put("minimum", 1);
			props.put("maximum", 12);
			props.put("exclusiveMaximum", false);
			props.put("exclusiveMinimum", false);
			break;

		}
		case "gMonthDay": {
			props.put("type", "string");
			props.put("pattern", "^(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$");
			break;

		}
		case "gDay": {
			props.put("type", "integer");
			props.put("minimum", 1);
			props.put("maximum", 31);
			props.put("exclusiveMaximum", false);
			props.put("exclusiveMinimum", false);
			break;

		}
		case "Name": {
			props.put("type", "string");
		}
		case "QName": {
			props.put("type", "string");
		}
		case "NCName": {
			props.put("type", "string");
		}
		case "anyURI": {
			props.put("type", "string");
			// props.put("pattern",
			// "^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?");
			break;
		}
		case "anySimpleType": {
			ArrayNode a = m.createArrayNode();
			a.add(m.createObjectNode().put("type", "integer"));
			a.add(m.createObjectNode().put("type", "string"));
			a.add(m.createObjectNode().put("type", "number"));
			a.add(m.createObjectNode().put("type", "boolean"));
			a.add(m.createObjectNode().put("type", "null"));
			props.set("oneOf", a);
		}
		case "boolean": {
			props.put("type", "boolean");
		}
		default:
			props.put("type", "unknown");
		}
		// TODO: missing data types

	}

	ObjectNode handleMaxOccurs(String maxOccurs, ObjectNode props) {
		if (maxOccurs.equals("unbounded")) {
			ObjectNode newObj = m.createObjectNode();
			ObjectNode newProps = m.createObjectNode();
			if (props.get("type") != null) {
				newObj.set("type", props.get("type"));
			} else {
				newObj.put("type", "object");
			}

			newObj.set("properties", newProps);
			props.remove("properties");
			props.set("items", newObj);
			props.put("type", "array");

			return newProps;
		}
		return props;
	}

	XsdSchema findSchema(XsdAbstractElement e) {
		
		XsdAbstractElement parent = e.getParent();
		if(parent == null) {
			if(e.getCloneOf() != null) {
				parent = e.getCloneOf().getParent();	
			}			
		}
		if(parent == null) {
			try {
				parent = e.getParent(true);
			}catch(Exception ex) {}
		}
		
		if(parent != null) {
			if((parent instanceof XsdSchema)) {
				return (XsdSchema)parent;
			}
			return findSchema(parent);
		}
		else {
			return null;
		}
	}

	
	XsdElement findNextParentElement(XsdAbstractElement e) {
		XsdAbstractElement p = e.getParent();
		if (p != null && !(p instanceof XsdSchema)) {
			if (p instanceof XsdElement) {
				return (XsdElement) p;
			} else {
				return findNextParentElement(p.getParent());
			}
		} else {
			return null;
		}
	}

	boolean handleComplexType(XsdSchema schema, XsdComplexType e, ObjectNode props, ObjectNode newProps, Set<String> handledTypes) {
		String ctypeName = e.getName();
		
		if(!"".equals(ctypeName)  && !handledTypes.contains(ctypeName)) {
			if(ctypeName != null) {
				handledTypes.add(e.getName());	
			}
			
			props.put("type", "object");
			if (newProps == null) {
				newProps = m.createObjectNode();
				props.set("properties", newProps);
			}
	
			if (e.getSimpleContent() != null) {
	
				XsdSimpleContent c = e.getSimpleContent();
				// TODO - also requires handling of attributes
				if (c.getXsdExtension() != null) {
					XsdExtension ext = c.getXsdExtension();
					if (ext.getBaseAsBuiltInDataType() != null) {
						handleBuiltInType(ext.getBaseAsBuiltInDataType(), props);
					} else if (ext.getBaseAsSimpleType() != null) {
						ObjectNode prop = handleSimpleType(ext.getBaseAsSimpleType(), null);
						props.set("type", prop.get("type"));
						props.remove("properties");
					} else if (ext.getBaseAsComplexType() != null) {
						handleComplexType(schema, ext.getBaseAsComplexType(), props, newProps, handledTypes);
					}
				}
	
			} else if (e.getComplexContent() != null) {
				XsdComplexContent c = e.getComplexContent();
				// TODO
				// Child elements xs:annotation, xs:extension, xs:restriction
				if (c.getXsdExtension() != null) {
					XsdExtension ext = c.getXsdExtension();
					if (ext.getBaseAsComplexType() != null) {
						handleComplexType(schema, ext.getBaseAsComplexType(), props, newProps, handledTypes);
					}
					if (ext.getChildAsSequence() != null) {
	
						handleMultipleElements(schema, ext.getChildAsSequence(), props, newProps, handledTypes);
					}
				}
	
			}
			try {
				XsdChoice c = e.getChildAsChoice();
				handleMultipleElements(schema, c, props, newProps, handledTypes);
			} catch (Exception ex) {
	
			}
			try {
				XsdAll a = e.getChildAsAll();
				handleMultipleElements(schema, a, props, newProps, handledTypes);
			} catch (Exception ex) {
	
			}
			try {
				XsdChoice c = e.getChildAsChoice();
				handleMultipleElements(schema, c, props, newProps, handledTypes);
			} catch (Exception ex) {
	
			}
			try {
				XsdSequence seq = e.getChildAsSequence();
				handleMultipleElements(schema, seq, props, newProps, handledTypes);
			} catch (Exception ex) {
	
			}
			return true;
		}
		else {
			return false;
		}
			

		/*
		 * if(e.getChildAsAll() != null) { }
		 */
		/*
		 * if(e.getChildAsSequence() != null) { XsdSequence seq =
		 * e.getChildAsSequence();
		 */
		/*
		 * if(seq.getMaxOccurs() != null) { newProps =
		 * handleMaxOccurs(seq.getMaxOccurs(), newProps); }
		 */
		/*
		 * if(e.getParent() instanceof XsdElement) { ObjectNode newObj =
		 * m.createObjectNode(); ObjectNode newProps2 = m.createObjectNode();
		 * newObj.set("properties", newProps2);
		 * newProps.set(((XsdElement)e.getParent()).getName(), newObj);
		 * handleMultipleElements(seq, newObj, newProps2); }
		 */
		// handleMultipleElements(seq, props, newProps);
		// }
	}

	private void handleMultipleElements(XsdSchema schema, XsdMultipleElements c, ObjectNode props, ObjectNode newProps, Set<String> handledTypes) {
		List<XsdAbstractElement> aes = c.getXsdElements().collect(Collectors.toList());

		for (XsdAbstractElement ae : aes) {

			if (ae instanceof XsdElement) {
				XsdSchema newSchema = findSchema(ae);
				if(newSchema != null) {
					handleElement((XsdElement) ae, newSchema, props, newProps, handledTypes);
				}
				else {
					handleElement((XsdElement) ae, schema, props, newProps, handledTypes);	
				}
				
			}
			if (ae instanceof XsdMultipleElements) {
				handleMultipleElements(schema, (XsdMultipleElements) ae, props, newProps, handledTypes);
			}
		}

	}


	ObjectNode handleSimpleType(XsdSimpleType e, ObjectNode props) {
		ObjectNode prop = m.createObjectNode();
		prop.put("type", "string"); // TODO: fix this default
		// (annotation?,(restriction|list|union))
		// TODO: handle multiple restrctions
		if (e.getUnion() != null) {
			// TODO: handle unions
		} else if (e.getRestriction() != null) {
			XsdRestriction r = e.getRestriction();

			if (r.getBaseAsBuiltInDataType() != null) {
				handleBuiltInType(r.getBaseAsBuiltInDataType(), prop);
			} // TODO: other types
			if (r.getPattern() != null) {
				XsdPattern p = r.getPattern();
				prop.put("pattern", p.getValue());
			}
			if (r.getEnumeration() != null && r.getEnumeration().size() > 0) {
				ArrayNode a = m.createArrayNode();				
				XsdBuiltInDataType datatype = r.getBaseAsBuiltInDataType();
				if(datatype != null) {
					r.getEnumeration().forEach(en -> {
						convertAndAddBuiltInType(datatype, a, en.getValue());
					});
					prop.set("enum", a);					
				}

			}
		}

		return prop;
	}

	private String getAnnotationStr(XsdElement e) {
		StringBuffer docStr = new StringBuffer();
		if (e.getAnnotation() != null) {
			e.getAnnotation().getDocumentations().forEach(d -> docStr.append(d.getContent()));
		}
		return docStr.toString();

	}

	XsdElement getRootElement(XsdParser p, String rootFilePath) {
		List<XsdSchema> s = p.getResultXsdSchemas().collect(Collectors.toList());
		XsdElement root = null;
		for (XsdSchema _s : s) {

			if (_s.getFilePath().equals(rootFilePath)) {
				List<XsdElement> elements = _s.getChildrenElements().collect(Collectors.toList());
				if (elements.size() > 1) {
					return root;
				}
				root = elements.get(0);
			}

		}
		return root;

	}
}
