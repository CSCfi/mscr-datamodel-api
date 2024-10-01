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
			XsdSchema rootSchema = p.getResultXsdSchemas().filter(e -> e.getFilePath().equals(filePath)).findFirst()
					.get(); // since parsing was successful this should work
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
		jroot.put("type", "object");
		jroot.set("properties", rootProperties);
		jroot.put("$schema", "http://json-schema.org/draft-04/schema#");	
		List<XsdElement> list = p.getResultXsdElements().toList();
		list.forEach(e -> {
			Set<Object> handledTypes = new HashSet<Object>();
			handleElement(e, findSchema(e), jroot, handledTypes);
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

	void handleElement(XsdElement e, XsdSchema schema, ObjectNode parentObj, Set<Object> handledTypes) {
		if (!handledTypes.contains(e)) {
			ObjectNode obj = m.createObjectNode();
			handledTypes.add(e);
			String elementNamespace = null;
			String elementName = e.getName();
			if (schema != null) {				
				if (e.getXsdSchema() != null && e.getXsdSchema().getTargetNamespace() != null) {
					elementNamespace = e.getXsdSchema().getTargetNamespace();
				}				
				else if  (findSchema(e) != null && findSchema(e).getTargetNamespace() != null) {
					elementNamespace = findSchema(e).getTargetNamespace();
				}
				else if(schema.getTargetNamespace() != null) {
					elementNamespace = schema.getTargetNamespace();					
				}
			}
			if (e.getTypeAsBuiltInDataType() != null) {
				handleBuiltInType(e.getTypeAsBuiltInDataType(), obj);		
			} else if (e.getXsdSimpleType() != null) {
				handleSimpleType(e.getXsdSimpleType(), obj);				
			} else if (e.getXsdComplexType() != null) {
				handleComplexType(schema, e.getXsdComplexType(), obj, handledTypes);
			} else {
				// just an element
				obj.put("type", "string");
			}
			handleDesc(e, obj);
			if (elementNamespace != null) {
				obj.put("namespace", elementNamespace);
			}			
			obj.put("title", e.getName());
			handleCardinalities(e, obj);
			ObjectNode properties = (ObjectNode)parentObj.get("properties");
			properties.set(elementName, obj);
			
		}

	}

	private void convertAndAddBuiltInType(XsdBuiltInDataType datatype, ArrayNode a, String value) {
		if (datatype != null) {

			String rawName = datatype.getRawName();
			if (rawName.indexOf(":") > 0) {
				rawName = rawName.substring(rawName.indexOf(":") + 1);
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

	private void handleBuiltInType(XsdBuiltInDataType datatype, ObjectNode obj) {
		// System.out.println(datatype.getRawName());		
		String rawName = datatype.getRawName();
		if (rawName.indexOf(":") >= 0) {
			rawName = rawName.substring(rawName.indexOf(":") + 1);
		}
		// System.out.println(rawName);
		switch (rawName) {
		case "string": {
			obj.put("type", "string");
			break;
		}
		case "normalizedString": {
			obj.put("type", "string");
			break;
		}
		case "token": {
			obj.put("type", "string");
			break;
		}
		case "base64Binary": {
			obj.put("type", "string");
			// 'pattern': '^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$'
			break;
		}
		case "hexBinary": {
			obj.put("type", "string");
			// 'pattern': '^([0-9a-fA-F]{2})*$'
			break;
		}
		case "integer": {
			obj.put("type", "integer");
			break;
		}
		case "positiveInteger": {
			obj.put("type", "integer");
			obj.put("minimum", 0);
			obj.put("exclusiveMinimum", true);
			break;
		}
		case "negativeInteger": {
			obj.put("type", "integer");
			obj.put("maximum", 0);
			obj.put("exclusiveMaximum", true);
			break;
		}
		case "nonNegativeInteger": {
			obj.put("type", "integer");
			obj.put("minimum", 0);
			obj.put("exclusiveMinimum", false);
			break;
		}
		case "nonPositiveInteger": {
			obj.put("type", "integer");
			obj.put("maximum", 0);
			obj.put("exclusiveMaximum", false);
			break;
		}
		case "long": {
			obj.put("type", "integer");
			obj.put("minimum", Long.MIN_VALUE);
			obj.put("maximum", Long.MAX_VALUE);
			obj.put("exclusiveMaximum", false);
			obj.put("exclusiveMinimum", false);
			break;
		}
		case "unsignedLong": {
			obj.put("type", "integer");
			obj.put("minimum", 0);
			obj.put("maximum", UnsignedLong.MAX_VALUE.longValue());
			obj.put("exclusiveMaximum", false);
			obj.put("exclusiveMinimum", false);
			break;
		}
		case "int": {
			obj.put("type", "integer");
			obj.put("minimum", Integer.MIN_VALUE);
			obj.put("maximum", Integer.MAX_VALUE);
			obj.put("exclusiveMaximum", false);
			obj.put("exclusiveMinimum", false);
			break;
		}
		case "unsignedInt": {
			obj.put("type", "integer");
			obj.put("minimum", 0);
			obj.put("maximum", UnsignedInteger.MAX_VALUE.intValue());
			obj.put("exclusiveMaximum", false);
			obj.put("exclusiveMinimum", false);
			break;
		}
		case "short": {
			obj.put("type", "integer");
			obj.put("exclusiveMaximum", false);
			obj.put("exclusiveMinimum", false);
			break;
		}
		case "unsignedShort": {
			obj.put("type", "integer");
			obj.put("minimum", 0);
			obj.put("exclusiveMaximum", false);
			obj.put("exclusiveMinimum", false);
			break;
		}
		case "byte": {
			obj.put("type", "integer");
			obj.put("minimum", -128);
			obj.put("maximum", 127);
			obj.put("exclusiveMaximum", false);
			obj.put("exclusiveMinimum", false);
			break;
		}
		case "unsignedByte": {
			obj.put("type", "integer");
			obj.put("minimum", 0);
			obj.put("maximum", 255);
			obj.put("exclusiveMaximum", false);
			obj.put("exclusiveMinimum", false);
			break;
		}
		case "decimal": {
			obj.put("type", "number");
			break;
		}
		case "float": {
			obj.put("type", "number");
			break;
		}
		case "double": {
			obj.put("type", "number");
			break;
		}
		case "duration": {
			obj.put("type", "string");
			obj.put("pattern", "^P(?!$)(\\d+Y)?(\\d+M)?(\\d+W)?(\\d+D)?(T(?=\\d+[HMS])(\\d+H)?(\\d+M)?(\\d+S)?)?$");
			break;
		}
		case "dateTime": {
			obj.put("type", "string");
			obj.put("pattern",
					"^([\\+-]?\\d{4}(?!\\d{2}\\b))((-?)((0[1-9]|1[0-2])(\\3([12]\\d|0[1-9]|3[01]))?|W([0-4]\\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\\d|[12]\\d{2}|3([0-5]\\d|6[1-6])))(T((([01]\\d|2[0-3])((:?)[0-5]\\d)?|24\\:?00)([\\.,]\\d+(?!:))?)?(\\17[0-5]\\d([\\.,]\\d+)?)?([zZ]|([\\+-])([01]\\d|2[0-3]):?([0-5]\\d)?)?)?)?$");
			break;
		}
		case "date": {
			obj.put("type", "string");
			obj.put("pattern", "^\\d{4}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$");
			break;
		}
		case "time": {
			obj.put("type", "string");
			obj.put("pattern", "^([01]\\d|2[0-3]):([0-5]\\d)(?::([0-5]\\d)(.(\\d{3}))?)?$");
			break;
		}
		case "gYear": {
			obj.put("type", "integer");
			obj.put("minimum", 1);
			obj.put("maximum", 9999);
			obj.put("exclusiveMaximum", false);
			obj.put("exclusiveMinimum", false);
			break;

		}
		case "gYearMonth": {
			obj.put("type", "string");
			obj.put("pattern", "^(19|20)\\d\\d-(0[1-9]|1[012])$");
			break;

		}
		case "gMonth": {
			obj.put("type", "integer");
			obj.put("minimum", 1);
			obj.put("maximum", 12);
			obj.put("exclusiveMaximum", false);
			obj.put("exclusiveMinimum", false);
			break;

		}
		case "gMonthDay": {
			obj.put("type", "string");
			obj.put("pattern", "^(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$");
			break;

		}
		case "gDay": {
			obj.put("type", "integer");
			obj.put("minimum", 1);
			obj.put("maximum", 31);
			obj.put("exclusiveMaximum", false);
			obj.put("exclusiveMinimum", false);
			break;

		}
		case "Name": {
			obj.put("type", "string");
		}
		case "QName": {
			obj.put("type", "string");
		}
		case "NCName": {
			obj.put("type", "string");
		}
		case "anyURI": {
			obj.put("type", "string");
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
			obj.set("oneOf", a);
		}
		case "boolean": {
			obj.put("type", "boolean");
		}
		default:
			obj.put("type", "unknown");
		}
		// TODO: missing data types

	}

	void handleCardinalities(XsdElement e, ObjectNode props) {				
		if (e.getMaxOccurs() != null) {
			if (!"unbounded".equals(e.getMaxOccurs())) {
				props.put("maxItems", Integer.parseInt(e.getMaxOccurs()));
			}
			else {
				props.put("maxItems", "unbounded");
			}
		}
		else {
			props.put("maxItems", 1);
		}
		if (e.getMinOccurs() != null) {
			props.put("minItems", e.getMinOccurs());			
		}
		else {
			props.put("minItems", 1);
		}

	}

	private XsdSchema findSchema(XsdAbstractElement e) {

		XsdAbstractElement parent = e.getParent();
		if (parent == null) {
			if (e.getCloneOf() != null) {
				parent = e.getCloneOf().getParent();
			}
		}
		if (parent == null) {
			try {
				parent = e.getParent(true);
			} catch (Exception ex) {
			}
		}

		if (parent != null) {
			if ((parent instanceof XsdSchema)) {
				return (XsdSchema) parent;
			}
			return findSchema(parent);
		} else {
			return null;
		}
	}

	void handleComplexType(XsdSchema schema, XsdComplexType e, ObjectNode obj,
			Set<Object> handledTypes) {
		String ctypeName = e.getName();
		if (!"".equals(ctypeName) && !handledTypes.contains(ctypeName)) {
			obj.put("type", "object");
			ObjectNode properties = (ObjectNode)obj.get("properties");
			if(properties == null) {
				properties = m.createObjectNode();
			}
			
			obj.set("properties", properties);

			if (e.getSimpleContent() != null) {
				
				XsdSimpleContent c = e.getSimpleContent();
				// TODO - also requires handling of attributes
				if (c.getXsdExtension() != null) {
					XsdExtension ext = c.getXsdExtension();
					if (ext.getBaseAsBuiltInDataType() != null) {
						obj.remove("properties");
						handleBuiltInType(ext.getBaseAsBuiltInDataType(), obj);
					} else if (ext.getBaseAsSimpleType() != null) {
						obj.remove("properties");
						handleSimpleType(ext.getBaseAsSimpleType(), obj);
					} else if (ext.getBaseAsComplexType() != null) {
						handleComplexType(schema, ext.getBaseAsComplexType(), obj, handledTypes);
					}
				}

			} else if (e.getComplexContent() != null) {
				XsdComplexContent c = e.getComplexContent();
				// TODO
				// Child elements xs:annotation, xs:extension, xs:restriction
				if (c.getXsdExtension() != null) {
					XsdExtension ext = c.getXsdExtension();
					if (ext.getBaseAsComplexType() != null) {
						handleComplexType(schema, ext.getBaseAsComplexType(), obj, handledTypes);
					}
					if (ext.getChildAsSequence() != null) {

						handleMultipleElements(schema, ext.getChildAsSequence(), obj, handledTypes);
					}
				}

			}
			try {
				XsdSequence seq = e.getChildAsSequence();
				handleMultipleElements(schema, seq, obj, handledTypes);
			} catch (Exception ex) {

			}
			try {
				XsdAll a = e.getChildAsAll();
				handleMultipleElements(schema, a, obj, handledTypes);
			} catch (Exception ex) {

			}
			try {
				XsdChoice c = e.getChildAsChoice();
				handleMultipleElements(schema, c, obj, handledTypes);
			} catch (Exception ex) {

			}
		}
	}

	private void handleMultipleElements(XsdSchema schema, XsdMultipleElements c, ObjectNode obj,
			Set<Object> handledTypes) {
		List<XsdAbstractElement> aes = c.getXsdElements().collect(Collectors.toList());

		for (XsdAbstractElement ae : aes) {
			if (ae instanceof XsdElement) {
				XsdSchema newSchema = findSchema(ae);				
				if (newSchema != null) {
					handleElement((XsdElement) ae, newSchema, obj, handledTypes);
				} else {
					handleElement((XsdElement) ae, schema, obj, handledTypes);
				}
			}
			else if (ae instanceof XsdMultipleElements) {
				handleMultipleElements(schema, (XsdMultipleElements) ae, obj, handledTypes);
			}
		}
	}

	void handleSimpleType(XsdSimpleType e, ObjectNode obj) {		
		obj.put("type", "string"); // TODO: fix this default
		// (annotation?,(restriction|list|union))
		// TODO: handle multiple restrictions
		if (e.getUnion() != null) {
			// TODO: handle unions
		} else if (e.getRestriction() != null) {
			XsdRestriction r = e.getRestriction();

			if (r.getBaseAsBuiltInDataType() != null) {
				handleBuiltInType(r.getBaseAsBuiltInDataType(), obj);
			} // TODO: other types
			if (r.getPattern() != null) {
				XsdPattern p = r.getPattern();
				obj.put("pattern", p.getValue());
			}
			if (r.getEnumeration() != null && r.getEnumeration().size() > 0) {
				ArrayNode a = m.createArrayNode();
				XsdBuiltInDataType datatype = r.getBaseAsBuiltInDataType();
				if (datatype != null) {
					r.getEnumeration().forEach(en -> {
						convertAndAddBuiltInType(datatype, a, en.getValue());
					});
					obj.set("enum", a);
				}

			}
		}
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
