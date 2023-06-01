package fi.vm.yti.datamodel.api.v2.mapper;

import fi.vm.yti.datamodel.api.v2.dto.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;

import java.net.URI;
import java.util.*;

@Service
public class VisualizationMapper {
    private final Logger LOG = LoggerFactory.getLogger(VisualizationMapper.class);

    private String defaultNamespace;
    
    public VisualizationMapper(@Value("${defaultNamespace}") String defaultNamespace) {
    	this.defaultNamespace = defaultNamespace;
    }

    public List<VisualizationClassDTO> mapVisualizationData(String prefix, Model model, Model positions) {
        var graph = defaultNamespace + prefix;
        LOG.info("Map visualization data for {}", graph);

        var modelResource = model.getResource(graph);
        var classURIs = getClassURIs(model, graph);
        var namespaces = getNamespaces(model, graph);

        var result = new ArrayList<VisualizationClassDTO>();

        while (classURIs.hasNext()) {
            String classURI = classURIs.next().getSubject().getURI();
            var classResource = model.getResource(classURI);
            var classDTO = new VisualizationClassDTO();
            classDTO.setLabel(MapperUtils.localizedPropertyToMap(classResource, RDFS.label));
            classDTO.setIdentifier(classResource.getProperty(DCTerms.identifier).getString());
            classDTO.setParentClasses(getParentClasses(classResource, namespaces));

            var attributesAndAssociations = getAttributeAndAssociationResource(model, modelResource, classResource);
            var attributes = new ArrayList<VisualizationAttributeDTO>();
            var associations = new ArrayList<VisualizationAssociationDTO>();

            // TODO: how to distinguish between attribute and association in application profiles
            for (var resource : attributesAndAssociations) {
                if (MapperUtils.hasType(resource, OWL.DatatypeProperty)) {
                    var dto = new VisualizationAttributeDTO();
                    dto.setIdentifier(resource.getProperty(DCTerms.identifier).getString());
                    dto.setLabel(MapperUtils.localizedPropertyToMap(resource, RDFS.label));
                    attributes.add(dto);
                } else if (MapperUtils.hasType(resource, OWL.ObjectProperty)) {
                    var dto = new VisualizationAssociationDTO();
                    dto.setIdentifier(resource.getProperty(DCTerms.identifier).getString());
                    dto.setLabel(MapperUtils.localizedPropertyToMap(resource, RDFS.label));
                    var target = MapperUtils.propertyToString(resource, RDFS.range);

                    // TODO: store path from source to target via possible invisible nodes (with id #corner-12345678)
                    var path = new LinkedList<String>();
                    path.add(getReferenceIdentifier(target, namespaces));
                    dto.setPath(path);
                    associations.add(dto);
                }
            }

            classDTO.setAttributes(attributes);
            classDTO.setAssociations(associations);

            // TODO save and map positions
            var positionResource = positions.getResource(classURI);
            if (positionResource == null) {
                classDTO.setPosition(new PositionDTO(0.0, 0.0));
            }
            result.add(classDTO);
        }

        return result;
    }

    private StmtIterator getClassURIs(Model model, String graph) {
        var classType = MapperUtils.hasType(model.getResource(graph), OWL.Ontology)
                ? OWL.Class
                : SH.NodeShape;
        return model.listStatements(
                new SimpleSelector(null, RDF.type, classType));
    }

    /**
     * Map external and internal namespaces and prefixes
     * @param model model
     * @param graph graph
     */
    private HashMap<String, String> getNamespaces(Model model, String graph) {
        var namespaces = new HashMap<String, String>();
        var modelResource = model.getResource(graph);
        Arrays.asList(OWL.imports, DCTerms.requires)
                .forEach(prop -> modelResource.listProperties(prop).toList().forEach(ns -> {
                    var uri = ns.getObject().toString();
                    if (uri.startsWith(defaultNamespace)) {
                        namespaces.put(uri, uri.replace(defaultNamespace, ""));
                    } else {
                        namespaces.put(uri, model.getResource(uri).getProperty(DCAP.preferredXMLNamespacePrefix).getString());
                    }
                }));
        return namespaces;
    }

    @NotNull
    private HashSet<String> getParentClasses(Resource resource, Map<String, String> namespaceMap) {
        var parentClasses = new HashSet<String>();
        var subClassOf = resource.listProperties(RDFS.subClassOf).toList();
        subClassOf.forEach(parent -> {
            String classURI = parent.getObject().toString();
            // skip default parent class (owl#Thing)
            if ("http://www.w3.org/2002/07/owl#Thing".equals(classURI)) {
                return;
            }
            parentClasses.add(getReferenceIdentifier(classURI, namespaceMap));
        });
        return parentClasses;
    }

    private String getReferenceIdentifier(String uri, Map<String, String> namespaces) {
        try {
            var u = new URI(uri);
            String fragment = u.getFragment();

            if (fragment == null) {
                LOG.warn("No fragment found from URI {}", uri);
                return null;
            }

            String prefix = namespaces.get(uri.substring(0, uri.lastIndexOf("#")));

            if (prefix == null) {
                // referenced class in the external namespace or other model in Interoperability platform
                return fragment;
            } else {
                // referenced class in the same model
                return prefix + ":" + fragment;
            }
        } catch (Exception e) {
            LOG.warn("Invalid uri reference {}", uri);
            return null;
        }
    }

    private List<Resource> getAttributeAndAssociationResource(
            Model model, Resource modelResource, Resource classResource) {
        if (MapperUtils.hasType(modelResource, OWL.Ontology)) {
            return model.listSubjectsWithProperty(RDFS.domain, classResource).toList();
        }
        // TODO: application profile attributes and associations
        return List.of();
    }
}
