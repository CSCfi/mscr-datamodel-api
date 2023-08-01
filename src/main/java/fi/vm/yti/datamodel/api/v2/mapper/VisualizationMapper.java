package fi.vm.yti.datamodel.api.v2.mapper;

import fi.vm.yti.datamodel.api.v2.dto.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDFS;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.topbraid.shacl.vocabulary.SH;

import java.net.URI;
import java.util.*;

public class VisualizationMapper {
    private static final Logger LOG = LoggerFactory.getLogger(VisualizationMapper.class);

    private VisualizationMapper() {
    }

    public static VisualizationClassDTO mapClass(String classURI, Model model, Map<String, String> namespaces) {
        var classResource = model.getResource(classURI);
        VisualizationClassDTO classDTO;

        if (MapperUtils.hasType(classResource, OWL.Class)) {
            classDTO = new VisualizationClassDTO();
        } else {
            var nodeShapeDTO = new VisualizationNodeShapeDTO();
            var targetClass = MapperUtils.propertyToString(classResource, SH.targetClass);
            nodeShapeDTO.setTargetClass(getReferenceIdentifier(targetClass, namespaces));
            classDTO = nodeShapeDTO;
        }

        mapCommon(classDTO, classResource, namespaces);

        classDTO.setParentClasses(getParentClasses(classResource, namespaces));

        return classDTO;
    }

    public static VisualizationClassDTO mapExternalClass(String identifier, Set<String> languages) {
        var dto = new VisualizationClassDTO();
        dto.setIdentifier(identifier);

        var label = new HashMap<String, String>();
        languages.forEach(key -> label.put(key, identifier));
        dto.setLabel(label);

        return dto;
    }

    public static void mapResource(VisualizationClassDTO dto, Resource resource, Model model, Map<String, String> namespaces) {
        if (MapperUtils.hasType(resource, OWL.DatatypeProperty)) {
            dto.addAttribute(mapAttribute(resource, namespaces));
        } else if (MapperUtils.hasType(resource, OWL.ObjectProperty)) {
            dto.addAssociation(mapAssociation(resource, model, namespaces));
        }
    }

    public static Model mapPositionDataToModel(String positionGraphURI, List<PositionDataDTO> positions) {
        var positionModel = ModelFactory.createDefaultModel();
        positions.forEach(node -> {
            var resource = positionModel.createResource(positionGraphURI + ModelConstants.RESOURCE_SEPARATOR + node.getIdentifier());
            resource.addLiteral(Iow.posX, node.getX());
            resource.addLiteral(Iow.posY, node.getY());
            resource.addProperty(DCTerms.identifier, node.getIdentifier());
            if (node.getReferenceTargets() != null) {
                node.getReferenceTargets().forEach(target -> resource.addProperty(Iow.referenceTarget, target));
            }
        });
        return positionModel;
    }

    public static Set<VisualizationHiddenNodeDTO> mapPositionsDataToDTOsAndCreateHiddenNodes(
            Model positions, String modelPrefix, Set<VisualizationClassDTO> classes) {
        var hiddenElements = new HashSet<VisualizationHiddenNodeDTO>();

        classes.forEach(dto -> {
            var positionResource = positions.getResource(ModelConstants.MODEL_POSITIONS_NAMESPACE + modelPrefix
                    + ModelConstants.RESOURCE_SEPARATOR
                    + dto.getIdentifier());
            dto.setPosition(getPositionFromResource(positionResource));

            // check if there are hidden nodes between parent relations
            if (dto.getParentClasses() != null && !dto.getParentClasses().isEmpty()) {
                var parentsWithTarget = new HashSet<String>();
                dto.getParentClasses().forEach(parent -> {
                    var path = handlePath(positions, dto.getIdentifier(), parent, hiddenElements);
                    parentsWithTarget.add(path.get(0));
                });
                dto.setParentClasses(parentsWithTarget);
            }

            // check if there are hidden nodes between association relations
            dto.getAssociations().forEach(association -> {
                var target = association.getReferenceTarget();
                if (target != null) {
                    var path = handlePath(positions, dto.getIdentifier(), target, hiddenElements);
                    association.setReferenceTarget(path.get(0));
                }
            });
        });
        return hiddenElements;
    }


    private static VisualizationAttributeDTO mapAttribute(Resource resource, Map<String, String> namespaces) {
        if (MapperUtils.hasType(resource, SH.PropertyShape)) {
            var dto = new VisualizationPropertyShapeAttributeDTO();
            mapCommon(dto, resource, namespaces);
            dto.setPath(getReferenceIdentifier(MapperUtils.propertyToString(resource, SH.path), namespaces));
            dto.setDataType(MapperUtils.propertyToString(resource, SH.datatype));
            dto.setMaxCount(MapperUtils.getLiteral(resource, SH.maxCount, Integer.class));
            dto.setMinCount(MapperUtils.getLiteral(resource, SH.minCount, Integer.class));
            dto.setCodeLists(MapperUtils.arrayPropertyToSet(resource, Iow.codeList));
            return dto;
        } else {
            var dto = new VisualizationAttributeDTO();
            mapCommon(dto, resource, namespaces);
            return dto;
        }
    }

    private static VisualizationAssociationDTO mapAssociation(Resource resource, Model model, Map<String, String> namespaces) {
        if (MapperUtils.hasType(resource, SH.PropertyShape)) {
            var dto = new VisualizationPropertyShapeAssociationDTO();
            mapCommon(dto, resource, namespaces);
            dto.setPath(getReferenceIdentifier(MapperUtils.propertyToString(resource, SH.path), namespaces));
            dto.setMaxCount(MapperUtils.getLiteral(resource, SH.maxCount, Integer.class));
            dto.setMinCount(MapperUtils.getLiteral(resource, SH.minCount, Integer.class));
            dto.setReferenceTarget(getPropertyShapeAssociationTarget(model, resource, namespaces));
            return dto;
        } else {
            var dto = new VisualizationAssociationDTO();
            var target = MapperUtils.propertyToString(resource, RDFS.range);
            mapCommon(dto, resource, namespaces);
            dto.setReferenceTarget(getReferenceIdentifier(target, namespaces));
            return dto;
        }
    }

    private static List<String> handlePath(Model positions, String sourceIdentifier, String target,
                                           Set<VisualizationHiddenNodeDTO> hiddenElements) {

        var positionResources = positions.listSubjectsWithProperty(Iow.referenceTarget, target).toList();

        for (Resource positionResource : positionResources) {
            var path = new LinkedList<String>();
            while (positionResource.hasProperty(Iow.referenceTarget)) {
                var identifier = MapperUtils.propertyToString(positionResource, DCTerms.identifier);

                if (identifier != null && !identifier.startsWith("corner-") && !identifier.equals(sourceIdentifier)) {
                    // if there is an element in the path, that is not a hidden node and not the same as source node,
                    // this is the wrong path. Jump to the next resource
                    break;
                } else if (sourceIdentifier.equals(identifier)) {
                    // reached the source node
                    if (path.isEmpty()) {
                        path.add(target);
                    }
                    return path;
                }

                path.addFirst(positionResource.getLocalName());
                hiddenElements.add(mapHiddenNode(positionResource));

                var references = positions.listSubjectsWithProperty(Iow.referenceTarget, positionResource.getLocalName()).toList();

                // no more references found, return path
                // should not happen as each hidden node should have references
                // and otherwise should have returned earlier
                if (references.isEmpty()) {
                    return path;
                }

                // assume, that there is only one reference for hidden nodes
                positionResource = references.get(0);
            }
        }
        // no hidden nodes between source and target
        return List.of(target);
    }

    private static VisualizationHiddenNodeDTO mapHiddenNode(Resource position) {
        var dto = new VisualizationHiddenNodeDTO();

        dto.setIdentifier(MapperUtils.propertyToString(position, DCTerms.identifier));
        dto.setPosition(getPositionFromResource(position));
        dto.setReferenceTarget(MapperUtils.propertyToString(position, Iow.referenceTarget));

        return dto;
    }

    private static PositionDTO getPositionFromResource(Resource positionResource) {
        var x = MapperUtils.getLiteral(positionResource, Iow.posX, Double.class);
        var y = MapperUtils.getLiteral(positionResource, Iow.posY, Double.class);
        return new PositionDTO(
                x != null ? x : 0.0,
                y != null ? y : 0.0
        );
    }

    private static void mapCommon(VisualizationItemDTO item, Resource resource, Map<String, String> namespaces) {
        item.setLabel(MapperUtils.localizedPropertyToMap(resource, RDFS.label));
        item.setIdentifier(getReferenceIdentifier(resource.getURI(), namespaces));
    }

    private static String getPropertyShapeAssociationTarget(Model model, Resource resource, Map<String, String> namespaces) {
        var target = MapperUtils.propertyToString(resource, SH.class_);
        if (target != null) {
            var iterator = model.listSubjectsWithProperty(SH.targetClass,
                    ResourceFactory.createResource(target));
            if (iterator.hasNext()) {
                return getReferenceIdentifier(iterator.next().getURI(), namespaces);
            } else {
                return getReferenceIdentifier(target, namespaces);
            }
        }
        return null;
    }

    @NotNull
    private static HashSet<String> getParentClasses(Resource resource, Map<String, String> namespaceMap) {
        var parentClasses = new HashSet<String>();

        if (MapperUtils.hasType(resource, OWL.Class)) {
            var subClassOf = resource.listProperties(RDFS.subClassOf).toList();
            subClassOf.forEach(parent -> {
                String classURI = parent.getObject().toString();
                // skip default parent class (owl#Thing)
                if ("http://www.w3.org/2002/07/owl#Thing".equals(classURI)) {
                    return;
                }
                parentClasses.add(getReferenceIdentifier(classURI, namespaceMap));
            });
        } else {
            var node = MapperUtils.propertyToString(resource, SH.node);
            if (node != null) {
                parentClasses.add(getReferenceIdentifier(node, namespaceMap));
            }
        }
        return parentClasses;
    }

    private static String getReferenceIdentifier(String uri, Map<String, String> namespaces) {
        try {
            var uriPath = URI.create(uri).getPath();
            var fragment = uriPath.substring(uriPath.lastIndexOf("/") + 1);
            String prefix = namespaces.get(uri.substring(0, uri.lastIndexOf("/")));

            if (prefix == null) {
                // referenced class in the same model
                return fragment;
            } else {
                // referenced class in the external namespace or other model in Interoperability platform
                return prefix + ":" + fragment;
            }
        } catch (Exception e) {
            LOG.warn("Invalid uri reference {}", uri);
            return null;
        }
    }
}
