package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.*;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import fi.vm.yti.datamodel.api.v2.mapper.VisualizationMapper;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.springframework.stereotype.Service;
import org.topbraid.shacl.vocabulary.SH;

import java.util.*;

import static fi.vm.yti.security.AuthorizationException.check;

@Service
public class VisualizationService {


    private final ResourceService resourceService;
    private final CoreRepository coreRepository;
    private final AuthorizationManager authorizationManager;
    public VisualizationService(ResourceService resourceService,
                                CoreRepository coreRepository,
                                AuthorizationManager authorizationManager) {
        this.resourceService = resourceService;
        this.coreRepository = coreRepository;
        this.authorizationManager = authorizationManager;
    }

    public VisualizationResultDTO getVisualizationData(String prefix) {
        var graph = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        var model = coreRepository.fetch(graph);
        Model positions;
        try {
            positions = coreRepository.fetch(ModelConstants.MODEL_POSITIONS_NAMESPACE + prefix);
        } catch(ResourceNotFoundException e) {
            positions = ModelFactory.createDefaultModel();
        }

        var modelResource = model.getResource(graph);
        var classURIs = getClassURIs(model, graph);
        var namespaces = getNamespaces(model, graph);
        var languages = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.language);

        var visualizationClasses = new HashSet<VisualizationClassDTO>();

        while (classURIs.hasNext()) {
            var classURI = classURIs.next().getSubject().getURI();
            var classResource = model.getResource(classURI);

            var classDTO = VisualizationMapper.mapClass(classURI, model, namespaces);

            var attributesAndAssociations = getAttributesAndAssociations(model, classResource, modelResource);
            var externalPropertyURIs = new HashSet<String>();
            for (var resource : attributesAndAssociations) {
                if (!model.contains(resource, null)) {
                    externalPropertyURIs.add(resource.getURI());
                } else {
                    VisualizationMapper.mapResource(classDTO, resource, model, namespaces);
                }
            }

            if (!externalPropertyURIs.isEmpty()) {
                var externalResources = resourceService.findResources(externalPropertyURIs);
                externalPropertyURIs.forEach(uri -> VisualizationMapper
                        .mapResource(classDTO, externalResources.getResource(uri), model, namespaces));
            }

            // add dummy classes for external classes
            addExternalClasses(classDTO, languages, visualizationClasses);

            visualizationClasses.add(classDTO);
        }
        var hiddenNodes = VisualizationMapper.mapPositionsDataToDTOsAndCreateHiddenNodes(positions, prefix, visualizationClasses);

        var visualizationResult = new VisualizationResultDTO();
        visualizationResult.setNodes(visualizationClasses);
        visualizationResult.setHiddenNodes(hiddenNodes);

        return visualizationResult;
    }

    public void savePositionData(String prefix, List<PositionDataDTO> positions) {
        var modelURI = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        var dataModel = coreRepository.fetch(modelURI);
        check(authorizationManager.hasRightToModel(prefix, dataModel));

        var positionGraphURI = ModelConstants.MODEL_POSITIONS_NAMESPACE + prefix;

        // remove old positions if exists
        if (coreRepository.graphExists(positionGraphURI)) {
            coreRepository.delete(positionGraphURI);
        }

        var positionModel = VisualizationMapper.mapPositionDataToModel(positionGraphURI, positions);
        coreRepository.put(positionGraphURI, positionModel);
    }

    private static void addExternalClasses(VisualizationClassDTO classDTO, Set<String> languages, HashSet<VisualizationClassDTO> result) {
        classDTO.getParentClasses().forEach(parent -> {
            if (parent.contains(":")) {
                result.add(VisualizationMapper.mapExternalClass(parent, languages));
            }
        });
        classDTO.getAssociations().forEach(association -> {
            var target = association.getReferenceTarget();
            if (target != null && target.contains(":")) {
                result.add(VisualizationMapper.mapExternalClass(target, languages));
            }
        });
    }

    private static StmtIterator getClassURIs(Model model, String graph) {
        var classType = MapperUtils.isLibrary(model.getResource(graph))
                ? OWL.Class
                : SH.NodeShape;
        return model.listStatements(
                new SimpleSelector(null, RDF.type, classType));
    }

    /**
     * Map external and internal namespaces and prefixes
     *
     * @param model model
     * @param graph graph
     */
    private static HashMap<String, String> getNamespaces(Model model, String graph) {
        var namespaces = new HashMap<String, String>();
        var modelResource = model.getResource(graph);
        Arrays.asList(OWL.imports, DCTerms.requires)
                .forEach(prop -> modelResource.listProperties(prop).toList().forEach(ns -> {
                    var uri = ns.getObject().toString();
                    if (uri.startsWith(ModelConstants.SUOMI_FI_NAMESPACE)) {
                        namespaces.put(uri, uri.replace(ModelConstants.SUOMI_FI_NAMESPACE, ""));
                    } else if (!uri.startsWith(ModelConstants.TERMINOLOGY_NAMESPACE)
                            && !uri.startsWith(ModelConstants.CODELIST_NAMESPACE)) {
                        namespaces.put(uri, model.getResource(uri).getProperty(DCAP.preferredXMLNamespacePrefix).getString());
                    }
                }));
        return namespaces;
    }

    private static List<Resource> getAttributesAndAssociations(
            Model model, Resource classResource, Resource modelResource) {
        if (MapperUtils.isLibrary(modelResource)) {
            return model.listSubjectsWithProperty(RDFS.domain, classResource).toList();
        } else if (MapperUtils.isApplicationProfile(modelResource)) {
            return classResource.listProperties(SH.property)
                    .mapWith(Statement::getResource)
                    .toList();
        }
        return List.of();
    }
}
