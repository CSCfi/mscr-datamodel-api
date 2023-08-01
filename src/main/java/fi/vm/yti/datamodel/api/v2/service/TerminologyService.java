package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.v2.dto.ConceptDTO;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.ResourceInfoBaseDTO;
import fi.vm.yti.datamodel.api.v2.dto.TerminologyNodeDTO;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResolvingException;
import fi.vm.yti.datamodel.api.v2.mapper.TerminologyMapper;
import fi.vm.yti.datamodel.api.v2.repository.ConceptRepository;
import fi.vm.yti.datamodel.api.v2.utils.SparqlUtils;
import org.apache.jena.arq.querybuilder.ConstructBuilder;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

@Service
public class TerminologyService {
    private static final Logger LOG = LoggerFactory.getLogger(TerminologyService.class);

    /**
     * Control which environment is used for resolving terminology uris.
     * Possible values: awsdev, awstest and awslocal. Resolve from prod if empty
     */
    @Value("${env:}")
    private String awsEnv;
    private final WebClient client;
    private final ConceptRepository conceptRepository;

    public TerminologyService(
            @Qualifier("uriResolveClient") WebClient webClient,
            ConceptRepository conceptRepository) {
        this.conceptRepository = conceptRepository;
        this.client = webClient;
    }

    /**
     * Fetch terminology information and persist to Fuseki
     * @param terminologyUris set of uris
     */
    public void resolveTerminology(Set<String> terminologyUris) {

        for (String u : terminologyUris) {
            var uri = URI.create(u);
            LOG.debug("Fetching terminology {}", uri);
            try {
                var result = client.get().uri(uriBuilder -> uriBuilder
                                .path(uri.getPath())
                                .queryParam("env", awsEnv)
                                .build()
                        )
                        .accept(MediaType.APPLICATION_JSON)
                        .retrieve()
                        .bodyToMono(new ParameterizedTypeReference<List<TerminologyNodeDTO>>() {
                        })
                        .block();

                if (result == null) {
                    continue;
                }
                var node = result.stream()
                        .filter(n -> n.getType().getId().equals("TerminologicalVocabulary"))
                        .findFirst();

                if (node.isPresent()) {
                    var model = TerminologyMapper.mapTerminologyToJenaModel(u, node.get());
                    conceptRepository.put(uri.toString(), model);
                } else {
                    LOG.warn("Could not find node with type TerminologicalVocabulary from {}", uri);
                }
            } catch (Exception e) {
                LOG.warn("Could not resolve terminology {}, {}", uri, e.getMessage());
            }
        }
    }

    public void resolveConcept(String conceptURI) {
        if (conceptURI == null || conceptURI.isBlank()) {
            return;
        }

        var uri = URI.create(conceptURI);
        LOG.debug("Fetching concept {}", uri);

        List<TerminologyNodeDTO> result;
        try {
            result = client.get().uri(uriBuilder -> uriBuilder
                            .path(uri.getPath())
                            .queryParam("env", awsEnv)
                            .build()
                    )
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .bodyToMono(new ParameterizedTypeReference<List<TerminologyNodeDTO>>() {
                    })
                    .block();
        } catch(Exception e) {
            LOG.warn("Could not resolve concept uri {}", uri);
            throw new ResolvingException("Concept not found", String.format("Concept %s not found", conceptURI));
        }

        var terminologyURI = conceptURI.substring(0, conceptURI.lastIndexOf("/"));
        var terminologyModel = conceptRepository.fetch(terminologyURI);
        if (terminologyModel == null) {
            LOG.warn("Terminology {} not added to model", terminologyURI);
            throw new ResolvingException("Terminology not added to the model",
                    String.format("Add %s to the model", terminologyURI));
        }

        TerminologyMapper.mapConceptToTerminologyModel(terminologyModel, terminologyURI,
                conceptURI, result.get(0));

        conceptRepository.put(terminologyURI, terminologyModel);
    }

    public Consumer<ResourceInfoBaseDTO> mapConcept() {
        return (var dto) -> dto.setSubject(getMappedConceptDTO(dto.getSubject()));
    }

    private ConceptDTO getMappedConceptDTO(ConceptDTO dto) {
        if (dto == null || dto.getConceptURI() == null || dto.getConceptURI().isEmpty()) {
            return null;
        }
        var conceptModel = getConcept(dto.getConceptURI());
        return TerminologyMapper.mapToConceptDTO(conceptModel, dto.getConceptURI());
    }

    public Model getConcept(String conceptURI) {
        var builder = new ConstructBuilder();
        var res = ResourceFactory.createResource(conceptURI);

        var label = "?label";
        var inScheme = "?inScheme";
        var definition = "?definition";
        var terminologyLabel = "?terminologyLabel";
        var status = "?status";

        builder.addPrefixes(ModelConstants.PREFIXES)
                .addConstruct(res, SKOS.prefLabel, label)
                .addConstruct(res, SKOS.inScheme, inScheme)
                .addConstruct(res, SKOS.definition, definition)
                .addConstruct(res, OWL.versionInfo, status)
                .addConstruct(res, RDFS.label, terminologyLabel)
                .addWhere(res, SKOS.prefLabel, label)
                .addWhere(res, SKOS.inScheme, inScheme)
                .addOptional(res, SKOS.definition, definition)
                .addWhere(res, OWL.versionInfo, status)
                .addWhere(inScheme, RDFS.label, terminologyLabel);

        return conceptRepository.queryConstruct(builder.build());
    }

    public Model getAllConcepts() {
        var builder = new ConstructBuilder().addPrefixes(ModelConstants.PREFIXES);
        SparqlUtils.addConstructProperty("?concept", builder, SKOS.prefLabel, "?label");
        SparqlUtils.addConstructProperty("?concept", builder, SKOS.inScheme, "?terminology");
        SparqlUtils.addConstructProperty("?terminology", builder, RDFS.label, "?terminologyLabel");
        return conceptRepository.queryConstruct(builder.build());
    }

}
