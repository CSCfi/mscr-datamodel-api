package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.v2.mapper.ResourceMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.repository.ImportsRepository;
import org.apache.jena.arq.querybuilder.AskBuilder;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RiotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
public class NamespaceResolver {

    private final Logger logger = LoggerFactory.getLogger(NamespaceResolver.class);


    private final ImportsRepository importsRepository;
    private final OpenSearchIndexer indexer;
    private static final List<String> ACCEPT_TYPES = List.of("application/rdf+xml;q=1.0", "text/turtle", "application/n-triples", "application/ld+json", "text/trig", "application/n-quads", "application/trix+xml", "application/rdf+thrift", "application/rdf+protobuf");

    public NamespaceResolver(ImportsRepository importsRepository,
                             OpenSearchIndexer indexer) {
        this.importsRepository = importsRepository;
        this.indexer = indexer;
    }

    /**
     * Resolved namespace
     * @param namespace Namespace uri
     * @return true if resolved
     */
    public boolean resolveNamespace(String namespace){
        logger.info("Resolving namespace: {}", namespace);
        var model = ModelFactory.createDefaultModel();
        try{
            RDFParser.create()
                    .source(namespace)
                    .lang(Lang.RDFXML)
                    .acceptHeader(String.join(", ", ACCEPT_TYPES))
                    .parse(model);
            if(model.size() > 0){
                importsRepository.put(namespace, model);
                var indexResources = model.listSubjects()
                        .mapWith(resource -> ResourceMapper.mapExternalToIndexResource(model, resource))
                        .toList()
                        .stream().filter(Objects::nonNull)
                        .toList();
                logger.info("Namespace {} resolved, add {} items to index", namespace, indexResources.size());
                indexer.bulkInsert(OpenSearchIndexer.OPEN_SEARCH_INDEX_EXTERNAL, indexResources);

                return true;
            }
        } catch (RiotException ex){
            logger.warn("Namespace: {}, not resolvable: {}", namespace, ex.getMessage());
            return false;
        } catch (HttpException ex){
            logger.warn("Namespace not resolvable due to HTTP error, Status code: {}", ex.getStatusCode());
            return false;
        }
        return false;
    }

    public boolean namespaceAlreadyResolved(String namespace){
        var askBuilder = new AskBuilder()
                .addGraph(NodeFactory.createURI(namespace), "?s", "?p", "?o");
        return importsRepository.queryAsk(askBuilder.build());
    }
}
