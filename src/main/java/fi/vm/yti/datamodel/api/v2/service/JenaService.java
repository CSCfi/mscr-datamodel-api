package fi.vm.yti.datamodel.api.v2.service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import org.apache.jena.arq.querybuilder.*;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class JenaService {

    private final Logger logger = LoggerFactory.getLogger(JenaService.class);

    private final RDFConnection coreWrite;
    private final RDFConnection coreRead;
    private final RDFConnection coreSparql;
    private final RDFConnection coreUpdate;

    private final RDFConnection importWrite;
    private final RDFConnection importRead;
    private final RDFConnection importSparql;
    private final RDFConnection conceptRead;
    private final RDFConnection conceptWrite;
    private final RDFConnection conceptSparql;
    
    private final RDFConnection schemaRead;
    private final RDFConnection schemaWrite;
    private final RDFConnection schemaSparql;
       

    private final Cache<String, Model> modelCache;

    public JenaService(@Value("${model.cache.expiration:1800}") Long cacheExpireTime,
                       @Value(("${endpoint}")) String endpoint) {
        this.coreWrite = RDFConnection.connect(endpoint + "/core/data");
        this.coreRead = RDFConnection.connect(endpoint + "/core/get");
        this.coreSparql = RDFConnection.connect( endpoint + "/core/sparql");
        this.coreUpdate = RDFConnection.connect(endpoint + "/core/update");
        this.importWrite = RDFConnection.connect(endpoint + "/imports/data");
        this.importRead = RDFConnection.connect(endpoint + "/imports/get");
        this.importSparql = RDFConnection.connect(endpoint + "/imports/sparql");
        this.conceptWrite = RDFConnection.connect(endpoint + "/concept/data");
        this.conceptRead = RDFConnection.connect(endpoint + "/concept/get");
        this.conceptSparql = RDFConnection.connect(endpoint + "/concept/sparql");
   
        this.schemaWrite = RDFConnection.connect(endpoint + "/schema/data");
        this.schemaRead = RDFConnection.connect(endpoint + "/schema/get");
        this.schemaSparql = RDFConnection.connect(endpoint + "/schema/sparql");
   
        this.modelCache = CacheBuilder.newBuilder()
                .expireAfterWrite(cacheExpireTime, TimeUnit.SECONDS)
                .maximumSize(1000)
                .build();
    }

    public void putDataModelToCore(String graphName, Model model) {
        coreWrite.put(graphName, model);
    }

    public void initServiceCategories() {
        var model = RDFDataMgr.loadModel("ptvl-skos.rdf");
        coreWrite.put(ModelConstants.SERVICE_CATEGORY_GRAPH, model);
    }

    public void saveOrganizations(Model model) {
        coreWrite.put(ModelConstants.ORGANIZATION_GRAPH, model);
    }

    public Model getDataModel(String graph) {
        logger.debug("Getting model from core {}", graph);
        try {
            return coreRead.fetch(graph);
        } catch (HttpException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
                logger.warn("Model not found with prefix {}", graph);
                throw new ResourceNotFoundException(graph);
            } else {
                throw new JenaQueryException();
            }
        }
    }

    public void deleteDataModel(String graph) {
        logger.debug("Deleting model from core {}", graph);
        try{
            coreWrite.delete(graph);
        } catch (HttpException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
                logger.warn("Model not found with prefix {}", graph);
                throw new ResourceNotFoundException(graph);
            } else {
                throw new JenaQueryException();
            }
        }
    }

    public void deleteResource(String resource) {
        logger.debug("Deleting resource from core {}", resource);
        var deleteBuilder = new UpdateBuilder();
        var expr = deleteBuilder.getExprFactory();
        var filter = expr.or(expr.eq("?s", NodeFactory.createURI(resource)), expr.eq("?o", NodeFactory.createURI(resource)));
        deleteBuilder.addDelete("?g", "?s", "?p", "?o")
                .addGraph("?g", new WhereBuilder().addWhere("?s", "?p", "?o").addFilter(filter));
        try{
            coreUpdate.update(deleteBuilder.buildRequest());
        } catch (HttpException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
                logger.warn("Model not found with prefix {}", resource);
                throw new ResourceNotFoundException(resource);
            } else {
                throw new JenaQueryException();
            }
        }
    }

    public Model constructWithQuery(Query query){
        try{
            return coreSparql.queryConstruct(query);
        }catch(HttpException ex){
            return null;
        }
    }

    /**
     * Check if Data model exists, this method just asks if a given graph can be found.
     * @param graph Graph url of data model
     * @return exists
     */
    public boolean doesDataModelExist(String graph){
        var askBuilder = new AskBuilder()
                .addGraph(NodeFactory.createURI(graph), "?s", "?p", "?o");
        try {
            return coreSparql.queryAsk(askBuilder.build());
        }catch(HttpException ex){
            throw new JenaQueryException();
        }
    }

    /**
     * Check if given classUri (subject) found in graph
     * @param graph Graph
     * @param classUri Class URI
     * @return exists
     */
    public boolean doesResourceExistInGraph(String graph, String classUri){
        var askBuilder = new AskBuilder()
                .addGraph(NodeFactory.createURI(graph),
                        NodeFactory.createURI(classUri), "?p", "?o");
        try{
            return coreSparql.queryAsk(askBuilder.build());
        }catch(HttpException ex){
            throw new JenaQueryException();
        }
    }

     /**
     * Check if resource uri is one of given types
     * @param resourceUri Resource uri
     * @param types List of types to check
     * @param checkImports Should imports be checked instead of core
     * @return true if resource is one of types
     */
    public boolean checkIfResourceIsOneOfTypes(String resourceUri, List<Resource> types, boolean checkImports) {
        var askBuilder  =new AskBuilder()
                .addWhere(NodeFactory.createURI(resourceUri), RDF.type, "?type")
                .addValueVar("?type", types.toArray());
        try{
            if(checkImports){
                return importSparql.queryAsk(askBuilder.build());
            }else {
                return coreSparql.queryAsk(askBuilder.build());
            }
        }catch(HttpException ex){
            throw new JenaQueryException();
        }
    }

    public Model getServiceCategories(){
        var serviceCategories = modelCache.getIfPresent("serviceCategories");

        if(serviceCategories != null){
            return serviceCategories;
        }

        var cat = "?category";
        ConstructBuilder builder = new ConstructBuilder()
                .addPrefixes(ModelConstants.PREFIXES)
                .addConstruct(cat, RDFS.label, "?label")
                .addConstruct(cat, RDF.type, FOAF.Group)
                .addConstruct(cat, SKOS.notation, "?id")
                .addConstruct(cat, SKOS.note, "?note")
                .addWhere(cat, RDF.type, SKOS.Concept)
                .addWhere(cat, SKOS.prefLabel, "?label")
                .addWhere(cat, SKOS.notation, "?id")
                .addWhere(cat, SKOS.note, "?note")
                .addFilter(new ExprFactory().notexists(
                        new WhereBuilder().addWhere(cat, SKOS.broader, "?topCategory")
                ));

        serviceCategories = constructWithQuery(builder.build());

        modelCache.put("serviceCategories", serviceCategories);
        return serviceCategories;
    }

    public Model getOrganizations(){
        var organizations = modelCache.getIfPresent("organizations");

        if(organizations != null){
            return organizations;
        }

        organizations = getDataModel("urn:yti:organizations");

        modelCache.put("organizations", organizations);
        return organizations;
    }

    public void putNamespaceToImports(String graphName, Model model){
        importWrite.put(graphName, model);
    }

    public Model getNamespaceFromImports(String graphName){
        logger.debug("Getting model from core {}", graphName);
        try {
            return importRead.fetch(graphName);
        } catch (HttpException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
                logger.warn("Namespace not found: {}", graphName);
                throw new ResourceNotFoundException(graphName);
            } else {
                throw new JenaQueryException();
            }
        }
    }

    public boolean doesResolvedNamespaceExist(String namespace){
        var askBuilder = new AskBuilder()
                .addGraph(NodeFactory.createURI(namespace), "?s", "?p", "?o");
        try {
            return importSparql.queryAsk(askBuilder.build());
        }catch(HttpException ex){
            throw new JenaQueryException();
        }
    }

    public boolean doesResourceExistInImportedNamespace(String namespace, String resourceUri){
        var askBuilder = new AskBuilder()
                .addGraph(NodeFactory.createURI(namespace),
                        NodeFactory.createURI(resourceUri), "?p", "?o");
        try{
            return importSparql.queryAsk(askBuilder.build());
        }catch(HttpException ex){
            throw new JenaQueryException();
        }
    }

    public void putTerminologyToConcepts(String graph, Model model) {
        conceptWrite.put(graph, model);
    }

    public Model getTerminology(String graph) {
        try {
            return modelCache.get(graph, () -> {
                logger.debug("Fetch terminology from Fuseki {}", graph);
                return conceptRead.fetch(graph);
            });
        } catch (Exception e) {
            logger.warn("Error fetching terminology information {}", e.getMessage());
            return null;
        }
    }

    public Model getConcept(String conceptURI) {
        ConstructBuilder builder = new ConstructBuilder();
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

        return conceptSparql.queryConstruct(builder.build());
    }

    public int getVersionNumber() {
        // TODO: migrations
        return 1;
    }

    public void setVersionNumber(int version) {
        // TODO: migrations
    }
    
    public void putToSchema(String graphName, Model model) {
        schemaWrite.put(graphName, model);
    }
    
    public void updateSchema(String graphName, Model model) {
    	schemaWrite.delete(graphName);
    	schemaWrite.put(graphName, model);
    }

	public Model getSchema(String graph) {
        logger.debug("Getting schema {}", graph);
        try {
            return schemaRead.fetch(graph);
        } catch (HttpException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
                logger.warn("Schema not found with PID {}", graph);
                throw new ResourceNotFoundException(graph);
            } else {
                throw new JenaQueryException();
            }
        }
	}
	
    /**
     * Check if Data model exists, this method just asks if a given graph can be found.
     * @param graph Graph url of data model
     * @return exists
     */
    public boolean doesSchemaExist(String graph){
        var askBuilder = new AskBuilder()
                .addGraph(NodeFactory.createURI(graph), "?s", "?p", "?o");
        try {
            return schemaSparql.queryAsk(askBuilder.build());
        }catch(HttpException ex){
            throw new JenaQueryException();
        }
    }	    
}
