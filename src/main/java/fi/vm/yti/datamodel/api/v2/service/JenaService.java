package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.arq.querybuilder.AskBuilder;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.vocabulary.OWL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
public class JenaService {


    private final Logger logger = LoggerFactory.getLogger(JenaService.class);

    
    private final RDFConnection schemaRead;
    private final RDFConnection schemaWrite;
    private final RDFConnection schemaSparql;

    private final RDFConnection crosswalkRead;
    private final RDFConnection crosswalkWrite;
    private final RDFConnection crosswalkSparql;


        
    private final String defaultNamespace;


    private final CoreRepository coreRepository;

    private static final String VERSION_NUMBER_GRAPH = "urn:yti:metamodel:version";    

    public JenaService(CoreRepository coreRepository, 
    					@Value("${model.cache.expiration:1800}") Long cacheExpireTime,
                       @Value(("${endpoint}")) String endpoint,
                       @Value("${defaultNamespace}") String defaultNamespace) {
    	
    	this.coreRepository = coreRepository;
    	this.defaultNamespace = defaultNamespace;
    	
        this.schemaWrite = RDFConnection.connect(endpoint + "/schema/data");
        this.schemaRead = RDFConnection.connect(endpoint + "/schema/get");
        this.schemaSparql = RDFConnection.connect(endpoint + "/schema/sparql");

        this.crosswalkWrite = RDFConnection.connect(endpoint + "/crosswalk/data");
        this.crosswalkRead = RDFConnection.connect(endpoint + "/crosswalk/get");
        this.crosswalkSparql = RDFConnection.connect(endpoint + "/crosswalk/sparql");


    }



    public Model constructWithQuerySchemas(Query query){
        try{
            return schemaSparql.queryConstruct(query);
        }catch(HttpException ex){
            return null;
        }
    }
    
    public Model constructWithQueryCrosswalks(Query query){
        try{
            return crosswalkSparql.queryConstruct(query);
        }catch(HttpException ex){
            return null;
        }
    }
    


    public int getVersionNumber() {
        var versionModel = coreRepository.fetch(VERSION_NUMBER_GRAPH);
        return versionModel.getResource(VERSION_NUMBER_GRAPH).getRequiredProperty(OWL.versionInfo).getInt();
    }

    public void setVersionNumber(int version) {
        var versionModel = ModelFactory.createDefaultModel().addLiteral(ResourceFactory.createResource(VERSION_NUMBER_GRAPH), OWL.versionInfo, version);
        versionModel.setNsPrefix("owl", "http://www.w3.org/2002/07/owl#");
        coreRepository.put(VERSION_NUMBER_GRAPH, versionModel);
    }

    public boolean isVersionGraphInitialized(){
        return coreRepository.graphExists(VERSION_NUMBER_GRAPH);
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
        } catch (org.apache.jena.atlas.web.HttpException ex) {
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

	public void putToCrosswalk(String graph, Model model) {
		crosswalkWrite.put(graph, model);
		
	}	  
	
	public Model getCrosswalk(String graph) {
        logger.debug("Getting crosswalk {}", graph);
        try {
            return crosswalkRead.fetch(graph);
        } catch (HttpException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
                logger.warn("Crosswalk not found with PID {}", graph);
                throw new ResourceNotFoundException(graph);
            } else {
                throw new JenaQueryException();
            }
        }
	}	
}
