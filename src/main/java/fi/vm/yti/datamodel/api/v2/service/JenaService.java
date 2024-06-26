package fi.vm.yti.datamodel.api.v2.service;

import org.apache.jena.arq.querybuilder.AskBuilder;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.vocabulary.OWL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;

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
    
    public void updateCrosswalk(String graphName, Model model) {
    	crosswalkWrite.delete(graphName);
    	crosswalkWrite.put(graphName, model);
    	
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
    
    public boolean doesCrosswalkExist(String graph){
        var askBuilder = new AskBuilder()
                .addGraph(NodeFactory.createURI(graph), "?s", "?p", "?o");
        try {
            return crosswalkSparql.queryAsk(askBuilder.build());
        }catch(HttpException ex){
            throw new JenaQueryException();
        }
    }
    

	public void putToCrosswalk(String graph, Model model) {
		crosswalkWrite.put(graph, model);
		
	}	  
	
	public void deleteFromCrosswalk(String graph) {
		crosswalkWrite.delete(graph);
	}
	
	public void deleteFromSchema(String graph) {
		schemaWrite.delete(graph);
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


	public Model getSchemaContent(String pid) {
		final String graph = pid+":content";
        try {
            return schemaRead.fetch(graph);
        } catch (org.apache.jena.atlas.web.HttpException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
                logger.warn("Content not found for schemas  with PID {}", pid);
                throw new ResourceNotFoundException(graph);
            } else {
                throw new JenaQueryException();
            }
        }
	}



	public Model getCrosswalkContent(String pid) {
		final String graph = pid+":content";
        try {
            return crosswalkRead.fetch(graph);
        } catch (org.apache.jena.atlas.web.HttpException ex) {
            if (ex.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
                logger.warn("Content not found for crosswalk  with PID {}", pid);
                throw new ResourceNotFoundException(graph);
            } else {
                throw new JenaQueryException();
            }
        }
	}

	public void deleteMapping(String crosswalkPID, String mappingPID) {
		// TODO Use an update instead of loading the whole graph
		Model m = getCrosswalk(crosswalkPID+":content");
		deleteMapping(crosswalkPID, mappingPID, m, true);
		putToCrosswalk(crosswalkPID+":content", m);
	}
	
	
	public void deleteMapping(String crosswalkPID, String mappingPID, Model m, boolean removeRef) {
		Resource mappingResource = m.createResource(mappingPID);
		// first remove the source and target sequences
		Seq s = mappingResource.getRequiredProperty(MSCR.source).getSeq();				
		for(int i = s.size(); i >= 1; i--) {
			if(s.getResource(i).hasProperty(MSCR.processing)) {
				Resource pros = s.getResource(i).getPropertyResourceValue(MSCR.processing);
				if(pros.hasProperty(MSCR.processingParams)) {
					Bag b = pros.getRequiredProperty(MSCR.processingParams).getBag();
					NodeIterator ii = b.iterator();
					while(ii.hasNext()) {
						ii.next().asResource().removeProperties();
					}
					b.removeProperties();
				}
				
				pros.removeProperties();
			}
			s.getResource(i).removeProperties();
			s.remove(i);
		}				
		s = mappingResource.getRequiredProperty(MSCR.target).getSeq();				
		for(int i = s.size(); i >= 1; i--) {
			if(s.getResource(i).hasProperty(MSCR.processing)) {
				Resource pros = s.getResource(i).getPropertyResourceValue(MSCR.processing);
				if(pros.hasProperty(MSCR.processingParams)) {
					Bag b = pros.getRequiredProperty(MSCR.processingParams).getBag();
					NodeIterator ii = b.iterator();
					while(ii.hasNext()) {
						ii.next().asResource().removeProperties();
					}
					b.removeProperties();
				}
				
				pros.removeProperties();
			}
			s.getResource(i).removeProperties();
			s.remove(i);
		}	
				
		Resource c = mappingResource.getPropertyResourceValue(MSCR.source);
		m.removeAll(c, null, null);
		Resource c2 = mappingResource.getPropertyResourceValue(MSCR.target);
		m.removeAll(c2, null, null);
		
		if(mappingResource.hasProperty(MSCR.processing)) {
			Resource pros = mappingResource.getPropertyResourceValue(MSCR.processing);
			if(pros.hasProperty(MSCR.processingParams)) {
				Bag b = pros.getRequiredProperty(MSCR.processingParams).getBag();
				NodeIterator ii = b.iterator();
				while(ii.hasNext()) {
					ii.next().asResource().removeProperties();
				}
				b.removeProperties();
			}
			
			pros.removeProperties();
		}		
		m.removeAll(mappingResource, null, null);
		if(removeRef) {
			m.removeAll(m.createResource(crosswalkPID), MSCR.mappings, mappingResource);	
		}
		
		
		
	}	
}
