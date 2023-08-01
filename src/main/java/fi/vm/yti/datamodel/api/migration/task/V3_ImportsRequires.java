package fi.vm.yti.datamodel.api.migration.task;

import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.migration.MigrationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class V3_ImportsRequires implements MigrationTask {

    private final Logger logger = LoggerFactory.getLogger(V3_ImportsRequires.class);

    private final CoreRepository coreRepository;

    public V3_ImportsRequires(CoreRepository coreRepository) {
        this.coreRepository = coreRepository;
    }

    @Override
    public void migrate() {
        logger.info("Migrating imports and requires");

        var terminologyQuery = """
                PREFIX iow:  <http://uri.suomi.fi/datamodel/ns/iow/>
                PREFIX dcterms: <http://purl.org/dc/terms/>
                
                DELETE {
                 GRAPH ?g {
                     ?sub dcterms:references ?terminology ;
                  }
                }INSERT {
                 GRAPH ?g {
                    ?sub dcterms:requires ?terminology ;
                  }
                } WHERE {
                  GRAPH ?g {
                    OPTIONAL { ?sub dcterms:references ?terminology .}
                  }
                }
                """;
        coreRepository.queryUpdate(terminologyQuery);

        var codelistQuery = """
                PREFIX iow:  <http://uri.suomi.fi/datamodel/ns/iow/>
                PREFIX dcterms: <http://purl.org/dc/terms/>
                
                DELETE {
                 GRAPH ?g {
                     ?sub iow:codeLists ?codelist .
                  }
                }INSERT {
                 GRAPH ?g {
                    ?sub dcterms:requires ?codelist .
                  }
                } WHERE {
                  GRAPH ?g {
                    OPTIONAL {?sub iow:codeLists ?codelist .}
                  }
                }
                """;
        coreRepository.queryUpdate(codelistQuery);

        //This one is adding core library to profile
        var profileCoreLibraryQuery = """
                PREFIX dcterms: <http://purl.org/dc/terms/>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX dcap: <http://purl.org/ws-mmi-dc/terms/>
                                
                DELETE {
                 GRAPH ?g {
                  ?sub owl:imports ?imports
                 }
                } INSERT {
                 GRAPH ?g {
                    ?sub dcterms:requires ?imports
                  }
                } WHERE {
                  { SELECT ?imports WHERE {
                     ?imports rdf:type owl:Ontology
                  }}
                  GRAPH ?g {
                    ?sub owl:imports ?imports .
                    ?sub rdf:type dcap:DCAP
                  }
                }
                """;
        coreRepository.queryUpdate(profileCoreLibraryQuery);

        var profileToProfile = """
                PREFIX dcap: <http://purl.org/ws-mmi-dc/terms/>
                PREFIX dcterms: <http://purl.org/dc/terms/>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                
                DELETE {
                 GRAPH ?g {
                     ?sub dcterms:requires ?requires
                  }
                }INSERT {
                 GRAPH ?g {
                    ?sub owl:imports ?requires
                  }
                } WHERE {
                    { SELECT ?requires WHERE {
                        ?requires rdf:type dcap:DCAP
                    }}
                    GRAPH ?g {
                    ?sub dcterms:requires ?requires .
                    ?sub rdf:type dcap:DCAP
                  }
                }
                """;

        coreRepository.queryUpdate(profileToProfile);

    }
}
