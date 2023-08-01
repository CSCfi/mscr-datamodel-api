package fi.vm.yti.datamodel.api.migration.task;

import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.migration.MigrationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class V4_ApplicationProfileType implements MigrationTask {

    private static final Logger LOG = LoggerFactory.getLogger(V4_ApplicationProfileType.class);

    private final CoreRepository coreRepository;

    public V4_ApplicationProfileType(CoreRepository coreRepository) {
        this.coreRepository = coreRepository;
    }

    @Override
    public void migrate() {
        LOG.info("Migrating application profile types");

        // Inserts new types for application profile (owl:Ontology and iow:ApplicationProfile)
        // and removes the old one (http://purl.org/ws-mmi-dc/terms/DCAP)
        var query = """
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX iow: <http://uri.suomi.fi/datamodel/ns/iow/>
                
                DELETE {
                   GRAPH ?g {
                     ?s rdf:type <http://purl.org/ws-mmi-dc/terms/DCAP> .
                   }
                }
                INSERT {
                  GRAPH ?g {
                     ?s rdf:type owl:Ontology ;
                     rdf:type iow:ApplicationProfile .
                  }
                }
                WHERE {
                   GRAPH ?g {
                     ?s rdf:type <http://purl.org/ws-mmi-dc/terms/DCAP> .
                   }
                }""";

        coreRepository.queryUpdate(query);
    }
}
