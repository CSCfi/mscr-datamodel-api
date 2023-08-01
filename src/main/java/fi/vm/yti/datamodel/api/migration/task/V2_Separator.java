package fi.vm.yti.datamodel.api.migration.task;

import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.migration.MigrationTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class V2_Separator implements MigrationTask {

    private static final Logger logger = LoggerFactory.getLogger(V2_Separator.class);

    private final CoreRepository coreRepository;
    public V2_Separator(CoreRepository coreRepository) {
        this.coreRepository = coreRepository;
    }

    @Override
    public void migrate() {
        logger.debug("Sending separator update");
        var query = """
            DELETE{
             GRAPH ?g {
              ?subject ?predicate ?object
             }
            }INSERT{
             GRAPH ?g {
              ?subject ?predicate ?replaced
             }
            }WHERE{
              GRAPH ?g {
               ?subject ?predicate ?object
               FILTER(REGEX(STR(?object), "http://uri.suomi.fi/datamodel/ns/[a-zA-Z0-9]+#", "i"))
               FILTER NOT EXISTS { ?subject <http://purl.org/ws-mmi-dc/terms/preferredXMLNamespaceName> ?object }
                BIND(IRI(REPLACE(str(?object), "#", "/", "i")) as ?replaced)
              }
            }
        """;
        coreRepository.queryUpdate(query);

        var xmlNamespaceUpdateQuery = """
            DELETE{
             GRAPH ?g {
              ?subject ?predicate ?object
             }
            }INSERT{
             GRAPH ?g {
              ?subject ?predicate ?replaced
             }
            }WHERE{
              GRAPH ?g {
               ?subject ?predicate ?object
               FILTER(REGEX(STR(?object), "http://uri.suomi.fi/datamodel/ns/[a-zA-Z0-9]+#", "i"))
               FILTER EXISTS { ?subject <http://purl.org/ws-mmi-dc/terms/preferredXMLNamespaceName> ?object }
               BIND(REPLACE(str(?object), "#", "/", "i") as ?replaced)
             }
            }
        """;
        coreRepository.queryUpdate(xmlNamespaceUpdateQuery);

        var updateSubjectQuery = """
            DELETE{
             GRAPH ?g {
              ?subject ?predicate ?object
             }
            }INSERT{
             GRAPH ?g {
              ?replaced ?predicate ?object
             }
            }WHERE{
              GRAPH ?g {
               ?subject ?predicate ?object
               FILTER(REGEX(STR(?subject), "http://uri.suomi.fi/datamodel/ns/[a-zA-Z0-9]+#", "i"))
                BIND(IRI(REPLACE(str(?subject), "#", "/", "i")) as ?replaced)
              }
            }
        """;
        coreRepository.queryUpdate(updateSubjectQuery);
    }
}
