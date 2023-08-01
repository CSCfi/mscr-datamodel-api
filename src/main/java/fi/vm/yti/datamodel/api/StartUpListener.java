package fi.vm.yti.datamodel.api;

import fi.vm.yti.datamodel.api.index.OpenSearchConnector;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.service.GroupManagementService;
import fi.vm.yti.datamodel.api.v2.service.NamespaceService;
import fi.vm.yti.migration.MigrationConfig;
import fi.vm.yti.migration.MigrationInitializer;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ImportAutoConfiguration(MigrationConfig.class)
public class StartUpListener {

    private static final Logger logger = LoggerFactory.getLogger(StartUpListener.class);

    private final GroupManagementService groupManagementService;
    private final OpenSearchConnector openSearchConnector;
    private final OpenSearchIndexer openSearchIndexer;
    private final CoreRepository coreRepository;
    private final NamespaceService namespaceService;

    @Autowired
    StartUpListener(GroupManagementService groupManagementService,
                    OpenSearchConnector openSearchConnector,
                    OpenSearchIndexer openSearchIndexer,
                    NamespaceService namespaceService,
                    MigrationInitializer migrationInitializer,
                    CoreRepository coreRepository) {
        this.groupManagementService = groupManagementService;
        this.openSearchConnector = openSearchConnector;
        this.openSearchIndexer = openSearchIndexer;
        this.namespaceService = namespaceService;
        this.coreRepository = coreRepository;
    }

    @PostConstruct
    public void contextInitialized() {
        logger.info("System is starting ...");

        initDefaultNamespaces();
        initOrganizations();
        initServiceCategories();
        initOpenSearchIndices();
        groupManagementService.updateUsers();
    }

    @Scheduled(cron = "0 */5 * * * *")
    void initOrganizations() {
        groupManagementService.initOrganizations();
    }

    private void initServiceCategories() {
        coreRepository.initServiceCategories();
    }

    private void initDefaultNamespaces() {
        namespaceService.resolveDefaultNamespaces();
    }

    private void initOpenSearchIndices() {
        try {
            openSearchConnector.waitForESNodes();
            openSearchIndexer.initIndexes();
        } catch (Exception e) {
            logger.warn("OpenSearch initialization failed!", e);
        }
    }
}
