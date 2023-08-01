package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.DataModelDTO;
import fi.vm.yti.datamodel.api.v2.dto.DataModelInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.ModelType;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.ModelMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.utils.DataModelUtils;
import fi.vm.yti.datamodel.api.v2.validator.ValidationConstants;
import fi.vm.yti.security.AuthenticatedUserProvider;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.SimpleSelector;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.SKOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;

import static fi.vm.yti.security.AuthorizationException.check;

@Service
public class DataModelService {

    private final Logger logger = LoggerFactory.getLogger(DataModelService.class);

    private final CoreRepository coreRepository;
    private final AuthorizationManager authorizationManager;
    private final GroupManagementService groupManagementService;
    private final ModelMapper mapper;
    private final TerminologyService terminologyService;
    private final CodeListService codeListService;
    private final OpenSearchIndexer openSearchIndexer;
    private final AuthenticatedUserProvider userProvider;

    @Autowired
    public DataModelService(CoreRepository coreRepository,
                            AuthorizationManager authorizationManager,
                            GroupManagementService groupManagementService,
                            ModelMapper modelMapper,
                            TerminologyService terminologyService,
                            CodeListService codeListService,
                            OpenSearchIndexer openSearchIndexer,
                            AuthenticatedUserProvider userProvider) {
        this.coreRepository = coreRepository;
        this.authorizationManager = authorizationManager;
        this.groupManagementService = groupManagementService;
        this.mapper = modelMapper;
        this.terminologyService = terminologyService;
        this.codeListService = codeListService;
        this.openSearchIndexer = openSearchIndexer;
        this.userProvider = userProvider;
    }

    public DataModelInfoDTO get(String prefix) {
        var model = coreRepository.fetch(ModelConstants.SUOMI_FI_NAMESPACE + prefix);
        var hasRightsToModel = authorizationManager.hasRightToModel(prefix, model);

        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;
        return mapper.mapToDataModelDTO(prefix, model, userMapper);
    }

    public URI create(DataModelDTO dto, ModelType modelType) throws URISyntaxException {
        check(authorizationManager.hasRightToAnyOrganization(dto.getOrganizations()));
        var graphUri = ModelConstants.SUOMI_FI_NAMESPACE + dto.getPrefix();

        terminologyService.resolveTerminology(dto.getTerminologies());
        codeListService.resolveCodelistScheme(dto.getCodeLists());
        var jenaModel = mapper.mapToJenaModel(dto, modelType, userProvider.getUser());

        coreRepository.put(graphUri, jenaModel);

        var indexModel = mapper.mapToIndexModel(dto.getPrefix(), jenaModel);
        openSearchIndexer.createModelToIndex(indexModel);
        return new URI(graphUri);
    }

    public void update(String prefix, DataModelDTO dto) {
        var graphUri = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        var oldModel = coreRepository.fetch(graphUri);

        check(authorizationManager.hasRightToModel(prefix, oldModel));

        terminologyService.resolveTerminology(dto.getTerminologies());
        codeListService.resolveCodelistScheme(dto.getCodeLists());
        var jenaModel = mapper.mapToUpdateJenaModel(prefix, dto, oldModel, userProvider.getUser());

        coreRepository.put(graphUri, jenaModel);

        var indexModel = mapper.mapToIndexModel(prefix, jenaModel);
        openSearchIndexer.updateModelToIndex(indexModel);
    }

    public void delete(String prefix) {
        var modelUri = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        if(!coreRepository.graphExists(modelUri)){
            throw new ResourceNotFoundException(modelUri);
        }
        var model = coreRepository.fetch(modelUri);
        check(authorizationManager.hasRightToModel(prefix, model));

        coreRepository.delete(modelUri);
        openSearchIndexer.deleteModelFromIndex(modelUri);
    }

    public boolean exists(String prefix) {
        if (ValidationConstants.RESERVED_WORDS.contains(prefix)) {
            return true;
        }
        return coreRepository.graphExists(ModelConstants.SUOMI_FI_NAMESPACE + prefix);
    }

    public ResponseEntity<String> export(String prefix, String resource, String accept) {
        var modelURI = ModelConstants.SUOMI_FI_NAMESPACE + prefix;
        logger.info("Exporting datamodel {}, {}", modelURI, accept);

        Model exportedModel;
        Model model;

        try {
            model = coreRepository.fetch(modelURI);
        } catch (ResourceNotFoundException e) {
            // cannot throw ResourceNotFoundException because accept header is not application/json
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            logger.error("Error exporting datamodel", e);
            return ResponseEntity.internalServerError().build();
        }

        if (resource != null) {
            var res = model.getResource(modelURI + ModelConstants.RESOURCE_SEPARATOR + resource);
            var properties = res.listProperties();
            if (!properties.hasNext()) {
                return ResponseEntity.notFound().build();
            }
            exportedModel = properties.toModel();
        } else {
            exportedModel = model;
        }

        DataModelUtils.addPrefixesToModel(modelURI, exportedModel);

        // remove editorial notes from resources
        if (!authorizationManager.hasRightToModel(prefix, model)) {
            var hiddenValues = model.listStatements(
                    new SimpleSelector(null, SKOS.editorialNote, (String) null)).toList();
            exportedModel.remove(hiddenValues);
        }

        var stringWriter = new StringWriter();
        switch (accept) {
            case "text/turtle":
                RDFDataMgr.write(stringWriter, exportedModel, Lang.TURTLE);
                break;
            case "application/rdf+xml":
                RDFDataMgr.write(stringWriter, exportedModel, Lang.RDFXML);
                break;
            case "application/ld+json":
            default:
                RDFDataMgr.write(stringWriter, exportedModel, Lang.JSONLD);
        }
        return ResponseEntity.ok(stringWriter.toString());
    }

}
