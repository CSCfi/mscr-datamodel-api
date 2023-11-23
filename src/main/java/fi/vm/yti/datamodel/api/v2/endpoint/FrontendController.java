package fi.vm.yti.datamodel.api.v2.endpoint;

import fi.vm.yti.datamodel.api.v2.dto.CrosswalkEditorSchemaDTO;
import fi.vm.yti.datamodel.api.v2.dto.FunctionDTO;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.OrganizationDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.ServiceCategoryDTO;
import fi.vm.yti.datamodel.api.v2.mapper.SchemaMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.OpenSearchUtil;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.*;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexResource;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexCrosswalk;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexModel;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexResourceInfo;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexSchema;
import fi.vm.yti.datamodel.api.v2.service.FrontendService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.JsonSchemaWriter;
import fi.vm.yti.datamodel.api.v2.service.NamespaceService;
import fi.vm.yti.datamodel.api.v2.service.SearchIndexService;
import fi.vm.yti.security.AuthenticatedUserProvider;
import fi.vm.yti.security.YtiUser;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;

import org.apache.jena.rdf.model.Model;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping("v2/frontend")
@Tag(name = "Frontend")
public class FrontendController {

    private static final Logger logger = LoggerFactory.getLogger(FrontendController.class);
    private final SearchIndexService searchIndexService;
    private final FrontendService frontendService;
    private final AuthenticatedUserProvider userProvider;
    private final NamespaceService namespaceService;
    
    private final JenaService jenaService;
    private final SchemaMapper schemaMapper;
    private final JsonSchemaWriter schemaWriter;

    @Autowired
    public FrontendController(SearchIndexService searchIndexService,
                              FrontendService frontendService,
                              AuthenticatedUserProvider userProvider,
                              NamespaceService namespaceService,
                              JenaService jenaService,
                              SchemaMapper schemaMapper,
                      		  JsonSchemaWriter schemaWriter) {
        this.searchIndexService = searchIndexService;
        this.frontendService = frontendService;
        this.userProvider = userProvider;
        this.namespaceService = namespaceService;
        
        this.jenaService = jenaService;
        this.schemaMapper = schemaMapper;
        this.schemaWriter = schemaWriter;        
    }

    @Operation(summary = "Get counts", description = "List counts of data model grouped by different search results")
    @ApiResponse(responseCode = "200", description = "Counts response container object as JSON")
    @GetMapping(path = "/counts", produces = MediaType.APPLICATION_JSON_VALUE)
    public CountSearchResponse getCounts(CountRequest request) {
        logger.info("GET /counts requested");
        return searchIndexService.getCounts(request);
    }

    @Operation(summary = "Get organizations", description = "List of organizations sorted by name")
    @ApiResponse(responseCode = "200", description = "Organization list as JSON")
    @GetMapping(path = "/organizations", produces = MediaType.APPLICATION_JSON_VALUE)
    public Collection<OrganizationDTO> getOrganizations(
            @RequestParam(value = "sortLang", required = false, defaultValue = ModelConstants.DEFAULT_LANGUAGE) String sortLang,
            @RequestParam(value = "includeChildOrganizations", required = false) boolean includeChildOrganizations) {
        logger.info("GET /organizations requested");
        return frontendService.getOrganizations(sortLang, includeChildOrganizations);
    }

    @Operation(summary = "Get service categories", description = "List of service categories sorted by name")
    @ApiResponse(responseCode = "200", description = "Service categories as JSON")
    @GetMapping(path = "/service-categories", produces = MediaType.APPLICATION_JSON_VALUE)
    public Collection<ServiceCategoryDTO> getServiceCategories(@RequestParam(value = "sortLang", required = false, defaultValue = ModelConstants.DEFAULT_LANGUAGE) String sortLang) {
        logger.info("GET /serviceCategories requested");
        return frontendService.getServiceCategories(sortLang);
    }

    @Operation(summary = "Search models")
    @ApiResponse(responseCode = "200", description = "List of data model objects")
    @GetMapping(value = "/search-models", produces = APPLICATION_JSON_VALUE)
    public SearchResponseDTO<IndexModel> getModels(ModelSearchRequest request) {
        return searchIndexService.searchModels(request, userProvider.getUser());
    }

    @Operation(summary = "Search resources", description = "List of resources")
    @ApiResponse(responseCode = "200", description = "List of resources as JSON")
    @GetMapping(path = "/search-internal-resources", produces = APPLICATION_JSON_VALUE)
    public SearchResponseDTO<IndexResource> getInternalResources(ResourceSearchRequest request) throws IOException {
        return searchIndexService.searchInternalResources(request, userProvider.getUser());
    }

    @Operation(summary = "Search resources", description = "List of resources")
    @ApiResponse(responseCode = "200", description = "List of resources as JSON")
    @GetMapping(path = "/search-internal-resources-info", produces = APPLICATION_JSON_VALUE)
    public SearchResponseDTO<IndexResourceInfo> getInternalResourcesInfo(ResourceSearchRequest request) throws IOException {
        return searchIndexService.searchInternalResourcesWithInfo(request, userProvider.getUser());
    }

    @Operation(summary = "Get supported data types")
    @ApiResponse(responseCode = "200", description = "List of supported data types")
    @GetMapping(path = "/data-types", produces = APPLICATION_JSON_VALUE)
    public List<String> getSupportedDataTypes() {
        return ModelConstants.SUPPORTED_DATA_TYPES;
    }

    @Operation(summary = "Search schemas")
    @ApiResponse(responseCode = "200", description = "List of schema objects")
    @GetMapping(value = "/searchSchemas", produces = APPLICATION_JSON_VALUE)
    public SearchResponseDTO<IndexSchema> getSchemas(ModelSearchRequest request) {
        return searchIndexService.searchSchemas(request, userProvider.getUser());
    }
    
    @Operation(summary = "Search crosswalks")
    @ApiResponse(responseCode = "200", description = "List of crosswalk objects")
    @GetMapping(value = "/searchCrosswalks", produces = APPLICATION_JSON_VALUE)
    public SearchResponseDTO<IndexCrosswalk> getCrosswalkss(CrosswalkSearchRequest request) {
        return searchIndexService.searchCrosswalks(request, userProvider.getUser());
    }

    @Operation(summary = "Get resolved external namespaces")
    @ApiResponse(responseCode = "200", description = "List of resolved namespaces")
    @GetMapping(path = "/namespaces", produces = APPLICATION_JSON_VALUE)
    public Set<String> getResolvedNamespaces() {
        return namespaceService.getResolvedNamespaces();
    }
    

    @Operation(summary = "MSCR Search")
    @ApiResponse(responseCode = "200", description = "Search for schemas and crosswalks")
    @GetMapping(value = "/mscrSearch", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> mscrSearch(MSCRSearchRequest request) {
    	SearchResponse<ObjectNode> r = searchIndexService.mscrSearch(request, true);
    	return new ResponseEntity<String>(OpenSearchUtil.serializePayload(r), HttpStatus.OK);
    	
    	
    } 
    
    @Operation(summary = "Search user content")
    @ApiResponse(responseCode = "200", description = "Search for schemas and crosswalks that are owned directly by the user")
    @GetMapping(value = "/mscrSearchPersonalContent", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> searchPersonalContent(MSCRSearchRequest request) {
    	SearchResponse<ObjectNode> r = searchIndexService.mscrSearch(request, false, Set.of(userProvider.getUser().getId().toString()));
    	return new ResponseEntity<String>(OpenSearchUtil.serializePayload(r), HttpStatus.OK);	    	
    } 
    
    @Operation(summary = "Search user's org content")
    @ApiResponse(responseCode = "200", description = "Search for schemas and crosswalks that are part of some organization of the user")
    @GetMapping(value = "/mscrSearchOrgContent", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> searchOrgContent(MSCRSearchRequest request, @RequestParam String ownerOrg) {
    	YtiUser user = userProvider.getUser();
    	if(!user.isInOrganization(UUID.fromString(ownerOrg))) {
    		throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "User is not part of  the given organization.");
    	}
    	SearchResponse<ObjectNode> r = searchIndexService.mscrSearch(request, false, Set.of(ownerOrg));
    	return new ResponseEntity<String>(OpenSearchUtil.serializePayload(r), HttpStatus.OK);	    	
    } 
    
    @Operation(summary = "Get functions")
    @ApiResponse(responseCode = "200", description = "")    
    @GetMapping(value="/functions", produces = APPLICATION_JSON_VALUE)
    public List<FunctionDTO> getFunctions() {    	
    	return frontendService.getFunctions();
    }
    
    @Operation(summary = "Get filters that can be used as part of mappings")
    @ApiResponse(responseCode = "200", description = "")    
    @GetMapping(value="/filters", produces = APPLICATION_JSON_VALUE)
    public List<FunctionDTO> getFilters() {    	
    	return frontendService.getFilters();
    }        
    
    @Operation(summary = "Get schema information for the crosswalk UI")
    @ApiResponse(responseCode = "200", description = "")    
    @GetMapping(value="/schema/{pid}", produces = APPLICATION_JSON_VALUE)
    public CrosswalkEditorSchemaDTO getSchema(@PathVariable String pid) {   
		Model model = jenaService.getSchema(pid);
		SchemaInfoDTO metadata = schemaMapper.mapToSchemaDTO(pid, model);
		String contentString = null;
		if(metadata.getFormat() == SchemaFormat.SKOSRDF) {
			try {
				contentString = schemaWriter.skosSchema(pid, model, "en");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else {
			contentString = schemaWriter.newModelSchema(pid, model, "en");
		}
		
    	try {
			return frontendService.getSchema(contentString, metadata);
		} catch (Exception e) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
		}
    }    
        
}
