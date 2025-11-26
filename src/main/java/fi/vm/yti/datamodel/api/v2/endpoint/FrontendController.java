package fi.vm.yti.datamodel.api.v2.endpoint;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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

import com.fasterxml.jackson.databind.node.ObjectNode;

import fi.vm.yti.datamodel.api.v2.dto.CrosswalkEditorSchemaDTO;
import fi.vm.yti.datamodel.api.v2.dto.FunctionDTO;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.OrganizationDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaParserResultDTO;
import fi.vm.yti.datamodel.api.v2.dto.ServiceCategoryDTO;
import fi.vm.yti.datamodel.api.v2.mapper.SchemaMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.OpenSearchUtil;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.CountRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.CountSearchResponse;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.CrosswalkSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.MSCRSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.ModelSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.ResourceSearchRequest;
import fi.vm.yti.datamodel.api.v2.opensearch.dto.SearchResponseDTO;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexCrosswalk;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexModel;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexResource;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexResourceInfo;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexSchema;
import fi.vm.yti.datamodel.api.v2.service.FrontendService;
import fi.vm.yti.datamodel.api.v2.service.GroupManagementService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.JsonSchemaWriter;
import fi.vm.yti.datamodel.api.v2.service.NamespaceService;
import fi.vm.yti.datamodel.api.v2.service.SearchIndexService;
import fi.vm.yti.security.AuthenticatedUserProvider;
import fi.vm.yti.security.YtiUser;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("v2/frontend")
@Tag(name = "Frontend")
public class FrontendController {

    private static final Logger logger = LoggerFactory.getLogger(FrontendController.class);
    private final SearchIndexService searchIndexService;
    private final FrontendService frontendService;
    private final AuthenticatedUserProvider userProvider;
    private final NamespaceService namespaceService;
    private final GroupManagementService groupManagementService;
    
    private final JenaService jenaService;
    private final SchemaMapper schemaMapper;
    private final JsonSchemaWriter schemaWriter;
    

    @Autowired
    public FrontendController(SearchIndexService searchIndexService,
                              FrontendService frontendService,
                              AuthenticatedUserProvider userProvider,
                              NamespaceService namespaceService,
                              GroupManagementService groupManagementService,
                              JenaService jenaService,
                              SchemaMapper schemaMapper,
                      		  JsonSchemaWriter schemaWriter
                      		  
    							) {
        this.searchIndexService = searchIndexService;
        this.frontendService = frontendService;
        this.userProvider = userProvider;
        this.namespaceService = namespaceService;
        this.groupManagementService = groupManagementService;
        
        this.jenaService = jenaService;
        this.schemaMapper = schemaMapper;
        this.schemaWriter = schemaWriter;        
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

    @Operation(summary = "MSCR Search")
    @ApiResponse(responseCode = "200", description = "Search for schemas and crosswalks")
    @GetMapping(value = "/mscrSearch", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> mscrSearch(MSCRSearchRequest request) {
    	
    	SearchResponse<ObjectNode> r = searchIndexService.mscrSearch(request, true, userProvider.getUser() != null && !userProvider.getUser().isAnonymous()? Set.of(userProvider.getUser().getId().toString()): null);
    	return new ResponseEntity<String>(OpenSearchUtil.serializePayload(r), HttpStatus.OK);
    	
    	
    } 
    
    @Operation(summary = "Search user content")
    @SecurityRequirement(name = "Bearer Authentication")
    @ApiResponse(responseCode = "200", description = "Search for schemas and crosswalks that are owned directly by the user")
    @GetMapping(value = "/mscrSearchPersonalContent", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<String> searchPersonalContent(MSCRSearchRequest request) {
    	SearchResponse<ObjectNode> r = searchIndexService.mscrSearch(request, false, Set.of(userProvider.getUser().getId().toString()));
    	return new ResponseEntity<String>(OpenSearchUtil.serializePayload(r), HttpStatus.OK);	    	
    } 
    
    @Operation(summary = "Search user's org content")
    @SecurityRequirement(name = "Bearer Authentication")
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
    @GetMapping(value="/schema/{pid}/content", produces = APPLICATION_JSON_VALUE)
    public CrosswalkEditorSchemaDTO getSchema(@PathVariable String pid) {   
		Model model = jenaService.getSchema(pid);
		model.add(jenaService.getSchema(pid+":content"));
		var ownerMapper = groupManagementService.mapOwner();
		SchemaInfoDTO metadata = schemaMapper.mapToFrontendSchemaDTO(pid, model, ownerMapper);
		String contentString = null;
		if(metadata.getFormat() == SchemaFormat.SKOSRDF|| metadata.getFormat() == SchemaFormat.ENUM) {
			try {
				contentString = schemaWriter.skosSchema(pid, model, "en");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if(metadata.getFormat() == SchemaFormat.RDFS) {
			try {
				contentString = schemaWriter.rdfs(pid, model, "en");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if(metadata.getFormat() == SchemaFormat.OWL) {
			try {
				contentString = schemaWriter.owlVocabulary(pid, model, "en");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
		else if(metadata.getFormat() == SchemaFormat.SHACL) {
			try {
				contentString = schemaWriter.shacl(pid, model, "en");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}			
		else {
			SchemaFormat format = metadata.getOriginalFormat() != null ? metadata.getOriginalFormat() : metadata.getFormat();
			contentString = schemaWriter.newModelSchema(pid, model, "en", format);
		}
		
    	try {
			return frontendService.getSchema(contentString, metadata);
		} catch (Exception e) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
		}
    }    
    
    
	@Operation(summary = "Get XSD schema file structure")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@GetMapping(path = "/xsdStructure", produces = APPLICATION_JSON_VALUE)
	public SchemaParserResultDTO getXsdSchemaStructure(
			@RequestParam(value = "url", required = true) String url) {
		// require a user session
		YtiUser user = userProvider.getUser();
		if(user == null || user.isAnonymous()) {
			throw new ResponseStatusException(HttpStatus.FORBIDDEN, "This endpoint requires a user session.");
		}
		return frontendService.getSchemaStructure(url);
		
	}    
        
}
