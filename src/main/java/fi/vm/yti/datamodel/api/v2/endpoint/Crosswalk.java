package fi.vm.yti.datamodel.api.v2.endpoint;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkDTO;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkFormat;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MSCRState;
import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.CrosswalkMapper;
import fi.vm.yti.datamodel.api.v2.mapper.MappingMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.service.GroupManagementService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.PIDService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFile;
import fi.vm.yti.datamodel.api.v2.service.impl.PostgresStorageService;
import fi.vm.yti.datamodel.api.v2.validator.ValidCrosswalk;
import fi.vm.yti.datamodel.api.v2.validator.ValidMapping;
import fi.vm.yti.security.AuthenticatedUserProvider;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("v2")
@Tag(name="Crosswalk")
@Validated
public class Crosswalk extends BaseMSCRController {
	private static final Logger logger = LoggerFactory.getLogger(Crosswalk.class);


	
    private final AuthorizationManager authorizationManager;
    private final OpenSearchIndexer openSearchIndexer;
	private final PIDService PIDService;
	private final StorageService storageService;
    private final JenaService jenaService;
	private final CrosswalkMapper mapper;
	private final MappingMapper mappingMapper;
	private final AuthenticatedUserProvider userProvider;
    private final GroupManagementService groupManagementService;


	public Crosswalk(AuthorizationManager authorizationManager,
            OpenSearchIndexer openSearchIndexer,
            PIDService PIDService,
            PostgresStorageService storageService,
            JenaService jenaService,
            CrosswalkMapper mapper,
            MappingMapper mappingMapper,
            AuthenticatedUserProvider userProvider,
            GroupManagementService groupManagementService) {
		this.openSearchIndexer = openSearchIndexer;
		this.authorizationManager = authorizationManager;
		this.PIDService = PIDService;
		this.storageService = storageService;		
		this.jenaService = jenaService;
		this.mapper = mapper;
		this.mappingMapper = mappingMapper;
		this.userProvider = userProvider;
		this.groupManagementService = groupManagementService;
	}
	
	private CrosswalkInfoDTO getCrosswalkDTO(String pid, boolean includeVersionInfo) {
        var model = jenaService.getCrosswalk(pid);
        if(model == null){
            throw new ResourceNotFoundException(pid);
        }
        var hasRightsToModel = authorizationManager.hasRightToModel(pid, model);
        
        check(hasRightsToModel);
        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;

        return mapper.mapToCrosswalkDTO(pid, model, includeVersionInfo, userMapper);
	}	
	
	private void createCrosswalkMetadata(final String PID, CrosswalkDTO dto, String aggregationKey, String target) {
		if(!dto.getOrganizations().isEmpty()) {
			check(authorizationManager.hasRightToAnyOrganization(dto.getOrganizations()));
		}
				
		

		Model jenaModel = mapper.mapToJenaModel(PID, dto, target, aggregationKey, userProvider.getUser());
		jenaService.putToCrosswalk(PID, jenaModel);
		
		// handle possible versioning data
		var crosswalkResource = jenaModel.createResource(PID);
		if(jenaModel.contains(crosswalkResource, MSCR.PROV_wasRevisionOf)) {			
			Model prevVersionModel = jenaService.getCrosswalk(target);
			Resource prevVersionResource = prevVersionModel.getResource(target);
			prevVersionResource.addProperty(MSCR.hasRevision, crosswalkResource);
			jenaService.updateCrosswalk(target, prevVersionModel);			
		}
		
		var indexModel = mapper.mapToIndexModel(PID, jenaModel);
        openSearchIndexer.createCrosswalkToIndex(indexModel);
	}
	
	private CrosswalkDTO mergeMetadata(CrosswalkInfoDTO prev, CrosswalkDTO input, boolean isRevision) {
		if(input == null) {
			// make a copy of the prev version's metadata and return it.
			return mapper.mapToCrosswalkDTO(prev);
		}
		CrosswalkDTO s = new CrosswalkDTO();
		// in case of revision the following data cannot be overridden
		// - organization
		s.setStatus(input.getStatus() != null ? input.getStatus() : Status.DRAFT);
		s.setState(input.getState() != null ? input.getState() : MSCRState.DRAFT);
		s.setLabel(!input.getLabel().isEmpty()? input.getLabel() : prev.getLabel());
		s.setDescription(!input.getDescription().isEmpty() ? input.getDescription() : prev.getDescription());
		s.setLanguages(!input.getLanguages().isEmpty() ? input.getLanguages() : prev.getLanguages());
		if(isRevision || input.getOrganizations().isEmpty()) {
			s.setOrganizations(prev.getOrganizations().stream().map(org ->  UUID.fromString(org.getId())).collect(Collectors.toSet()));
		}	
		else {
			s.setOrganizations(input.getOrganizations());			
		}	
		s.setVersionLabel(input.getVersionLabel() != null ? input.getVersionLabel() : "");
		s.setFormat(input.getFormat() != null ? input.getFormat() : prev.getFormat());
		s.setSourceSchema(input.getSourceSchema() != null ? input.getSourceSchema() : prev.getSourceSchema());
		s.setTargetSchema(input.getTargetSchema() != null ? input.getTargetSchema() : prev.getTargetSchema());
		return s;
		
	}	
	private void addFileToCrosswalk(final String pid, CrosswalkFormat format, MultipartFile file) {
		final String contentType = file.getContentType();		 
		try {
			if(EnumSet.of(CrosswalkFormat.CSV, CrosswalkFormat.MSCR, CrosswalkFormat.SSSOM, CrosswalkFormat.XSLT, CrosswalkFormat.PDF).contains(format)) {
				storageService.storeCrosswalkFile(pid, contentType, file.getBytes());
			}
			else {
				throw new Exception("Unsupported crosswalk description format. Supported formats are: " + String.join(", ", Arrays.toString(CrosswalkFormat.values()) ));
			}
		
		
		} catch (Exception ex) {
			throw new RuntimeException("Error occured while ingesting file based crosswalk description", ex);
		}
		
	}
	
	@Operation(summary = "Create crosswalk metadata record.")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path="/crosswalk", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public CrosswalkInfoDTO createCrosswalk(@ValidCrosswalk @RequestBody(required = false) CrosswalkDTO dto, @RequestParam(name = "action", required = false) CONTENT_ACTION action, @RequestParam(name = "target", required = false) String target) {
		logger.info("Create Crosswalk {}", dto);
		validateActionParams(dto, action, target); 
		String aggregationKey = null;
		if(action != null) {			
			CrosswalkInfoDTO prev = getCrosswalkDTO(target, true);
			dto = mergeMetadata(prev, dto, action == CONTENT_ACTION.revisionOf);			
			if(action == CONTENT_ACTION.revisionOf) {
				// revision must be made from the latest version
				if(prev.getHasRevisions() != null && !prev.getHasRevisions().isEmpty()) {
					throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Revisions can only be created from the latest revision. Check your target PID.");
				}
				aggregationKey = prev.getAggregationKey();
				
			}
		}
		final String PID = PIDService.mint(PIDType.HANDLE);
		try {
			createCrosswalkMetadata(PID, dto, aggregationKey, target);
			var userMapper = groupManagementService.mapUser();
			return mapper.mapToCrosswalkDTO(PID, jenaService.getCrosswalk(PID), false, userMapper);
		}catch(Exception ex) {
			// revert any possible changes
			try { jenaService.deleteFromCrosswalk(PID); }catch(Exception _ex) { logger.error(_ex.getMessage(), _ex);}
			try { openSearchIndexer.deleteCrosswalkFromIndex(PID);}catch(Exception _ex) { logger.error(_ex.getMessage(), _ex);}
			if( (ex instanceof ResponseStatusException) || (ex instanceof MappingError)) {
				throw ex;
			}
			else {
				throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Unknown error occured. " + ex.getMessage(), ex);
			}
		}  
	}
	
	
	@Operation(summary = "Upload and associate a crosswalk description file to an existing crosswalk")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/crosswalk/{pid}/upload", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public CrosswalkInfoDTO uploadCrosswalkFile(@PathVariable String pid,
			@RequestParam("file") MultipartFile file) throws Exception {
		
		try {
			// check for auth here because addFileToSchema is not doing it
			var model = jenaService.getCrosswalk(pid);
			var userMapper = groupManagementService.mapUser();
			CrosswalkInfoDTO crosswalkDTO = mapper.mapToCrosswalkDTO(pid, model, userMapper);
			if(!crosswalkDTO.getOrganizations().isEmpty()) {
				Collection<UUID> orgs = crosswalkDTO.getOrganizations().stream().map(org ->  UUID.fromString(org.getId())).toList();
				check(authorizationManager.hasRightToAnyOrganization(orgs));	
			}		
			
			addFileToCrosswalk(pid, crosswalkDTO.getFormat(), file);
			return mapper.mapToCrosswalkDTO(pid, model, userMapper);
		}catch(Exception ex) {
			// revert any possible changes
			
			try {storageService.deleteAllCrosswalkFiles(pid);}catch(Exception _ex) { logger.error(_ex.getMessage(), _ex);}
			if( (ex instanceof ResponseStatusException)) {
				throw ex;
			}
			else {
				throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Unknown error occured. " + ex.getMessage(), ex);
			}
		}
					
	}
	
	
	@Operation(summary = "Create crosswalk by uploading metadata and files in one multipart request")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/crosswalkFull", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public CrosswalkInfoDTO createCrosswalkFull(@RequestParam("metadata") String metadataString,
			@RequestParam("file") MultipartFile file, @RequestParam(name = "action", required = false) CONTENT_ACTION action, @RequestParam(name = "target", required = false) String target) {
		
		ObjectMapper objMapper = new ObjectMapper();
		CrosswalkDTO dto = null;
		try {
			dto = objMapper.readValue(metadataString, CrosswalkDTO.class);
		} catch (JsonProcessingException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Could not parse CrosswalkDTO from the metadata content. " + e.getMessage(), e);
		}
		
		CrosswalkInfoDTO infoDto = createCrosswalk(dto, action, target);
		final String PID = infoDto.getPID();
		addFileToCrosswalk(PID, dto.getFormat(), file);
		var userMapper = groupManagementService.mapUser();
		return mapper.mapToCrosswalkDTO(PID, jenaService.getCrosswalk(PID), false, userMapper);
		
	}	
	
    @Operation(summary = "Modify crosswalk")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new crosswalk node")
    @ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
    @SecurityRequirement(name = "Bearer Authentication")
    @PostMapping(path = "/crosswalk/{pid}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public CrosswalkInfoDTO updateModel(@RequestBody CrosswalkDTO dto,
                            @PathVariable String pid) {
        logger.info("Updating crosswalk {}", dto);

        var oldModel = jenaService.getCrosswalk(pid);
        if(oldModel == null){
            throw new ResourceNotFoundException(pid);
        }

        check(authorizationManager.hasRightToModel(pid, oldModel));
        var userMapper = groupManagementService.mapUser();
        CrosswalkInfoDTO prev =  mapper.mapToCrosswalkDTO(pid, oldModel, false, userMapper);        
        dto = mergeMetadata(prev, dto, false);		        

        var jenaModel = mapper.mapToUpdateJenaModel(pid, dto, oldModel, userProvider.getUser());

        jenaService.putToCrosswalk(pid, jenaModel);


        var indexModel = mapper.mapToIndexModel(pid, jenaModel);
        openSearchIndexer.updateCrosswalkToIndex(indexModel);
        CrosswalkInfoDTO updated = mapper.mapToCrosswalkDTO(pid, jenaModel, false, userMapper);
        return updated;
    }        
	
    @Operation(summary = "Get a crosswalk metadata")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(value = "/crosswalk/{pid}", produces = APPLICATION_JSON_VALUE)
    public CrosswalkInfoDTO getCrosswalkMetadata(@PathVariable String pid, @RequestParam(name = "includeVersionInfo", defaultValue = "false") String includeVersionInfo){
    	var jenaModel = jenaService.getCrosswalk(pid);
		var hasRightsToModel = authorizationManager.hasRightToModel(pid, jenaModel);
        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;

    	return mapper.mapToCrosswalkDTO(pid, jenaModel, Boolean.parseBoolean(includeVersionInfo), userMapper);
    }
    
    @Operation(summary = "Get original file version of the crosswalk (if available)", description = "If the result is only one file it is returned as is, but if the content includes multiple files they a returned as a zip file.")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(path = "/crosswalk/{pid}/original")
    public ResponseEntity<byte[]> exportOriginalFile(@PathVariable String pid) {
    	List<StoredFile> files = storageService.retrieveAllCrosswalkFiles(pid);
    	return handleFileDownload(files);
	}
    
	@Operation(summary = "Create a mapping")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path="/crosswalk/{pid}/mapping", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public MappingDTO createMapping(@ValidMapping @RequestBody MappingDTO dto, @PathVariable String pid) {
		logger.info("Create Mapping {} for crosswalk {}", dto, pid);
        var crosswalkModel = jenaService.getCrosswalk(pid);
        if(crosswalkModel == null){
            throw new ResourceNotFoundException(pid);
        }
        check(authorizationManager.hasRightToModel(pid, crosswalkModel));
        
		final String mappingPID = PIDService.mintPartIdentifier(pid);

		Model mappingModel = mappingMapper.mapToJenaModel(mappingPID, dto, pid);
		jenaService. putToCrosswalk(mappingPID, mappingModel);
		Resource crosswalkResource = crosswalkModel.getResource(pid);
		crosswalkResource.addProperty(MSCR.mappings, ResourceFactory.createResource(mappingPID));
		jenaService.putToCrosswalk(pid, crosswalkModel);
		return mappingMapper.mapToMappingDTO(mappingPID, mappingModel);		
	}
	
	@Operation(summary = "Get a mapping")
	@ApiResponse(responseCode = "200")	
	@GetMapping(path="/crosswalk/{pid}/mapping/{mappingPID}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public MappingDTO getMapping(@PathVariable String pid, @PathVariable String mappingPID) {
		logger.info("Get Mapping {} for crosswalk {}", mappingPID, pid);
		// TODO: check that crosswalk exists
		var mappingModel = jenaService.getCrosswalk(mappingPID);
        if(mappingModel == null){
            throw new ResourceNotFoundException(mappingPID);
        }
		
		return mappingMapper.mapToMappingDTO(mappingPID, mappingModel);		
	}
	
	@Operation(summary = "Delete a mapping")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path="/crosswalk/{pid}/mapping/{mappingPID}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public void deleteMapping(@PathVariable String pid, @PathVariable String mappingPID) {
		logger.info("Delete Mapping {} for crosswalk {}", mappingPID, pid);
		// TODO: check that crosswalk exists
		var crosswalkModel = jenaService.getCrosswalk(pid);
        if(crosswalkModel == null){
            throw new ResourceNotFoundException(pid);
        }
        check(authorizationManager.hasRightToModel(pid, crosswalkModel));
        crosswalkModel.remove(crosswalkModel.getResource(pid), MSCR.mappings, crosswalkModel.getResource(mappingPID));
		jenaService.deleteFromCrosswalk(mappingPID);		
				
	}
		
	@Operation(summary = "Get a mappings for a crosswalk")
	@ApiResponse(responseCode = "200")	
	@GetMapping(path="/crosswalk/{pid}/mapping", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public List<MappingDTO> getMappings(@PathVariable String pid) {
		logger.info("Get Mappings for crosswalk {}", pid);
		// TODO: check that crosswalk exists
		var crosswalkModel = jenaService.getCrosswalk(pid);
		List<MappingDTO> mappings = new ArrayList<MappingDTO>();
		NodeIterator i = crosswalkModel.listObjectsOfProperty(crosswalkModel.getResource(pid), MSCR.mappings);
		while(i.hasNext()) {
			Resource mappingResource = i.next().asResource();			
			MappingDTO dto = mappingMapper.mapToMappingDTO(mappingResource.getURI(), crosswalkModel);
			mappings.add(dto);
		}
		
		return mappings;
	}	
}
