package fi.vm.yti.datamodel.api.v2.endpoint;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DCTerms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.dto.MappingInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.CrosswalkMapper;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
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
import io.swagger.v3.oas.annotations.Hidden;
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
        var hasRightsToModel = authorizationManager.hasRightToModelMSCR(pid, model);
        
        check(hasRightsToModel);
        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;

        return mapper.mapToCrosswalkDTO(pid, model, includeVersionInfo, userMapper);
	}	
	
	private void createCrosswalkMetadata(final String PID, final String handle, CrosswalkDTO dto, String aggregationKey, String target) {
		if(!dto.getOrganizations().isEmpty()) {
			check(authorizationManager.hasRightToAnyOrganization(dto.getOrganizations()));
		}
		checkVisibility(dto);	
		checkState(null, dto.getState());

		Model jenaModel = mapper.mapToJenaModel(PID, handle, dto, target, aggregationKey, userProvider.getUser());
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
		s.setStatus(input != null && input.getStatus() != null ? input.getStatus() : prev.getStatus());
		s.setState(input != null && input.getState() != null ? input.getState() : prev.getState());
		s.setVisibility(input != null && input.getVisibility() != null ? input.getVisibility() : prev.getVisibility());
		s.setLabel(input != null && !input.getLabel().isEmpty()? input.getLabel() : prev.getLabel());
		s.setDescription(input != null && !input.getDescription().isEmpty() ? input.getDescription() : prev.getDescription());
		s.setLanguages(!input.getLanguages().isEmpty() ? input.getLanguages() : prev.getLanguages());
		if(input != null && input.getOrganizations() !=null && !input.getOrganizations().isEmpty()) {
			s.setOrganizations(input.getOrganizations());			
		}	
		else {
			s.setOrganizations(prev.getOrganizations().stream().map(org ->  UUID.fromString(org.getId())).collect(Collectors.toSet()));
		}	
		s.setVersionLabel(input != null && input.getVersionLabel() != null ? input.getVersionLabel() : "");
		s.setFormat(input != null && input.getFormat() != null ? input.getFormat() : prev.getFormat());
		s.setContact(input != null && input.getContact() != null ? input.getContact() : prev.getContact());
		s.setSourceSchema(prev.getSourceSchema());
		s.setTargetSchema(prev.getTargetSchema());
		return s;
		
	}	
	
	private MappingDTO mergeMetadata(MappingInfoDTO prev, MappingDTO input) {
		MappingDTO d = new MappingDTO();
		
		d.setId(input != null && input.getId() != null ? input.getId() : prev.getId());
		d.setDepends_on(input != null && input.getDepends_on() != null ? input.getDepends_on() : prev.getDepends_on());
		d.setSource(input != null && input.getSource() != null ? input.getSource() : prev.getSource());
		d.setSourceType(input != null && input.getSourceType() != null ? input.getSourceType() : prev.getSourceType());
		d.setSourceDescription(input != null && input.getSourceDescription() != null ? input.getSourceDescription() : prev.getSourceDescription());
		d.setPredicate(input != null && input.getPredicate() != null ? input.getPredicate() : prev.getPredicate());
		d.setFilter(input != null && input.getFilter() != null ? input.getFilter() : prev.getFilter());
		d.setTarget(input != null && input.getTarget() != null ? input.getTarget() : prev.getTarget());
		d.setTargetType(input != null && input.getTargetType() != null ? input.getTargetType() : prev.getTargetType());
		d.setTargetDescription(input != null && input.getTargetDescription() != null ? input.getTargetDescription() : prev.getTargetDescription());
		d.setProcessing(input != null && input.getProcessing() != null ? input.getProcessing() : prev.getProcessing());
		d.setOneOf(input != null && input.getOneOf() != null ? input.getOneOf() : prev.getOneOf());
		return d;
		
	}
	
	private void addFileToCrosswalk(final String pid, CrosswalkFormat format, MultipartFile file) {
		final String contentType = file.getContentType();		 
		try {
			if(EnumSet.of(CrosswalkFormat.CSV, CrosswalkFormat.MSCR, CrosswalkFormat.SSSOM, CrosswalkFormat.XSLT, CrosswalkFormat.PDF).contains(format)) {
				storageService.storeCrosswalkFile(pid, contentType, file.getBytes(), generateFilename(pid, file.getContentType()));
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
	public CrosswalkInfoDTO createCrosswalk(@ValidCrosswalk @RequestBody(required = false) CrosswalkDTO dto, @RequestParam(name = "action", required = false) CONTENT_ACTION action, @RequestParam(name = "target", required = false) String target) throws Exception {
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
		final String PID = "mscr:crosswalk:" + UUID.randomUUID();
		try {
			String handle = null;
			if(dto.getState() == MSCRState.PUBLISHED || dto.getState() == MSCRState.DEPRECATED) {
				 handle = PIDService.mint(PIDType.HANDLE, MSCRType.CROSSWALK, PID);
				
			}
			createCrosswalkMetadata(PID, handle, dto, aggregationKey, target);
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
			@RequestParam("file") MultipartFile file) {
		return uploadCrosswalkFile(pid, null, file);
		
	}
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/crosswalk/{pid}/{suffix}/upload", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public CrosswalkInfoDTO uploadCrosswalkFile(
			@PathVariable String pid,
			@PathVariable(name = "suffix") String suffix,
			@RequestParam("file") MultipartFile file
			) {
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}		
		try {
			pid = PIDService.mapToInternal(pid);
			// check for auth here because addFileToSchema is not doing it
			var model = jenaService.getCrosswalk(pid);
			if(!isEditable(model, pid)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Content can only be edited in the DRAFT state.");
			}

			var userMapper = groupManagementService.mapUser();
			CrosswalkInfoDTO crosswalkDTO = mapper.mapToCrosswalkDTO(pid, model, userMapper);
			
			if(!crosswalkDTO.getOrganizations().isEmpty()) {
				Collection<UUID> orgs = crosswalkDTO.getOrganizations().stream().map(org ->  UUID.fromString(org.getId())).toList();
				check(authorizationManager.hasRightToAnyOrganization(orgs));	
			}		
			
			addFileToCrosswalk(pid, crosswalkDTO.getFormat(), file);
			return mapper.mapToCrosswalkDTO(pid, model, userMapper);
		
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		} finally {
			try {storageService.deleteAllCrosswalkFiles(pid);}catch(Exception _ex) { }

		}							
	}
	
	
	@Operation(summary = "Create crosswalk by uploading metadata and files in one multipart request")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/crosswalkFull", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public CrosswalkInfoDTO createCrosswalkFull(@RequestParam("metadata") String metadataString,
			@RequestParam("file") MultipartFile file, @RequestParam(name = "action", required = false) CONTENT_ACTION action, @RequestParam(name = "target", required = false) String target) throws Exception {
		
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
	
    @Operation(summary = "Modify crosswalk metadata")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new crosswalk node")
    @ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
    @SecurityRequirement(name = "Bearer Authentication")
    @PatchMapping(path = "/crosswalk/{pid}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public CrosswalkInfoDTO updateModel(@RequestBody CrosswalkDTO dto,
    		@PathVariable String pid) {
    	return updateModel(dto, pid, null);
    }
    
    @Hidden
    @SecurityRequirement(name = "Bearer Authentication")
    @PatchMapping(path = "/crosswalk/{pid}/{suffix}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public CrosswalkInfoDTO updateModel(@RequestBody CrosswalkDTO dto,
                            @PathVariable String pid,
                            @PathVariable(name = "suffix") String suffix) {
        logger.info("Updating crosswalk {}", dto);
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);

	        var oldModel = jenaService.getCrosswalk(pid);
	        if(oldModel == null){
	            throw new ResourceNotFoundException(pid);
	        }
	
	        check(authorizationManager.hasRightToModelMSCR(pid, oldModel));
	        var userMapper = groupManagementService.mapUser();
	        CrosswalkInfoDTO prev =  mapper.mapToCrosswalkDTO(pid, oldModel, false, userMapper);        
	        dto = mergeMetadata(prev, dto, false);		    
	        checkVisibility(dto);
	        checkState(prev, dto.getState());
	        Model jenaModel = null;
	        if(prev.getState() == MSCRState.DRAFT && dto.getState() == MSCRState.PUBLISHED) {
				try {
					String handle = PIDService.mint(PIDType.HANDLE, MSCRType.CROSSWALK, pid);
					jenaModel = mapper.mapToUpdateJenaModel(pid, handle, dto, oldModel, userProvider.getUser());	
				}catch(Exception ex) {
					throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Exception while geting a new handle for the schema." + ex.getMessage());
				}
			}
			else {
				jenaModel = mapper.mapToUpdateJenaModel(pid, null, dto, oldModel, userProvider.getUser());	
			}
	        
	
	        jenaService.putToCrosswalk(pid, jenaModel);
	
	
	        var indexModel = mapper.mapToIndexModel(pid, jenaModel);
	        openSearchIndexer.updateCrosswalkToIndex(indexModel);
	        CrosswalkInfoDTO updated = mapper.mapToCrosswalkDTO(pid, jenaModel, false, userMapper);
	        return updated;
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}	        
    }        
	
    @Operation(summary = "Get a crosswalk metadata")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(value = "/crosswalk/{pid}", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> getCrosswalkMetadata(@PathVariable String pid, @RequestParam(name = "includeVersionInfo", defaultValue = "false") String includeVersionInfo){
    	return getCrosswalkMetadata(pid, null, includeVersionInfo);
    }
    
    @Hidden
    @GetMapping(value = "/crosswalk/{pid}/{suffix}", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> getCrosswalkMetadata(
    		@PathVariable String pid, 
    		@PathVariable(name = "suffix") String suffix,
    		@RequestParam(name = "includeVersionInfo", defaultValue = "false") String includeVersionInfo){
		// TODO: get rid of this
    	if(pid.indexOf("@") > 0 || (suffix != null && suffix.indexOf("@") > 0)) {

    		return getMapping(pid, suffix);
    	}
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);    	
    	
	    	var jenaModel = jenaService.getCrosswalk(pid);
			var hasRightsToModel = authorizationManager.hasRightToModelMSCR(pid, jenaModel);
	        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;
	
	    	return ResponseEntity.ok(mapper.mapToCrosswalkDTO(pid, jenaModel, Boolean.parseBoolean(includeVersionInfo), userMapper));
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}
	    	
    }
    
    @Operation(summary = "Delete crosswalk metadata and content")
    @SecurityRequirement(name = "Bearer Authentication")
    @ApiResponse(responseCode = "200", description = "")
    @DeleteMapping(value = "/crosswalk/{pid}")
    public void deleteCrosswalk(@PathVariable String pid){
    	deleteCrosswalk(pid, null);
    }
    
    @Hidden
    @SecurityRequirement(name = "Bearer Authentication")
    @DeleteMapping(value = "/crosswalk/{pid}/{suffix}")
    public void deleteCrosswalk(
    		@PathVariable String pid, 
    		@PathVariable(name = "suffix") String suffix){
		// TODO: get rid of this
    	if(pid.indexOf("@") > 0 || (suffix != null && suffix.indexOf("@") > 0)) {
    		deleteMapping(pid, suffix);
    	}
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			var internalID = PIDService.mapToInternal(pid);   
	        var model = jenaService.getCrosswalk(internalID);
	        if(model == null){
	            throw new ResourceNotFoundException(pid);
	        }
	        check(authorizationManager.hasRightToModelMSCR(internalID, model));
			var userMapper = groupManagementService.mapUser();
			CrosswalkInfoDTO prev = mapper.mapToCrosswalkDTO(pid, model, userMapper);
			if(prev.getState() == MSCRState.DRAFT) {
				storageService.deleteAllCrosswalkFiles(internalID);
				jenaService.deleteFromCrosswalk(internalID);	
				openSearchIndexer.deleteCrosswalkFromIndex(internalID);
			}
			else {
				checkState(prev, MSCRState.REMOVED);
				CrosswalkDTO dto = new CrosswalkDTO();
				dto.setState(MSCRState.REMOVED);
				dto = mergeMetadata(prev, dto, false);
				var jenaModel = mapper.mapToUpdateJenaModel(pid, null, dto, ModelFactory.createDefaultModel(), userProvider.getUser());
				var indexModel = mapper.mapToIndexModel(internalID, jenaModel);								
				jenaService.updateCrosswalk(internalID, jenaModel);
				storageService.deleteAllCrosswalkFiles(internalID);
				openSearchIndexer.updateCrosswalkToIndex(indexModel);
			}
			
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}
    }
    
    @Operation(summary = "Get original file version of the crosswalk (if available)", description = "If the result is only one file it is returned as is, but if the content includes multiple files they a returned as a zip file.")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(path = "/crosswalk/{pid}/original")
    public ResponseEntity<byte[]> exportOriginalFile(@PathVariable String pid) {
    	return exportOriginalFile(pid, null);
    }
    
    @Hidden
    @GetMapping(path = "/crosswalk/{pid}/{suffix}/original")
    public ResponseEntity<byte[]> exportOriginalFile(
    		@PathVariable String pid,
    		@PathVariable(name = "suffix") String suffix) {
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);    	
	    	List<StoredFile> files = storageService.retrieveAllCrosswalkFiles(pid);
	    	return handleFileDownload(files);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}	    	
	}
    
    @Operation(summary = "Download crosswalk related file with a given id.")
    @ApiResponse(responseCode ="200")
    @GetMapping(path = "/crosswalk/{pid}/files/{fileID}")
    public ResponseEntity<byte[]> downloadFile(@PathVariable String pid, @PathVariable String fileID, @RequestParam(name="download", defaultValue = "false" ) String download) {
    	return downloadFile(pid, null, fileID, download); 
    }
    
    @Hidden
    @GetMapping(path = "/crosswalk/{pid}/{suffix}/files/{fileID}")
    public ResponseEntity<byte[]> downloadFile(
    		@PathVariable String pid,
    		@PathVariable String suffix,
    		@PathVariable String fileID, 
    		@RequestParam(name="download", defaultValue = "false" ) String download) {
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);    	
	    	StoredFile file = storageService.retrieveFile(pid, Long.parseLong(fileID), MSCRType.CROSSWALK);
	    	if(file == null) {
	    		throw new ResourceNotFoundException(pid + "@file=" + fileID); 
	    	}
	    	return handleFileDownload(List.of(file), download);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}	    	
    }
    
	@Operation(summary = "Delete file")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path="/crosswalk/{pid}/files/{fileID}", produces = APPLICATION_JSON_VALUE)
	public void deleteFile(@PathVariable String pid, @PathVariable Long fileID) throws Exception {
		deleteFile(pid, null, fileID);
	}
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path="/crosswalk/{pid}/{suffix}/files/{fileID}", produces = APPLICATION_JSON_VALUE)
	public void deleteFile(
			@PathVariable String pid, 
			@PathVariable String suffix,
			@PathVariable Long fileID) throws Exception {
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);		
			var crosswalkModel = jenaService.getCrosswalk(pid);
			check(authorizationManager.hasRightToModelMSCR(pid, crosswalkModel));
			if(!isEditable(crosswalkModel, pid)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Content can only be edited in the DRAFT state.");
			}
			var fileMetadata = storageService.retrieveFileMetadata(pid, fileID, MSCRType.CROSSWALK);
			if(fileMetadata == null) {
				throw new ResourceNotFoundException(pid + "@file=" + fileID);
			}
			storageService.removeFile(fileID);	
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}			
	}        
    
	@Operation(summary = "Create a mapping")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path="/crosswalk/{pid}/mapping", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public MappingInfoDTO createMapping(@ValidMapping @RequestBody MappingDTO dto, @PathVariable String pid) {
		return createMapping(dto, pid, null);
	}
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path="/crosswalk/{pid}/{suffix}/mapping", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public MappingInfoDTO createMapping(
			@ValidMapping @RequestBody MappingDTO dto, 
			@PathVariable String pid,
			@PathVariable String suffix
			) {
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {	
			pid = PIDService.mapToInternal(pid);
			logger.info("Create Mapping {} for crosswalk {}", dto, pid);		
	        var crosswalkModel = jenaService.getCrosswalk(pid);
	        if(crosswalkModel == null){
	            throw new ResourceNotFoundException(pid);
	        }
	        check(authorizationManager.hasRightToModelMSCR(pid, crosswalkModel));
			if(!isEditable(crosswalkModel, pid)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Content can only be edited in the DRAFT state.");
			}
	        
			final String mappingPID = PIDService.mintPartIdentifier(pid);
	
			Model mappingModel = mappingMapper.mapToJenaModel(mappingPID, dto, pid);
			jenaService.putToCrosswalk(mappingPID, mappingModel);
			Resource crosswalkResource = crosswalkModel.getResource(pid);
			crosswalkResource.addProperty(MSCR.mappings, ResourceFactory.createResource(mappingPID));
			jenaService.putToCrosswalk(pid, crosswalkModel);
			return mappingMapper.mapToMappingDTO(mappingPID, mappingModel);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}		
	}
	
	
	
	@Operation(summary = "Update mapping")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path="/crosswalk/{mappingPID}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public MappingInfoDTO updateMapping(@ValidMapping @RequestBody MappingDTO dto, @PathVariable String mappingPID) {
		return updateMapping(dto, mappingPID, null);
	}
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path="/crosswalk/{mappingPID}/{suffix}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public MappingInfoDTO updateMapping(
			@ValidMapping @RequestBody MappingDTO dto,
			@PathVariable String mappingPID,
			@PathVariable String suffix) {
		if (suffix != null) {
			mappingPID = mappingPID + "/" + suffix;
		}
		try {
			mappingPID = PIDService.mapToInternal(mappingPID);		
			logger.info("Update Mapping {} for id {}", dto, mappingPID);
			var mappingModelOriginal = jenaService.getCrosswalk(mappingPID);
			Resource mappingResource = mappingModelOriginal.getResource(mappingPID);
			String pid = MapperUtils.propertyToString(mappingResource, DCTerms.isPartOf);		
			
	        var crosswalkModel = jenaService.getCrosswalk(pid);
	        if(crosswalkModel == null){
	            throw new ResourceNotFoundException(pid);
	        }
	        check(authorizationManager.hasRightToModelMSCR(pid, crosswalkModel));
			if(!isEditable(crosswalkModel, pid)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Content can only be edited in the DRAFT state.");
			}
	
			Model mappingModel = mappingMapper.mapToJenaModel(mappingPID, dto, pid);
			jenaService. putToCrosswalk(mappingPID, mappingModel);
			Resource crosswalkResource = crosswalkModel.getResource(pid);
			crosswalkResource.addProperty(MSCR.mappings, ResourceFactory.createResource(mappingPID));
			jenaService.putToCrosswalk(pid, crosswalkModel);
			return mappingMapper.mapToMappingDTO(mappingPID, mappingModel);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}		

	}	
	
	@Operation(summary = "Get a mapping")
	@ApiResponse(responseCode = "200")	
	@GetMapping(path="/crosswalk/{mappingPID}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public ResponseEntity<Object> getMapping(@PathVariable String mappingPID) {		
		return getMapping(mappingPID, null);
	}
	
	@Hidden
	@GetMapping(path="/crosswalk/{mappingPID}/{suffix}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public ResponseEntity<Object> getMapping(
			@PathVariable String mappingPID,
			@PathVariable String suffix
			) {
		// TODO: get rid of this
    	if((mappingPID.indexOf("@") < 0 && suffix == null) || (mappingPID.indexOf("@") < 0 && suffix != null && suffix.indexOf("@") < 0)) {
    		return getCrosswalkMetadata(mappingPID, suffix, "false");
    	}
    	
		if (suffix != null) {
			mappingPID = mappingPID + "/" + suffix;
		}
		try {
			mappingPID = PIDService.mapToInternal(mappingPID);			
			logger.info("Get Mapping {}", mappingPID);
			// TODO: check that crosswalk exists
			var mappingModel = jenaService.getCrosswalk(mappingPID);
	        if(mappingModel == null){
	            throw new ResourceNotFoundException(mappingPID);
	        }
			
			return ResponseEntity.ok().body(mappingMapper.mapToMappingDTO(mappingPID, mappingModel));
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}		
	}
	
	@Operation(summary = "Delete a mapping")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path="/crosswalk/{mappingPID}", produces = APPLICATION_JSON_VALUE)
	public void deleteMapping(@PathVariable String mappingPID) {
		deleteMapping(mappingPID, null);
	}
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path="/crosswalk/{mappingPID}/{suffix}", produces = APPLICATION_JSON_VALUE)
	public void deleteMapping(
			@PathVariable String mappingPID,
			@PathVariable String suffix) {
		if (suffix != null) {
			mappingPID = mappingPID + "/" + suffix;
		}
		try {
			mappingPID = PIDService.mapToInternal(mappingPID);		
			logger.info("Delete Mapping {}", mappingPID);
			// TODO: check that crosswalk exists
			var mappingModel = jenaService.getCrosswalk(mappingPID);
			Resource mappingResource = mappingModel.getResource(mappingPID);
			String pid = MapperUtils.propertyToString(mappingResource, DCTerms.isPartOf);		
			var crosswalkModel = jenaService.getCrosswalk(pid);
	        if(crosswalkModel == null){
	            throw new ResourceNotFoundException(pid);
	        }
	        check(authorizationManager.hasRightToModelMSCR(pid, crosswalkModel));
			if(!isEditable(crosswalkModel, pid)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Content can only be edited in the DRAFT state.");
			}
	
	        
	        crosswalkModel.remove(crosswalkModel.getResource(pid), MSCR.mappings, crosswalkModel.getResource(mappingPID));
			jenaService.deleteFromCrosswalk(mappingPID);
			jenaService.putToCrosswalk(pid, crosswalkModel);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}		
	}
		
	@Operation(summary = "Get a mappings for a crosswalk")
	@ApiResponse(responseCode = "200")	
	@GetMapping(path="/crosswalk/{pid}/mapping")
	public ResponseEntity<Object> getMappings(
			@PathVariable String pid, @RequestParam(name = "exportFormat", required = false) String exportFormat) {
		return getMappings(pid, null, exportFormat); 
	}

	@Hidden
	@ApiResponse(responseCode = "200")	
	@GetMapping(path="/crosswalk/{pid}/{suffix}/mapping")
	public ResponseEntity<Object> getMappings(
			@PathVariable String pid, 
			@PathVariable String suffix, 
			@RequestParam(name = "exportFormat", required = false) String exportFormat) {
		
		// TODO: check that crosswalk exists
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			logger.info("Get Mappings for crosswalk {}", pid);
			pid = PIDService.mapToInternal(pid);	
			var crosswalkModel = jenaService.getCrosswalk(pid);		
			List<MappingDTO> mappings = new ArrayList<MappingDTO>();
			NodeIterator i = crosswalkModel.listObjectsOfProperty(crosswalkModel.getResource(pid), MSCR.mappings);
			while(i.hasNext()) {
				Resource mappingResource = i.next().asResource();			
				MappingDTO dto = mappingMapper.mapToMappingDTO(mappingResource.getURI(), jenaService.getCrosswalk(mappingResource.getURI()));
				mappings.add(dto);
			}
			if(exportFormat !=null) {
				// TODO: check for crosswalk format == MSCR and source and target scheama format == X,Y,Z
				if(exportFormat.equals("skos")) {
					Model model = ModelFactory.createDefaultModel();
					mappings.forEach(mapping -> {
						String predicate = mapping.getPredicate();
						mapping.getSource().forEach(sourceNode -> {
							if(sourceNode.getUri() != null) {
								//map to all targets
								mapping.getTarget().forEach(targetNode -> {
									if(targetNode.getUri() != null) {									
										model.add(
											model.createResource(sourceNode.getUri()),
											model.createProperty(predicate),
											model.createResource(targetNode.getUri())
										);
									}
								});
							}
						});
					});
					
					StringWriter writer = new StringWriter();
	
					model.write(writer, "TURTLE");
					return ResponseEntity.ok(writer.getBuffer().toString());
				}
			}
			return ResponseEntity.ok(mappings);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}		
	}	
}
