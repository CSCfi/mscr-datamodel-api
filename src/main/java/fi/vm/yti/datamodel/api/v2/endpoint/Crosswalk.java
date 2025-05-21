package fi.vm.yti.datamodel.api.v2.endpoint;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.CrossOrigin;
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
import fi.vm.yti.datamodel.api.v2.dto.DeleteResponseDTO;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MSCRState;
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.dto.MappingInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.endpoint.BaseMSCRController.CONTENT_ACTION;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.CrosswalkMapper;
import fi.vm.yti.datamodel.api.v2.mapper.MappingMapper;
import fi.vm.yti.datamodel.api.v2.mapper.SchemaMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.service.CrosswalkService;
import fi.vm.yti.datamodel.api.v2.service.GroupManagementService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.PIDService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFile;
import fi.vm.yti.datamodel.api.v2.service.impl.PostgresStorageService;
import fi.vm.yti.datamodel.api.v2.transformation.RMLGenerator;
import fi.vm.yti.datamodel.api.v2.transformation.RMLGenerator2;
import fi.vm.yti.datamodel.api.v2.transformation.SPARQLGenerator;
import fi.vm.yti.datamodel.api.v2.transformation.XSLTGenerator2;
import fi.vm.yti.datamodel.api.v2.validator.ValidCrosswalk;
import fi.vm.yti.datamodel.api.v2.validator.ValidMapping;
import fi.vm.yti.security.AuthenticatedUserProvider;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletResponse;

@CrossOrigin(origins = "*")
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
    private final SchemaMapper schemaMapper;
	private final CrosswalkMapper mapper;
	private final MappingMapper mappingMapper;
	private final XSLTGenerator2 xsltGenerator;
	private final RMLGenerator2 rmlGenerator;
	private final SPARQLGenerator SPARQLGenerator;
	private final AuthenticatedUserProvider userProvider;
    private final GroupManagementService groupManagementService;
    private final CrosswalkService crosswalkService;


	public Crosswalk(AuthorizationManager authorizationManager,
            OpenSearchIndexer openSearchIndexer,
            PIDService PIDService,
            PostgresStorageService storageService,
            JenaService jenaService,
            SchemaMapper schemaMapper,
            CrosswalkMapper mapper,
            MappingMapper mappingMapper,
            XSLTGenerator2 xsltGenerator,
            RMLGenerator2 rmlGenerator,
            SPARQLGenerator SPARQLGenerator,
            AuthenticatedUserProvider userProvider,
            GroupManagementService groupManagementService,
            CrosswalkService crosswalkService) {
		this.openSearchIndexer = openSearchIndexer;
		this.authorizationManager = authorizationManager;
		this.PIDService = PIDService;
		this.storageService = storageService;		
		this.jenaService = jenaService;
		this.mapper = mapper;
		this.xsltGenerator = xsltGenerator;
		this.rmlGenerator = rmlGenerator;
		this.SPARQLGenerator = SPARQLGenerator;
		this.mappingMapper = mappingMapper;
		this.userProvider = userProvider;
		this.groupManagementService = groupManagementService;
		this.crosswalkService = crosswalkService;
		this.schemaMapper = schemaMapper;
	}
	
	private CrosswalkInfoDTO getCrosswalkDTO(String pid, boolean includeVersionInfo, CONTENT_ACTION action) throws Exception {
		pid = PIDService.mapToInternal(pid);
        var model = jenaService.getCrosswalk(pid);
        if(model == null){
            throw new ResourceNotFoundException(pid);
        }
        var hasRightsToModel = authorizationManager.hasRightToModelMSCR(pid, model);

        if(!Set.of(CONTENT_ACTION.mscrCopyOf, CONTENT_ACTION.copyOf).contains(action)) {
			check(hasRightsToModel);
			
		}                
        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;
        var ownerMapper = groupManagementService.mapOwner();

        return mapper.mapToCrosswalkDTO(pid, model, includeVersionInfo, false, userMapper, ownerMapper);
	}	
	
	private void createCrosswalkMetadata(final String PID, final String handle, CrosswalkDTO dto, String aggregationKey, String target, Model contentModel) {
		if(!dto.getOrganizations().isEmpty()) {
			check(authorizationManager.hasRightToAnyOrganization(dto.getOrganizations()));
		}
		checkVisibility(dto);	
		checkState(null, dto.getState());
		
		var ownerMapper = groupManagementService.mapOwner();
		Model sourceSchemaModel = jenaService.getSchema(dto.getSourceSchema());
		Model targetSchemaModel = jenaService.getSchema(dto.getTargetSchema());

		SchemaInfoDTO targetSchemaInfo = schemaMapper.mapToSchemaDTO(dto.getTargetSchema(), targetSchemaModel, null, ownerMapper);
		SchemaInfoDTO sourceSchemaInfo = schemaMapper.mapToSchemaDTO(dto.getSourceSchema(), sourceSchemaModel, null, ownerMapper);		
		
		String subType = getCrosswalkContentSubType(sourceSchemaInfo, targetSchemaInfo);
		
		Model jenaModel = mapper.mapToJenaModel(PID, handle, dto, target, aggregationKey, userProvider.getUser(), subType);
		if(!contentModel.isEmpty()) {
			jenaService.putToCrosswalk(PID+":content", contentModel);
		}
		jenaService.putToCrosswalk(PID, jenaModel);	
		
		// handle possible versioning data
		var crosswalkResource = jenaModel.createResource(PID);
		if(jenaModel.contains(crosswalkResource, MSCR.PROV_wasRevisionOf)) {			
			Model prevVersionModel = jenaService.getCrosswalk(target);
			Resource prevVersionResource = prevVersionModel.getResource(target);
			prevVersionResource.addProperty(MSCR.hasRevision, crosswalkResource);
			jenaService.updateCrosswalk(target, prevVersionModel);
			openSearchIndexer.updateCrosswalkToIndex(mapper.mapToIndexModel(target, prevVersionModel));
		}
		
		var indexModel = mapper.mapToIndexModel(PID, jenaModel);
        openSearchIndexer.createCrosswalkToIndex(indexModel);
	}
	
	private CrosswalkDTO mergeMetadata(CrosswalkInfoDTO prev, CrosswalkDTO input, CONTENT_ACTION action) {				
		CrosswalkDTO s = new CrosswalkDTO();
		s.setStatus(input != null && input.getStatus() != null ? input.getStatus() : prev.getStatus());
		s.setState(input != null && input.getState() != null ? input.getState() : prev.getState());
		s.setVisibility(input != null && input.getVisibility() != null ? input.getVisibility() : prev.getVisibility());
		s.setLabel(input != null && !input.getLabel().isEmpty()? input.getLabel() : prev.getLabel());
		s.setDescription(input != null && !input.getDescription().isEmpty() ? input.getDescription() : prev.getDescription());
		s.setLanguages(!input.getLanguages().isEmpty() ? input.getLanguages() : prev.getLanguages());
		if (action == CONTENT_ACTION.revisionOf || input == null || input.getOrganizations().isEmpty()) {
			s.setOrganizations(prev.getOrganizations().stream().map(org ->  UUID.fromString(org.getId())).collect(Collectors.toSet()));
		}	
		else {
			s.setOrganizations(input.getOrganizations());			
		}	
		s.setVersionLabel(input != null && input.getVersionLabel() != null ? input.getVersionLabel() : prev.getVersionLabel());
		s.setContact(input != null && input.getContact() != null ? input.getContact() : prev.getContact());
		s.setFormat(input != null && input.getFormat() != null ? input.getFormat() : prev.getFormat());
		if(action == CONTENT_ACTION.revisionOf) {
			s.setFormat(prev.getFormat());	
		}
		else if(action == CONTENT_ACTION.mscrCopyOf) { 
			s.setFormat(CrosswalkFormat.MSCR);
		}
		else {
			s.setFormat(input !=null && input.getFormat() != null ? input.getFormat() : prev.getFormat());
		}
		s.setSourceSchema(prev.getSourceSchema());
		s.setTargetSchema(prev.getTargetSchema());
		s.setSourceURL(input.getSourceURL());
		return s;
		
	}	
	
	private void addFileToCrosswalk(final String pid, final CrosswalkInfoDTO dto, final byte[] fileInBytes, final String contentURL,
			final String contentType) {	 
		try {
			Model contentModel = null;
			CrosswalkFormat format = dto.getFormat();
			if(format == CrosswalkFormat.SSSOM) {
				var userMapper = groupManagementService.mapUser();
				var ownerMapper = groupManagementService.mapOwner();

				
				Model sourceModel = jenaService.getSchemaContent(dto.getSourceSchema());
				Model targetModel = jenaService.getSchemaContent(dto.getTargetSchema());
				
				Model sourceSchemaModel = jenaService.getSchema(dto.getSourceSchema());
				Model targetSchemaModel = jenaService.getSchema(dto.getTargetSchema());

				SchemaInfoDTO targetSchemaInfo = schemaMapper.mapToSchemaDTO(dto.getTargetSchema(), targetSchemaModel, userMapper, ownerMapper);
				SchemaInfoDTO sourceSchemaInfo = schemaMapper.mapToSchemaDTO(dto.getSourceSchema(), sourceSchemaModel, userMapper, ownerMapper);
				
				contentModel = crosswalkService.transformSSSOMToInternal(pid, fileInBytes,  dto.getSourceSchema(), sourceSchemaInfo.getFormat().name(), sourceModel, dto.getTargetSchema(), targetSchemaInfo.getFormat().name(), targetModel);

			}
			else if(EnumSet.of(CrosswalkFormat.CSV, CrosswalkFormat.XSLT, CrosswalkFormat.PDF).contains(format)) {
				// do nothing
				contentModel = ModelFactory.createDefaultModel();
			}
			else {
				throw new Exception("Unsupported crosswalk description format. Supported formats are: " + String.join(", ", Arrays.toString(CrosswalkFormat.values()) ));
			}
			storageService.storeCrosswalkFile(pid, contentType, fileInBytes, generateFilename(pid, contentType));
			jenaService.putToCrosswalk(pid + ":content", contentModel);
			
		
		} catch (Exception ex) {
			throw new RuntimeException("Error occured while ingesting file based crosswalk description." + ex.getMessage(), ex);
		}
		
	}
	
	private byte[] validateFileUpload(byte[] fileInBytes, CrosswalkFormat format) {
		// TODO: file validation based on content
		return fileInBytes;
	}
	
	@Operation(summary = "Create crosswalk metadata record.")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path="/crosswalk", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public CrosswalkInfoDTO createCrosswalk(@ValidCrosswalk @RequestBody(required = false) CrosswalkDTO dto, @RequestParam(name = "action", required = false) CONTENT_ACTION action, @RequestParam(name = "target", required = false) String target) throws Exception {
		logger.info("Create Crosswalk {}", dto);
		validateActionParams(dto, action, target); 
		String aggregationKey = null;
		final String PID = "mscr:crosswalk:" + UUID.randomUUID();
		Model contentModel = ModelFactory.createDefaultModel();
		if(action != null) {			
			CrosswalkInfoDTO prev = getCrosswalkDTO(target, true, action);
			dto = mergeMetadata(prev, dto, action);			
			if(action == CONTENT_ACTION.revisionOf) {
				// revision must be made from the latest version
				if(prev.getRevisions() != null && prev.getRevisions().size() > 0 && !prev.getRevisions().get(prev.getRevisions().size() -1).getPid().equals(prev.getPID()) ) {
					throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Revisions can only be created from the latest revision. Check your target PID.");
				}
				aggregationKey = prev.getAggregationKey();
				if(prev.getFormat() == CrosswalkFormat.MSCR) {
					if(jenaService.doesCrosswalkExist(prev.getPID() + ":content")) {
						// This is very ugly temporary fix for the copying content 
						// TODO: clean up this monstrosity - use base uri for graphs
						Model tempModel = jenaService.getCrosswalkContent(prev.getPID());
						File tempFile = File.createTempFile("crosswalk", ".ttl");
						OutputStream tempOutput = new FileOutputStream(tempFile);
						tempModel.write(tempOutput, "TURTLE");
						String tempString = FileUtils.readFileToString(tempFile);						
						tempString = tempString.replaceAll(prev.getPID(), PID);
						FileUtils.write(tempFile, tempString); 
						contentModel = RDFDataMgr.loadModel(tempFile.toURI().toURL().toString(), Lang.TURTLE);
						tempOutput.close();
						tempFile.delete();
					}
					
				}
			}
			if(action == CONTENT_ACTION.mscrCopyOf) {
				if(!Set.of(CrosswalkFormat.MSCR, CrosswalkFormat.SSSOM, CrosswalkFormat.CSV).contains(prev.getFormat())) {
					throw new ResponseStatusException(HttpStatus.BAD_REQUEST, 
							"MSCR copy can only be made from a crosswalk with a format CSV, MSCR or SSSOM");
					
				}
				if(jenaService.doesCrosswalkExist(prev.getPID() + ":content")) {
					contentModel = jenaService.getCrosswalkContent(prev.getPID());					
				}
				
			}			
		}
		
		try {
			String handle = null;
			if(dto.getState() == MSCRState.PUBLISHED || dto.getState() == MSCRState.DEPRECATED) {
				 handle = PIDService.mint(PIDType.HANDLE, MSCRType.CROSSWALK, PID);
				
			}
			createCrosswalkMetadata(PID, handle, dto, aggregationKey, target, contentModel);
			var userMapper = groupManagementService.mapUser();
			var ownerMapper = groupManagementService.mapOwner();
			return mapper.mapToCrosswalkDTO(PID, jenaService.getCrosswalk(PID), false, false, userMapper, ownerMapper);
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
			var ownerMapper = groupManagementService.mapOwner();
			CrosswalkInfoDTO crosswalkDTO = mapper.mapToCrosswalkDTO(pid, model, userMapper, ownerMapper);
			
			if(!crosswalkDTO.getOrganizations().isEmpty()) {
				Collection<UUID> orgs = crosswalkDTO.getOrganizations().stream().map(org ->  UUID.fromString(org.getId())).toList();
				check(authorizationManager.hasRightToAnyOrganization(orgs));	
			}		
			
			addFileToCrosswalk(pid, crosswalkDTO, file.getBytes(), null, file.getContentType());
			return mapper.mapToCrosswalkDTO(pid, model, userMapper, ownerMapper);
		
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
			@RequestParam(name = "contentURL", required = false) String contentURL,
			@RequestParam(name = "file", required = false) MultipartFile file, @RequestParam(name = "action", required = false) CONTENT_ACTION action, @RequestParam(name = "target", required = false) String target) throws Exception {
		
		if (contentURL == null && file == null) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"Either file or contentURL parameter must be supplied.");
		}
		
		ObjectMapper objMapper = new ObjectMapper();
		CrosswalkDTO dto = null;
		try {
			dto = objMapper.readValue(metadataString, CrosswalkDTO.class);
			dto.setSourceURL(contentURL);
		} catch (JsonProcessingException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Could not parse CrosswalkDTO from the metadata content. " + e.getMessage(), e);
		}
		String contentType = "";
		byte[] fileBytes = null;
		try {
			if (file == null) {
				// try to download url to file
				File tempFile = File.createTempFile("crosswalk", "temp");
				FileUtils.copyURLToFile(new URL(contentURL), tempFile);
				fileBytes = validateFileUpload(FileUtils.readFileToByteArray(tempFile), dto.getFormat());
				contentType = "application/octet-stream"; // TODO: fix this
			} else {
				fileBytes = validateFileUpload(file.getBytes(), dto.getFormat());
				contentType = file.getContentType();
			}

		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
		}		
		
		CrosswalkInfoDTO infoDto = createCrosswalk(dto, action, target);
		final String PID = infoDto.getHandle() != null ? infoDto.getHandle() : infoDto.getPID();
		if(!dto.getOrganizations().isEmpty()) {
			Collection<UUID> orgs = dto.getOrganizations();
			check(authorizationManager.hasRightToAnyOrganization(orgs));

		}	
		try {
			addFileToCrosswalk(PID, infoDto, fileBytes, contentURL, contentType);
		}catch(Exception ex) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
		}
		var userMapper = groupManagementService.mapUser();
		var ownerMapper = groupManagementService.mapOwner();
		return mapper.mapToCrosswalkDTO(PID, jenaService.getCrosswalk(PID), false, false, userMapper, ownerMapper);
		
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
	        var ownerMapper = groupManagementService.mapOwner();
	        CrosswalkInfoDTO prev =  mapper.mapToCrosswalkDTO(pid, oldModel, false, false, userMapper, ownerMapper);        
	        dto = mergeMetadata(prev, dto, CONTENT_ACTION.update);		    
	        checkVisibility(dto);
	        checkState(prev, dto.getState());
	        Model jenaModel = null;
	        if(prev.getState() == MSCRState.DRAFT && dto.getState() == MSCRState.PUBLISHED) {
				try {
					String handle = PIDService.mint(PIDType.HANDLE, MSCRType.CROSSWALK, pid);
					jenaModel = mapper.mapToUpdateJenaModel(pid, handle, dto, oldModel, userProvider.getUser(), true, prev.getState() != dto.getState());	
				}catch(Exception ex) {
					throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Exception while geting a new handle for the schema." + ex.getMessage());
				}
			}
			else {
				jenaModel = mapper.mapToUpdateJenaModel(pid, null, dto, oldModel, userProvider.getUser(), true, prev.getState() != dto.getState());	
			}
	        
	
	        jenaService.putToCrosswalk(pid, jenaModel);
			// if state change must update the prev index model too 
			if(dto.getState() != prev.getState()) {
				if(prev.getRevisionOf() != null && !"".equals(prev.getRevisionOf())) {
					openSearchIndexer.updateCrosswalkToIndex(
						mapper.mapToIndexModel(prev.getRevisionOf(), 
								jenaService.getCrosswalk(prev.getRevisionOf())
						)						
					);					
				}
			}	
	
	        var indexModel = mapper.mapToIndexModel(pid, jenaModel);
	        openSearchIndexer.updateCrosswalkToIndex(indexModel);
	        CrosswalkInfoDTO updated = mapper.mapToCrosswalkDTO(pid, jenaModel, false, false, userMapper, ownerMapper);
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
	        var ownerMapper = groupManagementService.mapOwner();
	    	return ResponseEntity.ok(mapper.mapToCrosswalkDTO(pid, jenaModel, Boolean.parseBoolean(includeVersionInfo), true, userMapper, ownerMapper));
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
    public ResponseEntity<DeleteResponseDTO> deleteCrosswalk(@PathVariable String pid){
    	return deleteCrosswalk(pid, null);
    }
    
    @Hidden
    @SecurityRequirement(name = "Bearer Authentication")
    @DeleteMapping(value = "/crosswalk/{pid}/{suffix}")
    public ResponseEntity<DeleteResponseDTO> deleteCrosswalk(
    		@PathVariable String pid, 
    		@PathVariable(name = "suffix") String suffix){
		// TODO: get rid of this
    	if(pid.indexOf("@") > 0 || (suffix != null && suffix.indexOf("@") > 0)) {
    		return deleteMapping(pid, suffix);
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
			var ownerMapper = groupManagementService.mapOwner();
			CrosswalkInfoDTO prev = mapper.mapToCrosswalkDTO(pid, model, userMapper, ownerMapper);
			if(prev.getState() == MSCRState.DRAFT) {
				
				storageService.deleteAllCrosswalkFiles(internalID);
				jenaService.deleteFromCrosswalk(internalID);
				if(jenaService.doesCrosswalkExist(internalID+":content")) {
					jenaService.deleteFromCrosswalk(internalID+":content");
				}				
				openSearchIndexer.deleteCrosswalkFromIndex(internalID);
			}
			else {
				checkState(prev, MSCRState.REMOVED);
				CrosswalkDTO dto = new CrosswalkDTO();
				dto.setState(MSCRState.REMOVED);
				dto = mergeMetadata(prev, dto, CONTENT_ACTION.delete);
				var jenaModel = mapper.mapToUpdateJenaModel(pid, null, dto, ModelFactory.createDefaultModel(), userProvider.getUser(), false, true);
				var indexModel = mapper.mapToIndexModel(internalID, jenaModel);								
				jenaService.updateCrosswalk(internalID, jenaModel);
				if(jenaService.doesCrosswalkExist(internalID+":content")) {
					jenaService.deleteFromCrosswalk(internalID+":content");
				}				
				storageService.deleteAllCrosswalkFiles(internalID);
				openSearchIndexer.updateCrosswalkToIndex(indexModel);
			}
			if((prev.getRevisionOf() == null || prev.getRevisionOf().equals("")) && (prev.getHasRevisions() == null || prev.getHasRevisions().isEmpty()) ) {
				// do nothing
			}			
			// case - latest version was deleted = isrevision and !hasrevision
			else if(prev.getRevisionOf() != null && !prev.getRevisionOf().equals("") && (prev.getHasRevisions() == null || prev.getHasRevisions().isEmpty())) {
				// update the new latest 				
				String newLatestID = prev.getRevisionOf();
				var latestModel = jenaService.getCrosswalk(newLatestID);				
				var indexModel = mapper.mapToIndexModel(newLatestID, latestModel);
				openSearchIndexer.updateCrosswalkToIndex(indexModel);
			}
			// case - first version was deleted with revisions
			else if(prev.getRevisionOf() == null && prev.getHasRevisions() != null && !prev.getHasRevisions().isEmpty()) {
				// remove revision of from the nextVersion
				String nextRevision = prev.getHasRevisions().get(0); // should hold always with the condition above
				var versionModel = jenaService.getCrosswalk(nextRevision);		
				Resource versionResource = versionModel.getResource(nextRevision);
				versionResource.removeAll(MSCR.PROV_wasRevisionOf);				
				jenaService.putToCrosswalk(nextRevision, versionModel);
				var indexModel = mapper.mapToIndexModel(nextRevision, versionModel);
				openSearchIndexer.updateCrosswalkToIndex(indexModel);				
			}
			// case - in the middle
			else {
				String prevRevision = prev.getRevisionOf();
				String nextRevision = prev.getHasRevisions().get(0);
				
				var versionModel = jenaService.getCrosswalk(nextRevision);		
				Resource versionResource = versionModel.getResource(nextRevision);
				versionResource.removeAll(MSCR.PROV_wasRevisionOf);				
				versionResource.addProperty(MSCR.PROV_wasRevisionOf, versionModel.createResource(prevRevision));
				jenaService.putToCrosswalk(nextRevision, versionModel);
				var indexModel = mapper.mapToIndexModel(nextRevision, versionModel);
				openSearchIndexer.updateCrosswalkToIndex(indexModel);				
			}
			
			
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}
		return ResponseEntity.ok(new DeleteResponseDTO("ok", pid));
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
			var ownerMapper = groupManagementService.mapOwner();
			CrosswalkInfoDTO crosswalkInfo = mapper.mapToCrosswalkDTO(pid, jenaService.getCrosswalk(pid), null, ownerMapper);
			
	    	List<StoredFile> files = storageService.retrieveAllCrosswalkFiles(pid);
	    	return handleFileDownload(files, crosswalkInfo.getLabel().get("en") + "-" + crosswalkInfo.getVersionLabel(), crosswalkInfo.getFormat().name());

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
			var ownerMapper = groupManagementService.mapOwner();
			CrosswalkInfoDTO crosswalkInfo = mapper.mapToCrosswalkDTO(pid, jenaService.getCrosswalk(pid), null, ownerMapper);
			
	    	StoredFile file = storageService.retrieveFile(pid, Long.parseLong(fileID), MSCRType.CROSSWALK);
	    	if(file == null) {
	    		throw new ResourceNotFoundException(pid + "@file=" + fileID); 
	    	}
	    	return handleFileDownload(List.of(file), download, crosswalkInfo.getLabel().get("en") + "-" + crosswalkInfo.getVersionLabel(), crosswalkInfo.getFormat().name());
		
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
	public ResponseEntity<DeleteResponseDTO> deleteFile(@PathVariable String pid, @PathVariable Long fileID) throws Exception {
		return deleteFile(pid, null, fileID);
	}
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path="/crosswalk/{pid}/{suffix}/files/{fileID}", produces = APPLICATION_JSON_VALUE)
	public ResponseEntity<DeleteResponseDTO> deleteFile(
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
		return ResponseEntity.ok(new DeleteResponseDTO("ok", pid + ":" + fileID));
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
	        if(!jenaService.doesCrosswalkExist(pid)){
	            throw new ResourceNotFoundException(pid);
	        }	
	        Model metadataModel = jenaService.getCrosswalk(pid);
	        
	        check(authorizationManager.hasRightToModelMSCR(pid, metadataModel));
			if(!isEditable(metadataModel, pid)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Content can only be edited in the DRAFT state.");
			}
	        
			final String mappingID = UUID.randomUUID().toString();
			final String mappingPID = pid + "@mapping=" + mappingID;

			Model crosswalkModel = null;
			if(!jenaService.doesCrosswalkExist(pid+":content")) {
				crosswalkModel = ModelFactory.createDefaultModel();
			}
			else {
				crosswalkModel = jenaService.getCrosswalk(pid+":content");
			}
			Model mappingModel = mappingMapper.mapToJenaModel(mappingPID, mappingID, dto, pid);
			
			crosswalkModel.add(mappingModel);
			

			Resource crosswalkResource = crosswalkModel.getResource(pid);
			crosswalkResource.addProperty(MSCR.mappings, ResourceFactory.createResource(mappingPID));
			jenaService.putToCrosswalk(pid+":content", crosswalkModel);
			return mappingMapper.mapToMappingDTO(null, mappingPID, mappingModel);
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
			String crosswalkPID = mappingPID.substring(0, mappingPID.indexOf("@"));
			String mappingID = mappingPID.substring(mappingPID.indexOf("=") + 1);			
	        var metadataModel = jenaService.getCrosswalk(crosswalkPID);
	        if(metadataModel == null){
	            throw new ResourceNotFoundException(crosswalkPID);
	        }
	        check(authorizationManager.hasRightToModelMSCR(crosswalkPID, metadataModel));
			if(!isEditable(metadataModel, crosswalkPID)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Content can only be edited in the DRAFT state.");
			}
			Model crosswalkModel = jenaService.getCrosswalkContent(crosswalkPID);
			if(!crosswalkModel.contains(ResourceFactory.createResource(mappingPID), RDF.type, MSCR.MAPPING)) {
				throw new ResourceNotFoundException(mappingPID);
			}
			jenaService.deleteMapping(crosswalkPID, mappingPID, crosswalkModel, false);
			Model mappingModel = mappingMapper.mapToJenaModel(mappingPID, mappingID, dto, crosswalkPID);
			
			crosswalkModel.add(mappingModel);
			jenaService.putToCrosswalk(crosswalkPID+":content", crosswalkModel);
			return mappingMapper.mapToMappingDTO(null, mappingPID, mappingModel);
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
			String crosswalkPID = mappingPID.substring(0, mappingPID.indexOf("@"));
	        var metadataModel = jenaService.getCrosswalk(crosswalkPID);
	        if(metadataModel == null){
	            throw new ResourceNotFoundException(crosswalkPID);
	        }		
	        var ownerMapper = groupManagementService.mapOwner();			        
	        CrosswalkInfoDTO crosswalkDTO = mapper.mapToCrosswalkDTO(crosswalkPID, metadataModel, null, ownerMapper);
	        
	        Model crosswalkModel = jenaService.getCrosswalk(crosswalkPID+":content");
			
	        if(!crosswalkModel.contains(crosswalkModel.createResource(mappingPID), RDF.type, MSCR.MAPPING)) {
	            throw new ResourceNotFoundException(mappingPID);
	        }
			
			return ResponseEntity.ok().body(mappingMapper.mapToMappingDTO(crosswalkDTO.getHandle(), mappingPID, crosswalkModel));
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
	public ResponseEntity<DeleteResponseDTO> deleteMapping(@PathVariable String mappingPID) {
		return deleteMapping(mappingPID, null);
	}
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path="/crosswalk/{mappingPID}/{suffix}", produces = APPLICATION_JSON_VALUE)
	public ResponseEntity<DeleteResponseDTO> deleteMapping(
			@PathVariable String mappingPID,
			@PathVariable String suffix) {
    	if(mappingPID.indexOf("@") < 0) {
    		return deleteCrosswalk(mappingPID, null);
    	}		
		if (suffix != null) {
			mappingPID = mappingPID + "/" + suffix;
		}
		try {
			mappingPID = PIDService.mapToInternal(mappingPID);		
			logger.info("Delete Mapping {}", mappingPID);
			String crosswalkPID = mappingPID.substring(0, mappingPID.indexOf("@"));
	        var metadataModel = jenaService.getCrosswalk(crosswalkPID);
	        if(metadataModel == null){
	            throw new ResourceNotFoundException(crosswalkPID);
	        }			
	        check(authorizationManager.hasRightToModelMSCR(crosswalkPID, metadataModel));
			if(!isEditable(metadataModel, crosswalkPID)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Content can only be edited in the DRAFT state.");
			}
			
			jenaService.deleteMapping(crosswalkPID, mappingPID);
		} catch (RuntimeException rex) {
			rex.printStackTrace();
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}
		return ResponseEntity.ok(new DeleteResponseDTO("ok", mappingPID));
	}
		
	@Operation(summary = "Get a mappings for a crosswalk")
	@ApiResponse(responseCode = "200")	
	@GetMapping(path="/crosswalk/{pid}/mapping")
	public ResponseEntity<Object> getMappings(
			final HttpServletResponse response,
			@PathVariable String pid, @RequestParam(name = "exportFormat", required = false) String exportFormat,
			@RequestParam(name = "includeSource", required = false) String includeSource,			
			@RequestParam(name = "includeTarget", required = false) String includeTarget			
			
			) {
		return getMappings(response, pid, null, exportFormat, includeSource, includeTarget); 
	}

	@Hidden
	@ApiResponse(responseCode = "200")	
	@GetMapping(path="/crosswalk/{pid}/{suffix}/mapping")
	public ResponseEntity<Object> getMappings(
			final HttpServletResponse response,
			@PathVariable String pid, 
			@PathVariable String suffix, 
			@RequestParam(name = "exportFormat", required = false) String exportFormat,
			@RequestParam(name = "includeSource", required = false) String includeSource,			
			@RequestParam(name = "includeTarget", required = false) String includeTarget			
			
			) {
		
		// TODO: check that crosswalk exists
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			logger.info("Get Mappings for crosswalk {}", pid);
			pid = PIDService.mapToInternal(pid);	
			if(!jenaService.doesCrosswalkExist(pid)) {
				throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Crosswalk with pid " + pid + " not found");
			}
			Model crosswalkModel = null;
			if(!jenaService.doesCrosswalkExist(pid+":content")) {
				crosswalkModel = ModelFactory.createDefaultModel();
			}
			else {
				crosswalkModel = jenaService.getCrosswalk(pid+":content");
			}
			Model crosswalkMetadataModel = jenaService.getCrosswalk(pid);
	        var ownerMapper = groupManagementService.mapOwner();			        
			CrosswalkInfoDTO crosswalk = mapper.mapToCrosswalkDTO(pid, crosswalkMetadataModel, null, ownerMapper);
			Model targetSchemaContent = jenaService.getSchemaContent(crosswalk.getTargetSchema());
			List<MappingInfoDTO> mappings = new ArrayList<MappingInfoDTO>();
			String q = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
					+ "PREFIX mscr: <http://uri.suomi.fi/datamodel/ns/mscr#>\n"
					+ "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"
					+ "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"
					+ "PREFIX dcterms: <http://purl.org/dc/terms/>\n"
					+ "PREFIX sh: <http://www.w3.org/ns/shacl#>\n"
					+ "select ?mapping\n"
					+ "where {\n"
					+ "  <" + pid + "> mscr:mappings ?mapping .\n"
					+ "  ?mapping mscr:target/rdf:_1/mscr:uri ?prop .\n"
					+ "  OPTIONAL { ?prop sh:order ?order } .\n"
					+ "  OPTIONAL { ?prop mscr:depth ?depth } .\n"
					+ "  OPTIONAL {?prop sh:name ?name }\n"
					+ "} order by ASC(str(?prop)) ASC(?depth)";
					//+ "} order by ASC(?depth) ASC(?order) ASC(?name)";
			Model queryModel = ModelFactory.createDefaultModel();
			queryModel.add(crosswalkModel);
			queryModel.add(targetSchemaContent);
			QueryExecution qexec = QueryExecutionFactory.create(q, queryModel);
			ResultSet mapi = qexec.execSelect();
			while(mapi.hasNext()) {
//			NodeIterator i = crosswalkModel.listObjectsOfProperty(crosswalkModel.getResource(pid), MSCR.mappings);
//			while(i.hasNext()) {
				QuerySolution soln = mapi.next();
				Resource mappingResource = soln.getResource("mapping");			
				MappingInfoDTO dto = mappingMapper.mapToMappingDTO(
						crosswalk.getHandle(),
						mappingResource.getURI(), 
						crosswalkModel);
				mappings.add(dto);
			}
			if(exportFormat !=null) {
				Model sourceSchemaModel = jenaService.getSchema(crosswalk.getSourceSchema());
				Model targetSchemaModel = jenaService.getSchema(crosswalk.getTargetSchema());

				SchemaInfoDTO targetSchemaInfo = schemaMapper.mapToSchemaDTO(crosswalk.getTargetSchema(), targetSchemaModel, null, ownerMapper);
				SchemaInfoDTO sourceSchemaInfo = schemaMapper.mapToSchemaDTO(crosswalk.getSourceSchema(), sourceSchemaModel, null, ownerMapper);
				
				// TODO: check for crosswalk format == MSCR and source and target scheama format == X,Y,Z
				if(exportFormat.equalsIgnoreCase("skos")) {
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
	
					if(includeSource != null && includeSource.equals("true")) {
						model.add(jenaService.getSchemaContent(sourceSchemaInfo.getPID()));
					}
					if(includeTarget != null && includeTarget.equals("true")) {
						model.add(jenaService.getSchemaContent(targetSchemaInfo.getPID()));
					}
					model.write(writer, "TURTLE");
					writer.flush();
					String outputStr = writer.toString();
					writer.close();
					response.setContentType("text/turtle");
					return ResponseEntity.ok(outputStr);
				}
				else if(exportFormat.equalsIgnoreCase("xslt")) {
					
					if((sourceSchemaInfo.getFormat() == SchemaFormat.XSD || sourceSchemaInfo.getOriginalFormat() == SchemaFormat.XSD) && (targetSchemaInfo.getFormat() == SchemaFormat.XSD || targetSchemaInfo.getOriginalFormat() == SchemaFormat.XSD)) {
						String r = xsltGenerator.generateXMLtoXML(crosswalk.getSourceSchema(),jenaService.getSchemaContent(crosswalk.getSourceSchema()), crosswalkModel, jenaService.getSchemaContent(crosswalk.getTargetSchema()));
						return ResponseEntity.status(200).contentType(MediaType.APPLICATION_XML).body(r);						
					}
					if((sourceSchemaInfo.getFormat() == SchemaFormat.JSONSCHEMA || sourceSchemaInfo.getOriginalFormat() == SchemaFormat.JSONSCHEMA) && (targetSchemaInfo.getFormat() == SchemaFormat.XSD || targetSchemaInfo.getOriginalFormat() == SchemaFormat.XSD)) {
						String r = xsltGenerator.generateJSONtoXML(crosswalk.getSourceSchema(),jenaService.getSchemaContent(crosswalk.getSourceSchema()), crosswalkModel, jenaService.getSchemaContent(crosswalk.getTargetSchema()));
						return ResponseEntity.status(200).contentType(MediaType.APPLICATION_XML).body(r);						
					}
					if((sourceSchemaInfo.getFormat() == SchemaFormat.JSONSCHEMA || sourceSchemaInfo.getOriginalFormat() == SchemaFormat.JSONSCHEMA) && (targetSchemaInfo.getFormat() == SchemaFormat.JSONSCHEMA || targetSchemaInfo.getOriginalFormat() == SchemaFormat.JSONSCHEMA)) {
						String r = xsltGenerator.generateJSONtoJSON(crosswalk.getSourceSchema(),jenaService.getSchemaContent(crosswalk.getSourceSchema()), crosswalkModel, jenaService.getSchemaContent(crosswalk.getTargetSchema()));
						return ResponseEntity.status(200).contentType(MediaType.APPLICATION_JSON).body(r);						
					}
					if((sourceSchemaInfo.getFormat() == SchemaFormat.XSD || sourceSchemaInfo.getOriginalFormat() == SchemaFormat.XSD) && (targetSchemaInfo.getFormat() == SchemaFormat.JSONSCHEMA || targetSchemaInfo.getOriginalFormat() == SchemaFormat.JSONSCHEMA)) {
						String r = xsltGenerator.generateXMLtoJSON(crosswalk.getSourceSchema(),jenaService.getSchemaContent(crosswalk.getSourceSchema()), crosswalkModel, jenaService.getSchemaContent(crosswalk.getTargetSchema()));
						return ResponseEntity.status(200).contentType(MediaType.APPLICATION_JSON).body(r);						
					}
					if((sourceSchemaInfo.getFormat() == SchemaFormat.XSD || sourceSchemaInfo.getOriginalFormat() == SchemaFormat.XSD) && (targetSchemaInfo.getFormat() == SchemaFormat.CSV || targetSchemaInfo.getOriginalFormat() == SchemaFormat.CSV)) {
						String r = xsltGenerator.generateXMLtoCSV(crosswalk.getSourceSchema(),jenaService.getSchemaContent(crosswalk.getSourceSchema()), crosswalkModel, jenaService.getSchemaContent(crosswalk.getTargetSchema()));
						return ResponseEntity.status(200).contentType(MediaType.TEXT_PLAIN).body(r);						
					}
					if((sourceSchemaInfo.getFormat() == SchemaFormat.JSONSCHEMA || sourceSchemaInfo.getOriginalFormat() == SchemaFormat.JSONSCHEMA) && (targetSchemaInfo.getFormat() == SchemaFormat.CSV || targetSchemaInfo.getOriginalFormat() == SchemaFormat.CSV)) {						
						String r = xsltGenerator.generateJSONtoCSV(crosswalk.getSourceSchema(),jenaService.getSchemaContent(crosswalk.getSourceSchema()), crosswalkModel, jenaService.getSchemaContent(crosswalk.getTargetSchema()));
						return ResponseEntity.status(200).contentType(MediaType.TEXT_PLAIN).body(r);						
					}
					if((sourceSchemaInfo.getFormat() == SchemaFormat.CSV || sourceSchemaInfo.getOriginalFormat() == SchemaFormat.CSV) && (targetSchemaInfo.getFormat() == SchemaFormat.CSV || targetSchemaInfo.getOriginalFormat() == SchemaFormat.CSV)) {						
						String r = xsltGenerator.generateCSVtoCSV(crosswalk.getSourceSchema(),jenaService.getSchemaContent(crosswalk.getSourceSchema()), crosswalkModel, jenaService.getSchemaContent(crosswalk.getTargetSchema()));
						return ResponseEntity.status(200).contentType(MediaType.TEXT_PLAIN).body(r);						
					}
					if((sourceSchemaInfo.getFormat() == SchemaFormat.CSV || sourceSchemaInfo.getOriginalFormat() == SchemaFormat.CSV) && (targetSchemaInfo.getFormat() == SchemaFormat.JSONSCHEMA || targetSchemaInfo.getOriginalFormat() == SchemaFormat.JSONSCHEMA)) {						
						String r = xsltGenerator.generateCSVtoJSON(crosswalk.getSourceSchema(),jenaService.getSchemaContent(crosswalk.getSourceSchema()), crosswalkModel, jenaService.getSchemaContent(crosswalk.getTargetSchema()));
						return ResponseEntity.status(200).contentType(MediaType.APPLICATION_JSON).body(r);						
					}
					if((sourceSchemaInfo.getFormat() == SchemaFormat.CSV || sourceSchemaInfo.getOriginalFormat() == SchemaFormat.CSV) && (targetSchemaInfo.getFormat() == SchemaFormat.XSD || targetSchemaInfo.getOriginalFormat() == SchemaFormat.XSD)) {						
						String r = xsltGenerator.generateCSVtoXML(crosswalk.getSourceSchema(),jenaService.getSchemaContent(crosswalk.getSourceSchema()), crosswalkModel, jenaService.getSchemaContent(crosswalk.getTargetSchema()));
						return ResponseEntity.status(200).contentType(MediaType.APPLICATION_XML).body(r);						
					}
					
					throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "XSLT export is not supported between " + sourceSchemaInfo.getFormat() + " and " + targetSchemaInfo.getFormat());
							
					
				}
				else if(exportFormat.equalsIgnoreCase("rml")) {
					List<SchemaFormat> acceptedSources = List.of(SchemaFormat.CSV, SchemaFormat.XSD, SchemaFormat.JSONSCHEMA);
					List<SchemaFormat> acceptedTargets = List.of(SchemaFormat.RDFS, SchemaFormat.SHACL);
					if(!(
							(acceptedSources.contains(sourceSchemaInfo.getFormat()) || acceptedSources.contains(sourceSchemaInfo.getOriginalFormat()))
									&&
							(acceptedTargets.contains(targetSchemaInfo.getFormat()) || acceptedTargets.contains(targetSchemaInfo.getOriginalFormat()))
							
						)) {
						throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "RML generator is only available for crosswalk that are between CSV/XSD/JsonSchema and SHACL/RDFS schemas. Current source schema format " + sourceSchemaInfo.getFormat() + " and target schema format " + targetSchemaInfo.getFormat()); 
						
					}
					Model sourceModel = jenaService.getSchemaContent(crosswalk.getSourceSchema());
					Model targetModel = jenaService.getSchemaContent(crosswalk.getTargetSchema());
					Model rmlGeneratorInputModel = ModelFactory.createDefaultModel();
					rmlGeneratorInputModel.add(crosswalkModel);
					rmlGeneratorInputModel.add(sourceModel);
					rmlGeneratorInputModel.add(targetModel);
					rmlGeneratorInputModel.add(sourceSchemaModel);
					rmlGeneratorInputModel.add(targetSchemaModel);
					
					Model rmlModel = rmlGenerator.generateRMLFromMSCRGraph(rmlGeneratorInputModel, crosswalk.getPID(), crosswalk.getSourceSchema());
					StringWriter out = new StringWriter();
					rmlModel.write(out, "TURTLE");
					return ResponseEntity.status(200).contentType(MediaType.APPLICATION_JSON).body(out.toString());
				}
				else if(exportFormat.equalsIgnoreCase("sparql")) {
					if(sourceSchemaInfo.getFormat() == SchemaFormat.SHACL && targetSchemaInfo.getFormat() == SchemaFormat.CSV) {
						
						String r = SPARQLGenerator.generateRDFtoCSV(pid, mappings, crosswalkModel, 
								jenaService.getSchemaContent(crosswalk.getSourceSchema()), 
								jenaService.getSchemaContent(crosswalk.getTargetSchema()));
						return ResponseEntity.status(200).contentType(MediaType.TEXT_PLAIN).body(r);						
					}					
				}
				else if(exportFormat.equalsIgnoreCase("sssom")) {
					Set<SchemaFormat> acceptedFormat = Set.of(SchemaFormat.ENUM, SchemaFormat.SKOSRDF, SchemaFormat.OWL, SchemaFormat.RDFS);
					if(
						acceptedFormat.contains(sourceSchemaInfo.getFormat() == SchemaFormat.MSCR ? sourceSchemaInfo.getOriginalFormat() : sourceSchemaInfo.getFormat())
						&&
						acceptedFormat.contains(targetSchemaInfo.getFormat() == SchemaFormat.MSCR ? targetSchemaInfo.getOriginalFormat() : targetSchemaInfo.getFormat())
							
						) {
						String sssom = crosswalkService.exportAsSSSOM(crosswalk, mappings, sourceSchemaInfo, targetSchemaInfo);
						return ResponseEntity.status(200).contentType(MediaType.valueOf("text/plain;charset=UTF-8")).body(sssom);
						
					}
				}
				else if(exportFormat.equalsIgnoreCase("mscr")) {					
					crosswalkModel.write(response.getWriter(), "TURTLE");
					return ResponseEntity.status(200).contentType(MediaType.valueOf("text/turtle;charset=UTF-8")).build();
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
