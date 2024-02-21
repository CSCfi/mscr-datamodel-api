package fi.vm.yti.datamodel.api.v2.endpoint;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MSCRState;
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.MSCRVisibility;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.dto.SchemaDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.SchemaMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.service.GroupManagementService;
import fi.vm.yti.datamodel.api.v2.service.JSONValidationService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.PIDService;
import fi.vm.yti.datamodel.api.v2.service.SchemaService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFile;
import fi.vm.yti.datamodel.api.v2.service.ValidationRecord;
import fi.vm.yti.datamodel.api.v2.service.impl.PostgresStorageService;
import fi.vm.yti.datamodel.api.v2.validator.ValidSchema;
import fi.vm.yti.security.AuthenticatedUserProvider;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("v2")
@Tag(name = "Schema")
@Validated
public class Schema extends BaseMSCRController {
	

	private static final Logger logger = LoggerFactory.getLogger(Schema.class);

	private final AuthorizationManager authorizationManager;

	private final OpenSearchIndexer openSearchIndexer;

	private final JenaService jenaService;

	private final SchemaMapper mapper;

	private final SchemaService schemaService;

	private final PIDService PIDService;
	
	private final StorageService storageService;	
	
	private final AuthenticatedUserProvider userProvider;
	
    private final GroupManagementService groupManagementService;
    
	public Schema(JenaService jenaService,
            AuthorizationManager authorizationManager,
            OpenSearchIndexer openSearchIndexer,
            SchemaMapper schemaMapper,
            SchemaService schemaService,
            PIDService PIDService,
            PostgresStorageService storageService,
            AuthenticatedUserProvider userProvider,
            GroupManagementService groupManagementService) {
		
		this.jenaService = jenaService;
		this.openSearchIndexer = openSearchIndexer;
		this.authorizationManager = authorizationManager;
		this.mapper = schemaMapper;
		this.schemaService = schemaService;
		this.PIDService = PIDService;
		this.storageService = storageService;
		this.userProvider = userProvider;
		this.groupManagementService = groupManagementService;		
	}
	
	private void validateFileUpload(MultipartFile file, SchemaFormat format) {
		try {
			byte[] fileInBytes = file.getBytes();
			
			if (format == SchemaFormat.JSONSCHEMA) {
				JsonNode jsonObj = schemaService.parseSchema(new String(fileInBytes));
				ValidationRecord validationRecord = JSONValidationService.validateJSONSchema(jsonObj);

				boolean isValidJSONSchema = validationRecord.isValid();
				List<String> validationMessages = validationRecord.validationOutput();

				if (!isValidJSONSchema) {
					String exceptionOutput = String.join("\n", validationMessages);
					throw new Exception(exceptionOutput);
				}

			} else if (format == SchemaFormat.XSD || 
					format == SchemaFormat.XML || 
					format == SchemaFormat.CSV ||
					format == SchemaFormat.SKOSRDF ||
					format == SchemaFormat.PDF) {
				// do nothing for now
			}			
			else {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, String.format("Unsupported schema description format: %s not supported",
						format));
			}			

		} catch(ResponseStatusException statusex) {
			throw statusex;
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occured while ingesting file based schema description. " + ex.getMessage(), ex);
		}		
		
	}
	
	private SchemaInfoDTO addFileToSchema(SchemaInfoDTO schemaDTO, MultipartFile file) {
		var userMapper = groupManagementService.mapUser();
		String contentType = file.getContentType();
		final String pid = schemaDTO.getPID();
		Model metadataModel = jenaService.getSchema(pid);		
		try {
			byte[] fileInBytes = file.getBytes();
			Model schemaModel = null;
			
			if (schemaDTO.getFormat() == SchemaFormat.JSONSCHEMA) {
				JsonNode jsonObj = schemaService.parseSchema(new String(fileInBytes));
				ValidationRecord validationRecord = JSONValidationService.validateJSONSchema(jsonObj);

				boolean isValidJSONSchema = validationRecord.isValid();
				List<String> validationMessages = validationRecord.validationOutput();

				if (isValidJSONSchema) {
					schemaModel = schemaService.transformJSONSchemaToInternal(pid, jsonObj);
				} else {
					String exceptionOutput = String.join("\n", validationMessages);
					throw new Exception(exceptionOutput);
				}

			}else if (schemaDTO.getFormat() == SchemaFormat.CSV) {
				schemaModel = schemaService.transformCSVSchemaToInternal(pid, fileInBytes, ";");
				
			}else if(schemaDTO.getFormat() == SchemaFormat.SKOSRDF) {
				// TODO: validate skos file
				schemaModel = schemaService.addSKOSVocabulary(pid, fileInBytes);				
			}else if(schemaDTO.getFormat() == SchemaFormat.PDF) {
				// do nothing
				schemaModel = ModelFactory.createDefaultModel();
			}else if(schemaDTO.getFormat() == SchemaFormat.XSD) {
				schemaModel = schemaService.transformXSDToInternal(pid, fileInBytes);
						
			} else if(schemaDTO.getFormat() == SchemaFormat.XML) {
				// do nothing
				schemaModel = ModelFactory.createDefaultModel();				
			} else {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, String.format("Unsupported schema description format: %s not supported",
						schemaDTO.getFormat()));
			}
			schemaModel.add(metadataModel);
			jenaService.updateSchema(pid, schemaModel);
			storageService.storeSchemaFile(pid, contentType, file.getBytes(), generateFilename(pid, file));
			
		} catch(ResponseStatusException statusex) {
			throw statusex;		
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occured while ingesting file based schema description", ex);
		}
		return mapper.mapToSchemaDTO(pid, metadataModel, userMapper);
	}
	
	private SchemaDTO mergeSchemaMetadata(SchemaInfoDTO prevSchema, SchemaDTO inputSchema, boolean isRevision) {
		SchemaDTO s = new SchemaDTO();
		// in case of revision the following data cannot be overridden
		// - organization
		s.setStatus(inputSchema != null && inputSchema.getStatus() != null ? inputSchema.getStatus() : prevSchema.getStatus());
		s.setState(inputSchema != null && inputSchema.getState() != null ? inputSchema.getState() : prevSchema.getState());
		s.setVisibility(inputSchema != null && inputSchema.getVisibility() != null ? inputSchema.getVisibility() : prevSchema.getVisibility());
		s.setLabel(!inputSchema.getLabel().isEmpty()? inputSchema.getLabel() : prevSchema.getLabel());
		s.setDescription(inputSchema != null && !inputSchema.getDescription().isEmpty() ? inputSchema.getDescription() : prevSchema.getDescription());
		s.setLanguages(inputSchema != null && !inputSchema.getLanguages().isEmpty() ? inputSchema.getLanguages() : prevSchema.getLanguages());
		s.setNamespace(inputSchema != null && inputSchema.getNamespace() != null ? inputSchema.getNamespace() : prevSchema.getNamespace());
		s.setContact(inputSchema != null && inputSchema.getContact() != null ? inputSchema.getContact() : prevSchema.getContact());
		if(isRevision || inputSchema == null || inputSchema.getOrganizations().isEmpty()) {
			s.setOrganizations(prevSchema.getOrganizations().stream().map(org ->  UUID.fromString(org.getId())).collect(Collectors.toSet()));
		}	
		else {
			s.setOrganizations(inputSchema.getOrganizations());			
		}	
		s.setVersionLabel(inputSchema != null && inputSchema.getVersionLabel() != null ? inputSchema.getVersionLabel() : "");
		s.setFormat(inputSchema != null && inputSchema.getFormat() != null ? inputSchema.getFormat() : prevSchema.getFormat());
		return s;
		
	}
	
	private SchemaInfoDTO getSchemaDTO(String pid, boolean includeVersionInfo, boolean includeVariantInfo) {
        var model = jenaService.getSchema(pid);
        if(model == null){
            throw new ResourceNotFoundException(pid);
        }
        var hasRightsToModel = authorizationManager.hasRightToModelMSCR(pid, model);
        
        check(hasRightsToModel);
        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;

        return mapper.mapToSchemaDTO(pid, model, includeVersionInfo, includeVariantInfo, userMapper);
	}

	

	private void validateActionParams(SchemaDTO dto, CONTENT_ACTION action, String target) {
		if(dto == null && action == null) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Request body must present if no action is provided.");
		}
		if(action ==null && target != null) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Target parameter requires an action.");
		}
		if(action !=null && target == null) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Action parameter requires a target");
		}		
	}
	
	@Operation(summary = "Create schema metadata")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schema", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO createSchema(@ValidSchema() @RequestBody(required = false) SchemaDTO schemaDTO, @RequestParam(name = "action", required = false) CONTENT_ACTION action, @RequestParam(name = "target", required = false) String target) {
		
		validateActionParams(schemaDTO, action, target);
		checkVisibility(schemaDTO);
		checkState(null, schemaDTO);
		
		String aggregationKey = null;
		if(action != null) {			
			SchemaInfoDTO prevSchema = getSchemaDTO(target, true, false);
			schemaDTO = mergeSchemaMetadata(prevSchema, schemaDTO, action == CONTENT_ACTION.revisionOf);			
			if(action == CONTENT_ACTION.revisionOf) {
				// revision must be made from the latest version
				if(prevSchema.getHasRevisions() != null && !prevSchema.getHasRevisions().isEmpty()) {
					throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Revisions can only be created from the latest revision. Check your target PID.");
				}
				aggregationKey = prevSchema.getAggregationKey();
				
			}
		}
		logger.info("Create Schema {}", schemaDTO);
		if(!schemaDTO.getOrganizations().isEmpty()) {
			check(authorizationManager.hasRightToAnyOrganization(schemaDTO.getOrganizations()));	
		}
			
		final String PID = PIDService.mint(PIDType.HANDLE);
		try {
			var jenaModel = mapper.mapToJenaModel(PID, schemaDTO, target, aggregationKey, userProvider.getUser());
			jenaService.putToSchema(PID, jenaModel);
			
			// handle possible versioning data
			var schemaResource = jenaModel.createResource(PID);
			if(jenaModel.contains(schemaResource, MSCR.PROV_wasRevisionOf)) {			
				Model prevVersionModel = jenaService.getSchema(target);
				Resource prevVersionResource = prevVersionModel.getResource(target);
				prevVersionResource.addProperty(MSCR.hasRevision, schemaResource);
				jenaService.updateSchema(target, prevVersionModel);
				
			}
			var indexModel = mapper.mapToIndexModel(PID, jenaModel);
	        openSearchIndexer.createSchemaToIndex(indexModel);
	        var userMapper = groupManagementService.mapUser();
	
	        return mapper.mapToSchemaDTO(PID, jenaService.getSchema(PID), userMapper);
		}catch(Exception ex) {
			// revert any possible changes
			try { jenaService.deleteFromSchema(PID); }catch(Exception _ex) { logger.error(_ex.getMessage(), _ex);}
			try { openSearchIndexer.deleteSchemaFromIndex(PID);}catch(Exception _ex) { logger.error(_ex.getMessage(), _ex);}
			if( (ex instanceof ResponseStatusException) || (ex instanceof MappingError)) {
				throw ex;
			}
			else {
				throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Unknown error occured. " + ex.getMessage(), ex);
			}
		}        
				
	}
    
	@Operation(summary = "Upload and associate a schema description file to an existing schema")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schema/{pid}/upload", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public SchemaInfoDTO uploadSchemaFile(@PathVariable String pid, @RequestParam("file") MultipartFile file, boolean isFull) {
		try {
			// check for auth here because addFileToSchema is not doing it
			var model = jenaService.getSchema(pid);
			if(!isEditable(model, pid)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Content can only be edited in the DRAFT state.");
			}
			
	        var userMapper = groupManagementService.mapUser();
			SchemaInfoDTO schemaDTO = mapper.mapToSchemaDTO(pid, model, userMapper);

			if(!schemaDTO.getOrganizations().isEmpty()) {
				Collection<UUID> orgs = schemaDTO.getOrganizations().stream().map(org ->  UUID.fromString(org.getId())).toList();
				check(authorizationManager.hasRightToAnyOrganization(orgs));	
			}									
			return addFileToSchema(schemaDTO, file);
		}catch(Exception ex) {
			// revert any possible changes
			try {storageService.deleteAllSchemaFiles(pid);}catch(Exception _ex) { logger.error(_ex.getMessage(), _ex);}
			if( (ex instanceof ResponseStatusException) || (ex instanceof MappingError)) {
				throw ex;
			}
			else {
				throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Unknown error occured. " + ex.getMessage(), ex);
			}
		}
	}
	
	@Operation(summary = "Create schema by uploading metadata and files in one multipart request")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schemaFull", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public SchemaInfoDTO createSchemaFull(@RequestParam("metadata") String metadataString,
			@RequestParam("file") MultipartFile file, @RequestParam(name = "action", required = false) CONTENT_ACTION action, @RequestParam(name = "target", required = false) String target) {		
		
		ObjectMapper objMapper = new ObjectMapper();
		SchemaDTO schemaDTO = null;
		try {
			schemaDTO = objMapper.readValue(metadataString, SchemaDTO.class);
		} catch (JsonProcessingException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Could not parse SchemaDTO from the metadata content. " + e.getMessage(), e);
		}
		try {
			validateFileUpload(file, schemaDTO.getFormat());
		}catch(Exception ex) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
		}
		SchemaInfoDTO dto = createSchema(schemaDTO, action, target);
		final String PID = dto.getPID();
		uploadSchemaFile(PID, file, true);
		var userMapper = groupManagementService.mapUser();
		return mapper.mapToSchemaDTO(PID, jenaService.getSchema(PID), userMapper);
	}
  
    @Operation(summary = "Modify schema")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new schema node")
    @ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
    @PatchMapping(path = "/schema/{pid}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public SchemaInfoDTO updateModel(@RequestBody SchemaDTO schemaDTO,
                            @PathVariable String pid) {
        logger.info("Updating schema {}", schemaDTO);

        var oldModel = jenaService.getSchema(pid);
        if(oldModel == null){
            throw new ResourceNotFoundException(pid);
        }
        check(authorizationManager.hasRightToModelMSCR(pid, oldModel));        
        var userMapper = groupManagementService.mapUser();
        SchemaInfoDTO prevSchema =  mapper.mapToSchemaDTO(pid, oldModel, false, false, userMapper);        
        schemaDTO = mergeSchemaMetadata(prevSchema, schemaDTO, false);		
		checkVisibility(schemaDTO);
		checkState(prevSchema, schemaDTO);
        var jenaModel = mapper.mapToUpdateJenaModel(pid, schemaDTO, oldModel, userProvider.getUser());

        jenaService.putToSchema(pid, jenaModel);


        var indexModel = mapper.mapToIndexModel(pid, jenaModel);
        openSearchIndexer.updateSchemaToIndex(indexModel);
        return mapper.mapToSchemaDTO(pid, jenaModel, false, false, userMapper);
    }

    
    
    @Operation(summary = "Get a schema metadata")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(value = "/schema/{pid}", produces = APPLICATION_JSON_VALUE)
    public SchemaInfoDTO getSchemaMetadata(@PathVariable(name = "pid") String pid, @RequestParam(name = "includeVersionInfo", defaultValue = "false") String includeVersionInfo, @RequestParam(name = "includeVariantInfo", defaultValue = "false") String includeVariantInfo){    	
    	var jenaModel = jenaService.getSchema(pid);
		var hasRightsToModel = authorizationManager.hasRightToModelMSCR(pid, jenaModel);
        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;

    	return mapper.mapToSchemaDTO(pid, jenaModel, Boolean.parseBoolean(includeVersionInfo), Boolean.parseBoolean(includeVariantInfo), userMapper);
    }
    

     
    @Operation(summary = "Get original file version of the schema (if available)", description = "If the result is only one file it is returned as is, but if the content includes multiple files they a returned as a zip file.")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(path = "/schema/{pid}/original")
    public ResponseEntity<byte[]> exportOriginalFile(@PathVariable("pid") String pid) throws IOException {
    	List<StoredFile> files = storageService.retrieveAllSchemaFiles(pid);
    	return handleFileDownload(files);

	}
    
    @Operation(summary = "Download schema related file with a given id.")
    @ApiResponse(responseCode ="200")
    @GetMapping(path = "/schema/{pid}/files/{fileID}")
    public ResponseEntity<byte[]> downloadFile(@PathVariable String pid, @PathVariable String fileID, @RequestParam(name="download", defaultValue = "false" ) String download) {
    	StoredFile file = storageService.retrieveFile(pid, Long.parseLong(fileID), MSCRType.SCHEMA);
    	if(file == null) {
    		throw new ResourceNotFoundException(pid + "@file=" + fileID); 
    	}
    	return handleFileDownload(List.of(file), download);
    }    

	@Operation(summary = "Delete file")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path="/schema/{pid}/files/{fileID}", produces = APPLICATION_JSON_VALUE)
	public void deleteFile(@PathVariable String pid, @PathVariable Long fileID) throws Exception {
		var model = jenaService.getCrosswalk(pid);
		check(authorizationManager.hasRightToModelMSCR(pid, model));
		if(!isEditable(model, pid)) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Content can only be edited in the DRAFT state.");
		}

		var fileMetadata = storageService.retrieveFileMetadata(pid, fileID, MSCRType.SCHEMA);
		if(fileMetadata == null) {
			throw new ResourceNotFoundException(pid + "@file=" + fileID);
		}
		storageService.removeFile(fileID);	
	} 
	
	@Operation(summary = "Get SHACL version of the schema")
	@ApiResponse(responseCode = "200", description = "")
	@GetMapping(path = "/schema/{pid}/internal", produces = "text/turtle")
	public ResponseEntity<StreamingResponseBody> exportRawModel(@PathVariable String pid) {
		var model = jenaService.getSchema(pid);
		StreamingResponseBody responseBody = httpResponseOutputStream -> {
			model.write(httpResponseOutputStream, "TURTLE");
		};
		return ResponseEntity.status(HttpStatus.OK).body(responseBody);
	}

}