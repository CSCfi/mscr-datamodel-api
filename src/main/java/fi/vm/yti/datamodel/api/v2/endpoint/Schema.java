package fi.vm.yti.datamodel.api.v2.endpoint;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
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
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fasterxml.jackson.databind.JsonNode;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MSCRState;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.dto.SchemaDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.MimeTypes;
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
public class Schema {
	
	public enum CreateActions {
		copyOf, revisionOf
	}

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
	
	
	private SchemaInfoDTO addFileToSchema(String pid, String contentType, MultipartFile file) {
		Model metadataModel = jenaService.getSchema(pid);		
        var hasRightsToModel = authorizationManager.hasRightToModel(pid, metadataModel);
		check(hasRightsToModel);
        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;

		SchemaInfoDTO schemaDTO = mapper.mapToSchemaDTO(pid, metadataModel, userMapper);
		if(schemaDTO.getState() != MSCRState.DRAFT) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Files can only be added to content in the DRAFT state.");			
		}
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
				
				
			} else {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, String.format("Unsupported schema description format: %s not supported",
						schemaDTO.getFormat()));
			}
			schemaModel.add(metadataModel);
			jenaService.updateSchema(pid, schemaModel);
			storageService.storeSchemaFile(pid, contentType, file.getBytes());
			

		} catch (Exception ex) {
			ex.printStackTrace();
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Error occured while ingesting file based schema description", ex);
		}
		return mapper.mapToSchemaDTO(pid, metadataModel, userMapper);
	}
	
	private SchemaDTO mergeSchemaMetadata(SchemaInfoDTO prevSchema, SchemaDTO inputSchema, boolean isRevision) {
		if(inputSchema == null) {
			return mapper.mapToSchemaDTO(prevSchema);
		}
		SchemaDTO s = new SchemaDTO();
		// in case of revision the following data cannot be overridden
		// - organization
		s.setStatus(inputSchema.getStatus() != null ? inputSchema.getStatus() : Status.DRAFT);
		s.setState(inputSchema.getState() != null ? inputSchema.getState() : MSCRState.DRAFT);
		s.setLabel(!inputSchema.getLabel().isEmpty()? inputSchema.getLabel() : prevSchema.getLabel());
		s.setDescription(!inputSchema.getDescription().isEmpty() ? inputSchema.getDescription() : prevSchema.getDescription());
		s.setLanguages(!inputSchema.getLanguages().isEmpty() ? inputSchema.getLanguages() : prevSchema.getLanguages());
		s.setNamespace(inputSchema.getNamespace() != null ? inputSchema.getNamespace() : prevSchema.getNamespace());		
		if(isRevision || inputSchema.getOrganizations().isEmpty()) {
			s.setOrganizations(prevSchema.getOrganizations().stream().map(org ->  UUID.fromString(org.getId())).collect(Collectors.toSet()));
		}	
		else {
			s.setOrganizations(inputSchema.getOrganizations());			
		}	
		s.setVersionLabel(inputSchema.getVersionLabel() != null ? inputSchema.getVersionLabel() : "");
		s.setFormat(inputSchema.getFormat() != null ? inputSchema.getFormat() : prevSchema.getFormat());
		return s;
		
	}
	
	private SchemaInfoDTO getSchemaDTO(String pid, boolean includeVersionInfo) {
        var model = jenaService.getSchema(pid);
        if(model == null){
            throw new ResourceNotFoundException(pid);
        }
        var hasRightsToModel = authorizationManager.hasRightToModel(pid, model);
        
        check(hasRightsToModel);
        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;

        return mapper.mapToSchemaDTO(pid, model, includeVersionInfo, userMapper);
	}

	

	private void validateActionParams(SchemaDTO dto, CreateActions action, String target) {
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
	public SchemaInfoDTO createSchema(@ValidSchema() @RequestBody(required = false) SchemaDTO schemaDTO, @RequestParam(name = "action", required = false) CreateActions action, @RequestParam(name = "target", required = false) String target) {
		validateActionParams(schemaDTO, action, target); 
		String aggregationKey = null;
		if(action != null) {			
			SchemaInfoDTO prevSchema = getSchemaDTO(target, true);
			schemaDTO = mergeSchemaMetadata(prevSchema, schemaDTO, action == CreateActions.revisionOf);			
			if(action == CreateActions.revisionOf) {
				// revision must be made from the latest version
				if(prevSchema.getHasRevisions() != null && !prevSchema.getHasRevisions().isEmpty()) {
					throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Revisions can only be created from the latest revision. Check your target PID.");
				}
				aggregationKey = prevSchema.getAggregationKey();
				
			}
		}
		logger.info("Create Schema {}", schemaDTO);
		check(authorizationManager.hasRightToAnyOrganization(schemaDTO.getOrganizations()));	
		final String PID = PIDService.mint(PIDType.HANDLE);

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
				
	}
    
	@Operation(summary = "Upload and associate a schema description file to an existing schema")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schema/{pid}/upload", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public SchemaInfoDTO uploadSchemaFile(@PathVariable String pid, @RequestParam("contentType") String contentType,
			@RequestParam("file") MultipartFile file) throws Exception {
		return addFileToSchema(pid, contentType, file);
	}
	
	@Operation(summary = "Create schema by uploading metadata and files in one multipart request")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schemaFull", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public SchemaInfoDTO createSchemaFull(@ValidSchema @RequestParam("metadata") SchemaDTO schemaDTO,
			@RequestParam("file") MultipartFile file, @RequestParam(name = "action", required = false) CreateActions action, @RequestParam(name = "target", required = false) String target) {		
		SchemaInfoDTO dto = createSchema(schemaDTO, action, target);
		return addFileToSchema(dto.getPID(), file.getContentType(), file);						
	}
  
    @Operation(summary = "Modify schema")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new schema node")
    @ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
    @PostMapping(path = "/schema/{pid}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public void updateModel(@ValidSchema @RequestBody SchemaDTO schemaDTO,
                            @PathVariable String pid) {
        logger.info("Updating schema {}", schemaDTO);

        var oldModel = jenaService.getSchema(pid);
        if(oldModel == null){
            throw new ResourceNotFoundException(pid);
        }

        check(authorizationManager.hasRightToModel(pid, oldModel));

        var jenaModel = mapper.mapToUpdateJenaModel(pid, schemaDTO, oldModel, userProvider.getUser());

        jenaService.putToSchema(pid, jenaModel);


        var indexModel = mapper.mapToIndexModel(pid, jenaModel);
        openSearchIndexer.updateSchemaToIndex(indexModel);
    }

    
    
    @Operation(summary = "Get a schema metadata")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(value = "/schema/{pid}", produces = APPLICATION_JSON_VALUE)
    public SchemaInfoDTO getSchemaMetadata(@PathVariable(name = "pid") String pid, @RequestParam(name = "includeVersionInfo", defaultValue = "false") String includeVersionInfo){    	
    	var jenaModel = jenaService.getSchema(pid);
		var hasRightsToModel = authorizationManager.hasRightToModel(pid, jenaModel);
        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;

    	return mapper.mapToSchemaDTO(pid, jenaModel, Boolean.parseBoolean(includeVersionInfo), userMapper);
    }
    

     
    @Operation(summary = "Get original file version of the schema (if available)", description = "If the result is only one file it is returned as is, but if the content includes multiple files they a returned as a zip file.")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(path = "/schema/{pid}/original")
    public ResponseEntity<byte[]> exportOriginalFile(@PathVariable("pid") String pid) throws IOException {
    	List<StoredFile> files = storageService.retrieveAllSchemaFiles(pid);
    	
    	if (files.isEmpty()) {
    		return ResponseEntity.notFound().build();   				
    	}
    	
    	if (files.size() == 1) {
    		StoredFile file = files.get(0);
    		return ResponseEntity.ok()
    				.contentType(MediaType.parseMediaTypes(file.contentType()).get(0))
    				.body(file.data());		
    	}
    	else {
    		ByteArrayOutputStream baos = new ByteArrayOutputStream();
    		ZipOutputStream zipOut = new ZipOutputStream(baos);

    		for (StoredFile file : files) {
    			ZipEntry zipEntry = new ZipEntry(file.fileID() + MimeTypes.getExtension(file.contentType()));
    			zipOut.putNextEntry(zipEntry);
    			zipOut.write(file.data(), 0, file.data().length);
    		}
      
    		zipOut.close();           
    		//baos.close();               
    		
    		byte [] zip = baos.toByteArray();    
    				  
    		return ResponseEntity.ok()
    				.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=files.zip")
    				.contentType(MediaType.parseMediaType("application/zip"))
    				.contentLength(zip.length)
    				.body(zip); 										
    	}
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