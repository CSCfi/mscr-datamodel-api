package fi.vm.yti.datamodel.api.v2.endpoint;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
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
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MSCRState;
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.dto.SchemaDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
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
import io.swagger.v3.oas.annotations.Hidden;
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

	public Schema(JenaService jenaService, AuthorizationManager authorizationManager,
			OpenSearchIndexer openSearchIndexer, SchemaMapper schemaMapper, SchemaService schemaService,
			PIDService PIDService, PostgresStorageService storageService, AuthenticatedUserProvider userProvider,
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

	private byte[] validateFileUpload(byte[] fileInBytes, SchemaFormat format) {
		try {

			if (format == SchemaFormat.JSONSCHEMA) {
				JsonNode jsonObj = schemaService.parseSchema(new String(fileInBytes));
				ValidationRecord validationRecord = JSONValidationService.validateJSONSchema(jsonObj);

				boolean isValidJSONSchema = validationRecord.isValid();
				List<String> validationMessages = validationRecord.validationOutput();

				if (!isValidJSONSchema) {
					String exceptionOutput = String.join("\n", validationMessages);
					throw new Exception(exceptionOutput);
				}

			} else if (format == SchemaFormat.XSD || format == SchemaFormat.XML || format == SchemaFormat.CSV
					|| format == SchemaFormat.SKOSRDF || format == SchemaFormat.RDFS || format == SchemaFormat.SHACL
					|| format == SchemaFormat.PDF || format == SchemaFormat.OWL) {
				// do nothing for now
			} else {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
						String.format("Unsupported schema description format: %s not supported", format));
			}

		} catch (ResponseStatusException statusex) {
			throw statusex;
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
					"Error occured while ingesting file based schema description. " + ex.getMessage(), ex);
		}
		return fileInBytes;

	}

	private SchemaInfoDTO addFileToSchema(final String pid, final SchemaFormat format, final byte[] fileInBytes, final String contentURL,
			final String contentType) {
		var userMapper = groupManagementService.mapUser();
		Model metadataModel = jenaService.getSchema(pid);
		try {
			Model schemaModel = null;

			if (format == SchemaFormat.JSONSCHEMA) {
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

			} else if (format == SchemaFormat.CSV) {
				schemaModel = schemaService.transformCSVSchemaToInternal(pid, fileInBytes, ";");

			} else if (format == SchemaFormat.SKOSRDF) {
				// TODO: validate skos file
				schemaModel = schemaService.addSKOSVocabulary(pid, fileInBytes);
			} else if (format == SchemaFormat.PDF) {
				// do nothing
				schemaModel = ModelFactory.createDefaultModel();
			} else if (format == SchemaFormat.OWL) {
				schemaModel = schemaService.addOWL(pid, contentURL, fileInBytes);				
			} else if (format == SchemaFormat.RDFS) {
				schemaModel = schemaService.addRDFS(pid, fileInBytes);

			} else if (format == SchemaFormat.SHACL) {
				schemaModel = schemaService.addSHACL(pid, fileInBytes);

			} else if (format == SchemaFormat.XSD) {
				if(contentURL != null) {
					schemaModel = schemaService.transformXSDToInternal(pid,contentURL);	
				}
				else {
					schemaModel = schemaService.transformXSDToInternal(pid, fileInBytes);	
				}
				

			} else if (format == SchemaFormat.XML) {
				// do nothing
				schemaModel = ModelFactory.createDefaultModel();
			} else {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
						String.format("Unsupported schema description format: %s not supported", format));
			}
			schemaModel.add(metadataModel);
			jenaService.updateSchema(pid, schemaModel);
			storageService.storeSchemaFile(pid, contentType, fileInBytes, generateFilename(pid, contentType));

		} catch (ResponseStatusException statusex) {
			throw statusex;
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
					"Error occured while ingesting file based schema description." + ex.getMessage(), ex);
		}
		return mapper.mapToSchemaDTO(pid, metadataModel, userMapper);
	}

	private SchemaDTO mergeSchemaMetadata(SchemaInfoDTO prevSchema, SchemaDTO inputSchema, boolean isRevision) {
		SchemaDTO s = new SchemaDTO();
		// in case of revision the following data cannot be overridden
		// - organization
		s.setStatus(inputSchema != null && inputSchema.getStatus() != null ? inputSchema.getStatus()
				: prevSchema.getStatus());
		s.setState(
				inputSchema != null && inputSchema.getState() != null ? inputSchema.getState() : prevSchema.getState());
		s.setVisibility(inputSchema != null && inputSchema.getVisibility() != null ? inputSchema.getVisibility()
				: prevSchema.getVisibility());
		s.setLabel(!inputSchema.getLabel().isEmpty() ? inputSchema.getLabel() : prevSchema.getLabel());
		s.setDescription(inputSchema != null && !inputSchema.getDescription().isEmpty() ? inputSchema.getDescription()
				: prevSchema.getDescription());
		s.setLanguages(inputSchema != null && !inputSchema.getLanguages().isEmpty() ? inputSchema.getLanguages()
				: prevSchema.getLanguages());
		s.setNamespace(inputSchema != null && inputSchema.getNamespace() != null ? inputSchema.getNamespace()
				: prevSchema.getNamespace());
		s.setContact(inputSchema != null && inputSchema.getContact() != null ? inputSchema.getContact()
				: prevSchema.getContact());
		if (isRevision || inputSchema == null || inputSchema.getOrganizations().isEmpty()) {
			s.setOrganizations(prevSchema.getOrganizations().stream().map(org -> UUID.fromString(org.getId()))
					.collect(Collectors.toSet()));
		} else {
			s.setOrganizations(inputSchema.getOrganizations());
		}
		s.setVersionLabel(
				inputSchema != null && inputSchema.getVersionLabel() != null ? inputSchema.getVersionLabel() : "");
		s.setFormat(inputSchema != null && inputSchema.getFormat() != null ? inputSchema.getFormat()
				: prevSchema.getFormat());
		return s;

	}

	private SchemaInfoDTO getSchemaDTO(String pid, boolean includeVersionInfo, boolean includeVariantInfo) {
		var model = jenaService.getSchema(pid);
		if (model == null) {
			throw new ResourceNotFoundException(pid);
		}
		var hasRightsToModel = authorizationManager.hasRightToModelMSCR(pid, model);

		check(hasRightsToModel);
		var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;

		return mapper.mapToSchemaDTO(pid, model, includeVersionInfo, includeVariantInfo, userMapper);
	}

	private void validateActionParams(SchemaDTO dto, CONTENT_ACTION action, String target) {
		if (dto == null && action == null) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"Request body must present if no action is provided.");
		}
		if (action == null && target != null) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Target parameter requires an action.");
		}
		if (action != null && target == null) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Action parameter requires a target");
		}
	}

	@Operation(summary = "Create schema metadata")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schema", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO createSchema(@ValidSchema() @RequestBody(required = false) SchemaDTO schemaDTO,
			@RequestParam(name = "action", required = false) CONTENT_ACTION action,
			@RequestParam(name = "target", required = false) String target) throws Exception {

		validateActionParams(schemaDTO, action, target);
		checkVisibility(schemaDTO);
		checkState(null, schemaDTO.getState());

		String aggregationKey = null;
		if (action != null) {
			SchemaInfoDTO prevSchema = getSchemaDTO(target, true, false);
			schemaDTO = mergeSchemaMetadata(prevSchema, schemaDTO, action == CONTENT_ACTION.revisionOf);
			if (action == CONTENT_ACTION.revisionOf) {
				// revision must be made from the latest version
				if (prevSchema.getHasRevisions() != null && !prevSchema.getHasRevisions().isEmpty()) {
					throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
							"Revisions can only be created from the latest revision. Check your target PID.");
				}
				aggregationKey = prevSchema.getAggregationKey();

			}
		}
		logger.info("Create Schema {}", schemaDTO);
		if (!schemaDTO.getOrganizations().isEmpty()) {
			check(authorizationManager.hasRightToAnyOrganization(schemaDTO.getOrganizations()));
		}

		final String PID = "mscr:schema:" + UUID.randomUUID();
		try {
			String handle = null;
			if (schemaDTO.getState() == MSCRState.PUBLISHED || schemaDTO.getState() == MSCRState.DEPRECATED) {
				handle = PIDService.mint(PIDType.HANDLE, MSCRType.SCHEMA, PID);

			}
			var jenaModel = mapper.mapToJenaModel(PID, handle, schemaDTO, target, aggregationKey,
					userProvider.getUser());
			jenaService.putToSchema(PID, jenaModel);

			// handle possible versioning data
			var schemaResource = jenaModel.createResource(PID);
			if (jenaModel.contains(schemaResource, MSCR.PROV_wasRevisionOf)) {
				Model prevVersionModel = jenaService.getSchema(target);
				Resource prevVersionResource = prevVersionModel.getResource(target);
				prevVersionResource.addProperty(MSCR.hasRevision, schemaResource);
				jenaService.updateSchema(target, prevVersionModel);

			}
			var indexModel = mapper.mapToIndexModel(PID, jenaModel);
			openSearchIndexer.createSchemaToIndex(indexModel);
			var userMapper = groupManagementService.mapUser();

			return mapper.mapToSchemaDTO(PID, jenaService.getSchema(PID), userMapper);
		} catch (Exception ex) {
			// revert any possible changes
			try {
				jenaService.deleteFromSchema(PID);
			} catch (Exception _ex) {
				logger.error(_ex.getMessage(), _ex);
			}
			try {
				openSearchIndexer.deleteSchemaFromIndex(PID);
			} catch (Exception _ex) {
				logger.error(_ex.getMessage(), _ex);
			}
			if ((ex instanceof ResponseStatusException) || (ex instanceof MappingError)) {
				throw ex;
			} else {
				throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
						"Unknown error occured. " + ex.getMessage(), ex);
			}
		}

	}

	@Operation(summary = "Upload and associate a schema description file to an existing schema")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schema/{pid}/upload", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public SchemaInfoDTO uploadSchemaFile(@PathVariable String pid, @RequestParam("file") MultipartFile file) {
		return uploadSchemaFile(pid, null, file);
	}

	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schema/{pid}/{suffix}/upload", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public SchemaInfoDTO uploadSchemaFile(
			@PathVariable String pid,
			@PathVariable String suffix, 
			@RequestParam("file") MultipartFile file) {

		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);
			// check for auth here because addFileToSchema is not doing it
			var model = jenaService.getSchema(pid);
			if (!isEditable(model, pid)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
						"Content can only be edited in the DRAFT state.");
			}

			var userMapper = groupManagementService.mapUser();
			SchemaInfoDTO schemaDTO = mapper.mapToSchemaDTO(pid, model, userMapper);

			if (!schemaDTO.getOrganizations().isEmpty()) {
				Collection<UUID> orgs = schemaDTO.getOrganizations().stream().map(org -> UUID.fromString(org.getId()))
						.toList();
				check(authorizationManager.hasRightToAnyOrganization(orgs));
			}
			return addFileToSchema(pid, schemaDTO.getFormat(), file.getBytes(), null, file.getContentType());
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		} finally {
			try {
				storageService.deleteAllSchemaFiles(pid);
			} catch (Exception _ex) {
			}
		}
	}

	@Operation(summary = "Create schema by uploading metadata and files in one multipart request")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schemaFull", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public SchemaInfoDTO createSchemaFull(@RequestParam("metadata") String metadataString,
			@RequestParam(name = "contentURL", required = false) String contentURL,
			@RequestParam(name = "file", required = false) MultipartFile file,
			@RequestParam(name = "action", required = false) CONTENT_ACTION action,
			@RequestParam(name = "target", required = false) String target) throws Exception {

		if (contentURL == null && file == null) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"Either file or contentURL parameter must be supplied.");
		}
		ObjectMapper objMapper = new ObjectMapper();
		SchemaDTO schemaDTO = null;
		try {
			schemaDTO = objMapper.readValue(metadataString, SchemaDTO.class);
		} catch (JsonProcessingException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"Could not parse SchemaDTO from the metadata content. " + e.getMessage(), e);
		}
		String contentType = "";
		byte[] fileBytes = null;
		try {
			if (file == null) {
				// try to download url to file
				File tempFile = File.createTempFile("schema", "temp");
				FileUtils.copyURLToFile(new URL(contentURL), tempFile);
				fileBytes = validateFileUpload(FileUtils.readFileToByteArray(tempFile), schemaDTO.getFormat());
				contentType = "application/octet-stream"; // TODO: fix this
			} else {
				fileBytes = validateFileUpload(file.getBytes(), schemaDTO.getFormat());
				contentType = file.getContentType();
			}

		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
		}

		System.out.println(contentType);
		SchemaInfoDTO dto = createSchema(schemaDTO, action, target);
		final String PID = dto.getPID();

		if (!schemaDTO.getOrganizations().isEmpty()) {
			Collection<UUID> orgs = schemaDTO.getOrganizations();
			check(authorizationManager.hasRightToAnyOrganization(orgs));
		}
		return addFileToSchema(PID, schemaDTO.getFormat(), fileBytes, contentURL, contentType);

	}

	@Operation(summary = "Modify schema")
	@io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new schema node")
	@ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
	@PatchMapping(path = "/schema/{pid}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO updateModel(@RequestBody SchemaDTO schemaDTO, @PathVariable String pid) {
		return updateModel(schemaDTO, pid, null);
	}

	@Hidden
	@ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
	@PatchMapping(path = "/schema/{pid}/{suffix}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO updateModel(@RequestBody SchemaDTO schemaDTO, @PathVariable String pid,
			@PathVariable String suffix) {
		logger.info("Updating schema {}", schemaDTO);
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);

			var oldModel = jenaService.getSchema(pid);
			if (oldModel == null) {
				throw new ResourceNotFoundException(pid);
			}
			check(authorizationManager.hasRightToModelMSCR(pid, oldModel));
			var userMapper = groupManagementService.mapUser();
			SchemaInfoDTO prevSchema = mapper.mapToSchemaDTO(pid, oldModel, false, false, userMapper);
			schemaDTO = mergeSchemaMetadata(prevSchema, schemaDTO, false);
			checkVisibility(schemaDTO);
			checkState(prevSchema, schemaDTO.getState());
			Model jenaModel = null;
			if (prevSchema.getState() == MSCRState.DRAFT && schemaDTO.getState() == MSCRState.PUBLISHED) {
				try {
					String handle = PIDService.mint(PIDType.HANDLE, MSCRType.SCHEMA, pid);
					jenaModel = mapper.mapToUpdateJenaModel(pid, handle, schemaDTO, oldModel, userProvider.getUser());
				} catch (Exception ex) {
					throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
							"Exception while geting a new handle for the schema." + ex.getMessage());
				}
			} else {
				jenaModel = mapper.mapToUpdateJenaModel(pid, null, schemaDTO, oldModel, userProvider.getUser());
			}

			jenaService.putToSchema(pid, jenaModel);

			var indexModel = mapper.mapToIndexModel(pid, jenaModel);
			openSearchIndexer.updateSchemaToIndex(indexModel);
			return mapper.mapToSchemaDTO(pid, jenaModel, false, false, userMapper);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}

	}

	@Operation(summary = "Get a schema metadata")
	@ApiResponse(responseCode = "200", description = "")
	@GetMapping(value = "/schema/{pid}", produces = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO getSchemaMetadata(@PathVariable(name = "pid") String pid,
			@RequestParam(name = "includeVersionInfo", defaultValue = "false") String includeVersionInfo,
			@RequestParam(name = "includeVariantInfo", defaultValue = "false") String includeVariantInfo) {
		return getSchemaMetadata(pid, null, includeVersionInfo, includeVariantInfo);
	}

	@Hidden
	@GetMapping(value = "/schema/{pid}/{suffix}", produces = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO getSchemaMetadata(
			@PathVariable String pid,
			@PathVariable String suffix,
			@RequestParam(name = "includeVersionInfo", defaultValue = "false") String includeVersionInfo,
			@RequestParam(name = "includeVariantInfo", defaultValue = "false") String includeVariantInfo) {
		try {
			if (suffix != null) {
				pid = pid + "/" + suffix;
			}
			pid = PIDService.mapToInternal(pid);
			var jenaModel = jenaService.getSchema(pid);
			var hasRightsToModel = authorizationManager.hasRightToModelMSCR(pid, jenaModel);
			var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;

			return mapper.mapToSchemaDTO(pid, jenaModel, Boolean.parseBoolean(includeVersionInfo),
					Boolean.parseBoolean(includeVariantInfo), userMapper);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}
	}
	
    @Operation(summary = "Delete schema metadata and content")
    @SecurityRequirement(name = "Bearer Authentication")
    @ApiResponse(responseCode = "200", description = "")
    @DeleteMapping(value = "/schema/{pid}")
    public void deleteSchema(@PathVariable String pid){
    	deleteSchema(pid, null);
    }
    
    @Hidden
    @SecurityRequirement(name = "Bearer Authentication")
    @DeleteMapping(value = "/schema/{pid}/{suffix}")
    public void deleteSchema(
    		@PathVariable String pid, 
    		@PathVariable(name = "suffix") String suffix){
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			var internalID = PIDService.mapToInternal(pid);
			var model = jenaService.getSchema(internalID);
			if (model == null) {
				throw new ResourceNotFoundException(pid);
			}
			check(authorizationManager.hasRightToModelMSCR(internalID, model));
			var userMapper = groupManagementService.mapUser();
			SchemaInfoDTO prevSchema = mapper.mapToSchemaDTO(internalID, model, false, false, userMapper);
			if(prevSchema.getState() == MSCRState.DRAFT) {
				jenaService.deleteFromSchema(internalID);
				storageService.deleteAllSchemaFiles(internalID);				
			}
			else {
				checkState(prevSchema, MSCRState.REMOVED);
				SchemaDTO schemaDTO = new SchemaDTO();
				schemaDTO.setState(MSCRState.REMOVED);
				schemaDTO = mergeSchemaMetadata(prevSchema, schemaDTO, false);
				var jenaModel = mapper.mapToUpdateJenaModel(pid, null, schemaDTO, ModelFactory.createDefaultModel(), userProvider.getUser());
				jenaService.updateSchema(internalID, jenaModel);
				storageService.deleteAllSchemaFiles(internalID);				
			}			
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}
    }

	@Operation(summary = "Get original file version of the schema (if available)", description = "If the result is only one file it is returned as is, but if the content includes multiple files they a returned as a zip file.")
	@ApiResponse(responseCode = "200", description = "")
	@GetMapping(path = "/schema/{pid}/original")
	public ResponseEntity<byte[]> exportOriginalFile(@PathVariable("pid") String pid) {
		return exportOriginalFile(pid, null);
	}

	@Hidden
	@GetMapping(path = "/schema/{pid}/{suffix}/original")
	public ResponseEntity<byte[]> exportOriginalFile(
			@PathVariable String pid,
			@PathVariable String suffix) {
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);
			List<StoredFile> files = storageService.retrieveAllSchemaFiles(pid);
			return handleFileDownload(files);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}

	}

	@Operation(summary = "Download schema related file with a given id.")
	@ApiResponse(responseCode = "200")
	@GetMapping(path = "/schema/{pid}/files/{fileID}")
	public ResponseEntity<byte[]> downloadFile(@PathVariable String pid, @PathVariable String fileID,
			@RequestParam(name = "download", defaultValue = "false") String download) {
		return downloadFile(pid, null, fileID, download);
	}

	@Hidden
	@GetMapping(path = "/schema/{pid}/{suffix}/files/{fileID}")
	public ResponseEntity<byte[]> downloadFile(
			@PathVariable String pid,
			@PathVariable String suffix, @PathVariable String fileID,
			@RequestParam(name = "download", defaultValue = "false") String download) {
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);
			StoredFile file = storageService.retrieveFile(pid, Long.parseLong(fileID), MSCRType.SCHEMA);
			if (file == null) {
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
	@DeleteMapping(path = "/schema/{pid}/files/{fileID}", produces = APPLICATION_JSON_VALUE)
	public void deleteFile(@PathVariable String pid, @PathVariable Long fileID) {
		deleteFile(pid, null, fileID);
	}

	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path = "/schema/{pid}/{suffix}/files/{fileID}", produces = APPLICATION_JSON_VALUE)
	public void deleteFile(
			@PathVariable String pid, 
			@PathVariable String suffix,
			@PathVariable Long fileID) {
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);
			var model = jenaService.getCrosswalk(pid);
			check(authorizationManager.hasRightToModelMSCR(pid, model));
			if (!isEditable(model, pid)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
						"Content can only be edited in the DRAFT state.");
			}

			var fileMetadata = storageService.retrieveFileMetadata(pid, fileID, MSCRType.SCHEMA);
			if (fileMetadata == null) {
				throw new ResourceNotFoundException(pid + "@file=" + fileID);
			}
			storageService.removeFile(fileID);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}

	}

	@Operation(summary = "Get SHACL version of the schema")
	@ApiResponse(responseCode = "200", description = "")
	@GetMapping(path = "/schema/{pid}/internal", produces = "text/turtle")
	public ResponseEntity<StreamingResponseBody> exportRawModel(@PathVariable String pid) {
		return exportRawModel(pid, null);
	}

	@Hidden
	@GetMapping(path = "/schema/{pid}/{suffix}/internal", produces = "text/turtle")
	public ResponseEntity<StreamingResponseBody> exportRawModel(
			@PathVariable String pid,
			@PathVariable String suffix) {
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);
			var model = jenaService.getSchema(pid);
			StreamingResponseBody responseBody = httpResponseOutputStream -> {
				model.write(httpResponseOutputStream, "TURTLE");
			};
			return ResponseEntity.status(HttpStatus.OK).body(responseBody);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}
	}
	
	@Operation(summary = "Update data type of a SHACL property")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PatchMapping(path = "/dtr/schema/{schemaID}/properties", produces = "application/json")
	public void updateProperty(@PathVariable(name = "schemaID") String schemaID, @RequestParam(name="target") String target, @RequestParam(name="datatype") String datatype) {
		updateProperty(null, schemaID, target, datatype);
	}	
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@PatchMapping(path = "/dtr/schema/{prefix}/{schemaID}/properties", produces = "application/json")
	public void updateProperty(@PathVariable String prefix, @PathVariable String schemaID, @RequestParam String target, @RequestParam String datatype) {
		if (prefix != null) {
			schemaID = prefix + "/" + schemaID;
		}
		try {
			schemaID = PIDService.mapToInternal(schemaID);
			Model model = jenaService.getSchema(schemaID);
			check(authorizationManager.hasRightToModelMSCR(schemaID, model));
			if(model == null) {
				throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Schema " + schemaID + " not found");
			}
			// only available for certain schema formats
			String format = MapperUtils.propertyToString(model.getResource(schemaID), MSCR.format);
			if(!Set.of(SchemaFormat.CSV.name(), SchemaFormat.JSONSCHEMA.name(), SchemaFormat.SHACL.name(), SchemaFormat.XSD.name()).contains(format)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Schema format must be CSV, JSONSCHEMA, SHAC or XSD");
			}
			Resource propResource = model.getResource(target);
			if(propResource == null) {
				throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Property " + target + " not in schema " + schemaID);
			}
			
			Model propModel = schemaService.fetchAndMapDTRType(datatype);
			Resource datatypeResource = propModel.listSubjectsWithProperty(RDF.type).next();
			jenaService.putToSchema(datatypeResource.getURI(), propModel);
			schemaService.updatePropertyDataTypeFromDTR(model, target, datatypeResource.getURI());
			jenaService.putToSchema(schemaID, model);
			

		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}		
	}

}