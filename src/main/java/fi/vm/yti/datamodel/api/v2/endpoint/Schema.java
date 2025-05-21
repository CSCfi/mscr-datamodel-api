package fi.vm.yti.datamodel.api.v2.endpoint;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.io.File;
import java.io.FileOutputStream;
import java.io.StringReader;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import fi.vm.yti.datamodel.api.v2.dto.DeleteResponseDTO;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MSCRState;
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.dto.PublicSchemaMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.PublicSchemaMetadataInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.UpdateResponseDTO;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import fi.vm.yti.datamodel.api.v2.mapper.SchemaMapper;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.ExternalSchemaMetadataToInternalConverter;
import fi.vm.yti.datamodel.api.v2.mapper.mscr.InternalSchemaMetadataToExternalConverter;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.service.GroupManagementService;
import fi.vm.yti.datamodel.api.v2.service.JSONValidationService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.PIDService;
import fi.vm.yti.datamodel.api.v2.service.SchemaService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFile;
import fi.vm.yti.datamodel.api.v2.service.TransformationService;
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
	
	private final TransformationService transformationService;

	@Autowired
	private InternalSchemaMetadataToExternalConverter convertToExternal;
	
	@Autowired
	private ExternalSchemaMetadataToInternalConverter convertToInternal;
	
	public Schema(JenaService jenaService, AuthorizationManager authorizationManager,
			OpenSearchIndexer openSearchIndexer, SchemaMapper schemaMapper, SchemaService schemaService,
			PIDService PIDService, PostgresStorageService storageService, AuthenticatedUserProvider userProvider,
			GroupManagementService groupManagementService, TransformationService transformationService) {

		this.jenaService = jenaService;
		this.openSearchIndexer = openSearchIndexer;
		this.authorizationManager = authorizationManager;
		this.mapper = schemaMapper;
		this.schemaService = schemaService;
		this.PIDService = PIDService;
		this.storageService = storageService;
		this.userProvider = userProvider;
		this.groupManagementService = groupManagementService;
		this.transformationService = transformationService;
		
	}

	private byte[] validateFileUpload(byte[] fileInBytes, SchemaFormat format, boolean skipProcessing) {
		try {

			if (format == SchemaFormat.JSONSCHEMA) {
				if(!skipProcessing) {
					JsonNode jsonObj = schemaService.parseSchema(new String(fileInBytes));
					ValidationRecord validationRecord = JSONValidationService.validateJSONSchema(jsonObj);
	
					boolean isValidJSONSchema = validationRecord.isValid();
					List<String> validationMessages = validationRecord.validationOutput();
	
					if (!isValidJSONSchema) {
						String exceptionOutput = String.join("\n", validationMessages);
						throw new Exception(exceptionOutput);
					}
				}
			} else if (format == SchemaFormat.XSD || format == SchemaFormat.XML || format == SchemaFormat.CSV
					|| format == SchemaFormat.SKOSRDF || format == SchemaFormat.RDFS || format == SchemaFormat.SHACL
					|| format == SchemaFormat.PDF || format == SchemaFormat.OWL ||format == SchemaFormat.ENUM) {
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

	private void addFileToSchema(final String pid, final SchemaFormat format, final byte[] fileInBytes, final String contentURL,
			final String contentType, boolean skipProcessing) {
		try {
			Model schemaModel = ModelFactory.createDefaultModel();
			
			if(skipProcessing) {
				// do nothing
			}
			else if (format == SchemaFormat.JSONSCHEMA) {
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
				
			} else if (format == SchemaFormat.ENUM) {
				schemaModel = schemaService.transformEnumSkos(pid,fileInBytes);	
				
			} else if (format == SchemaFormat.XML) {
				// do nothing
				schemaModel = ModelFactory.createDefaultModel();
			} else {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
						String.format("Unsupported schema description format: %s not supported", format));
			}
			
			jenaService.putToSchema(pid + ":content", schemaModel);
			storageService.storeSchemaFile(pid, contentType, fileInBytes, generateFilename(pid, contentType));

		} catch (ResponseStatusException statusex) {
			throw statusex;
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
					"Error occured while ingesting file based schema description." + ex.getMessage(), ex);
		}
		
	}

	private SchemaDTO mergeSchemaMetadata(SchemaInfoDTO prevSchema, SchemaDTO inputSchema, CONTENT_ACTION action) {		
		SchemaDTO s = new SchemaDTO();
		// in case of revision the following data cannot be overridden
		// - organization
		// - format
		// - versionLabel - defaults to ""
		if(inputSchema != null) {
			s.setState(inputSchema.getState() != null ? inputSchema.getState() : prevSchema.getState());
			s.setVisibility(inputSchema.getVisibility() != null ? inputSchema.getVisibility()
					: prevSchema.getVisibility());
			s.setLabel(!inputSchema.getLabel().isEmpty() ? inputSchema.getLabel() : prevSchema.getLabel());
			s.setDescription(inputSchema.getDescription() != null && !inputSchema.getDescription().isEmpty() ? inputSchema.getDescription()
					: prevSchema.getDescription());
			s.setLanguages(inputSchema.getLanguages() != null && !inputSchema.getLanguages().isEmpty() ? inputSchema.getLanguages()
					: prevSchema.getLanguages());
			s.setNamespace(inputSchema.getNamespace() != null ? inputSchema.getNamespace()
					: prevSchema.getNamespace());
			s.setContact(inputSchema.getContact() != null ? inputSchema.getContact()
					: prevSchema.getContact());
			s.setDcatKeywords(inputSchema.getDcatKeywords() != null ? inputSchema.getDcatKeywords(): prevSchema.getDcatKeywords());		
			s.setDctContributors(inputSchema.getDctContributors() != null ? inputSchema.getDctContributors() : prevSchema.getDctContributors());
			s.setDctCreators(inputSchema.getDctCreators() != null ? inputSchema.getDctCreators() : prevSchema.getDctCreators());
			s.setDctIdentifiers(inputSchema.getDctIdentifiers() != null ? inputSchema.getDctIdentifiers() : prevSchema.getDctIdentifiers());
			s.setDctIssued(inputSchema.getDctIssued() != null ? inputSchema.getDctIssued(): prevSchema.getDctIssued());
			s.setDctLicense(inputSchema.getDctLicense() != null ? inputSchema.getDctLicense(): prevSchema.getDctLicense());
			s.setDctPublisher(inputSchema.getDctPublisher() != null ? inputSchema.getDctPublisher() : prevSchema.getDctPublisher());
			s.setDctRelations(inputSchema.getDctRelations() != null ? inputSchema.getDctRelations(): prevSchema.getDctRelations());
			s.setDomain(inputSchema.getDomain() != null ? inputSchema.getDomain(): prevSchema.getDomain());
			
			s.setSourceURL(inputSchema.getSourceURL());

		}

		
		if (action == CONTENT_ACTION.revisionOf || inputSchema == null || inputSchema.getOrganizations().isEmpty()) {
			s.setOrganizations(prevSchema.getOrganizations().stream().map(org -> UUID.fromString(org.getId()))
					.collect(Collectors.toSet()));
		} else {
			s.setOrganizations(inputSchema.getOrganizations());
		}
		s.setVersionLabel(
				inputSchema != null && inputSchema.getVersionLabel() != null ? inputSchema.getVersionLabel() : prevSchema.getVersionLabel());
		
		if(action == CONTENT_ACTION.revisionOf) {
			s.setFormat(prevSchema.getFormat());
			if(prevSchema.getFormat() == SchemaFormat.MSCR) {
				s.setOriginalFormat(prevSchema.getOriginalFormat());	
			}			
		}
		else if(action == CONTENT_ACTION.mscrCopyOf) { 
			s.setFormat(SchemaFormat.MSCR);
			s.setOriginalFormat(prevSchema.getFormat());				
		}
		else {
			s.setFormat(inputSchema !=null && inputSchema.getFormat() != null ? inputSchema.getFormat() : prevSchema.getFormat());
		}
		s.setSubType(prevSchema.getSubType());
		
		
		
		return s;

	}

	private SchemaInfoDTO getSchemaDTO(String pid, Model model, boolean includeVersionInfo, boolean includeVariantInfo, CONTENT_ACTION action) {
		var hasRightsToModel = authorizationManager.hasRightToModelMSCR(pid, model);
		if(!Set.of(CONTENT_ACTION.mscrCopyOf, CONTENT_ACTION.copyOf).contains(action)) {
			check(hasRightsToModel);			
		}
		var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;
		var ownerMapper = groupManagementService.mapOwner();
		return mapper.mapToSchemaDTO(pid, model, includeVersionInfo, includeVariantInfo, userMapper, ownerMapper);
	}
	
	private Model getSchemaModel(String pid) throws Exception {
		// handle possible Handle (pun intended!)
		String internalID = PIDService.mapToInternal(pid);
		var model = jenaService.getSchema(internalID);
		if (model == null) {
			throw new ResourceNotFoundException(pid);
		}
		return model;
	}	
	
	@Tag(name = "Schema")
	@Operation(summary = "Create schema metadata")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schema", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public PublicSchemaMetadataInfoDTO createSchema(@ValidSchema() @RequestBody(required = false) PublicSchemaMetadataDTO schemaDTO,
			@RequestParam(name = "action", required = false) CONTENT_ACTION action,
			@RequestParam(name = "target", required = false) String target,
			@RequestParam(name = "skipProcessing", required = false, defaultValue = "false") boolean skipProcessing) throws Exception {
		return convertToExternal.convert(
				createSchemaFrontend(
						(SchemaDTO)convertToInternal.convert(schemaDTO), 
						action, 
						target, 
						skipProcessing));
	}

		
	@Tag(name = "Frontend")
	@Operation(summary = "Create schema metadata")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/frontend/schema", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO createSchemaFrontend(@ValidSchema() @RequestBody(required = false) SchemaDTO schemaDTO,
			@RequestParam(name = "action", required = false) CONTENT_ACTION action,
			@RequestParam(name = "target", required = false) String target,
			@RequestParam(name = "skipProcessing", required = false, defaultValue = "false") boolean skipProcessing) throws Exception {

		validateActionParams(schemaDTO, action, target);
		checkVisibility(schemaDTO);
		checkState(null, schemaDTO);

		final String PID = "mscr:schema:" + UUID.randomUUID();
		String aggregationKey = null;
		Model contentModel = ModelFactory.createDefaultModel();
		if (action != null) {
			Model prevModel = getSchemaModel(target);
			SchemaInfoDTO prevSchema = getSchemaDTO(target, prevModel, true, false, action);
			schemaDTO = mergeSchemaMetadata(prevSchema, schemaDTO, action);
			if (action == CONTENT_ACTION.revisionOf) {
				// revision must be made from the latest version
				if(prevSchema.getRevisions() != null && prevSchema.getRevisions().size() > 0 && !prevSchema.getRevisions().get(prevSchema.getRevisions().size() -1).getPid().equals(prevSchema.getID()) ) {
					throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
							"Revisions can only be created from the latest revision. Check your target PID.");
				}
				aggregationKey = prevSchema.getAggregationKey();
				if(prevSchema.getFormat() == SchemaFormat.MSCR) {
					if(jenaService.doesSchemaExist(prevSchema.getID() + ":content")) {
						// This is really hacky!
						File tempFile = File.createTempFile("model", ".ttl");
						Model tempModel = jenaService.getSchemaContent(prevSchema.getID());
						FileOutputStream fos = new FileOutputStream(tempFile);						
						RDFDataMgr.write(fos, tempModel, Lang.TTL);
						
						String fileContent = FileUtils.readFileToString(tempFile);
						fileContent = 
								fileContent
								.replaceFirst("<" + prevSchema.getID() + "#", "<" + PID + "#")
								.replaceFirst("<" + prevSchema.getID() + ">", "<" + PID + ">");
						StringReader r = new StringReader(fileContent);
						contentModel.read(r, null, "TURTLE");
						r.close();
						fos.close();
						
						
					}
										
				}

			}
			if(action == CONTENT_ACTION.mscrCopyOf) {
				if(!Set.of(SchemaFormat.CSV, SchemaFormat.JSONSCHEMA, SchemaFormat.MSCR, SchemaFormat.SHACL, SchemaFormat.XSD).contains(prevSchema.getFormat())) {
					throw new ResponseStatusException(HttpStatus.BAD_REQUEST, 
							"MSCR copy can only be made from a schema with a format CSV, JSONSCHEMA, MSCR, SHACL or XSD");
					
				}
				String contentType = "";
				byte[] fileBytes = null;
				// copy content by redoing the registration process
				if(prevSchema.getSourceURL() != null && !"".equals(prevSchema.getSourceURL())) {
					// try to download url to file
					File tempFile = File.createTempFile("schema", "temp");
					FileUtils.copyURLToFile(new URL(prevSchema.getSourceURL()), tempFile);
					fileBytes = validateFileUpload(FileUtils.readFileToByteArray(tempFile), prevSchema.getFormat(), skipProcessing);
					contentType = "application/octet-stream"; // TODO: fix this
				}
				else {
					StoredFile schemaFile = storageService.retrieveAllSchemaFiles(target).get(0); // there is currently only one file always
					fileBytes = schemaFile.data();
					contentType = schemaFile.contentType();
				}
				addFileToSchema(PID, schemaDTO.getOriginalFormat() != null ? schemaDTO.getOriginalFormat() : schemaDTO.getFormat(), fileBytes, prevSchema.getSourceURL(), contentType, skipProcessing);	
				
				
				
				
			}
		}
		logger.info("Create Schema {}", schemaDTO);
		if (!schemaDTO.getOrganizations().isEmpty()) {
			check(authorizationManager.hasRightToAnyOrganization(schemaDTO.getOrganizations()));
		}

		
		try {
			String handle = null;
			if (schemaDTO.getState() == MSCRState.PUBLISHED || schemaDTO.getState() == MSCRState.DEPRECATED) {
				handle = PIDService.mint(PIDType.HANDLE, MSCRType.SCHEMA, PID);
			}
			String subType = getSchemaContentSubType(schemaDTO.getFormat().name());	
			var jenaModel = mapper.mapToJenaModel(PID, handle, schemaDTO, target, aggregationKey,
					userProvider.getUser(), subType);
			if(!contentModel.isEmpty()) {
				jenaService.putToSchema(PID+":content", contentModel);
			}
			jenaService.putToSchema(PID, jenaModel);

			// handle possible versioning data
			var schemaResource = jenaModel.createResource(PID);
			if (jenaModel.contains(schemaResource, MSCR.PROV_wasRevisionOf)) {
				Model prevVersionModel = jenaService.getSchema(target); // this is redundant - refactor!
				Resource prevVersionResource = prevVersionModel.getResource(target);
				prevVersionResource.addProperty(MSCR.hasRevision, schemaResource);
				jenaService.updateSchema(target, prevVersionModel);
				openSearchIndexer.updateSchemaToIndex(mapper.mapToIndexModel(target, prevVersionModel));

			}
			var indexModel = mapper.mapToIndexModel(PID, jenaModel);
			openSearchIndexer.createSchemaToIndex(indexModel);
			var userMapper = groupManagementService.mapUser();
			var ownerMapper = groupManagementService.mapOwner();
			return mapper.mapToSchemaDTO(PID, jenaService.getSchema(PID), userMapper, ownerMapper);
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


	@Tag(name = "Frontend")
	@Operation(summary = "Upload and associate a schema description file to an existing schema")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/frontend/schema/{pid}/upload", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public SchemaInfoDTO uploadSchemaFile(@PathVariable String pid, @RequestParam("file") MultipartFile file, @RequestParam(name = "skipProcessing", required = false, defaultValue = "false") boolean skipProcessing) {
		return uploadSchemaFile(pid, null, file, skipProcessing);
	}

	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/frontend/schema/{pid}/{suffix}/upload", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public SchemaInfoDTO uploadSchemaFile(
			@PathVariable String pid,
			@PathVariable String suffix, 
			@RequestParam("file") MultipartFile file,
			@RequestParam(name = "skipProcessing", required = false, defaultValue = "false") boolean skipProcessing) {

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
			var ownerMapper = groupManagementService.mapOwner();
			SchemaInfoDTO schemaDTO = mapper.mapToSchemaDTO(pid, model, userMapper, ownerMapper);

			if (!schemaDTO.getOrganizations().isEmpty()) {
				Collection<UUID> orgs = schemaDTO.getOrganizations().stream().map(org -> UUID.fromString(org.getId()))
						.toList();
				check(authorizationManager.hasRightToAnyOrganization(orgs));
			}
			addFileToSchema(pid, schemaDTO.getFormat(), file.getBytes(), null, file.getContentType(), skipProcessing);
			return schemaDTO;
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

	@Tag(name = "Schema")
	@Operation(summary = "Create schema by uploading metadata and files in one multipart request")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schemaFull", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public PublicSchemaMetadataInfoDTO createSchemaFull(@RequestParam("metadata") String metadataString,
			@RequestParam(name = "contentURL", required = false) String contentURL,
			@RequestParam(name = "file", required = false) MultipartFile file,
			@RequestParam(name = "action", required = false) CONTENT_ACTION action,
			@RequestParam(name = "target", required = false) String target,
			@RequestParam(name = "skipProcessing", required = false, defaultValue = "false") boolean skipProcessing) throws Exception {
		
		ObjectMapper m = new ObjectMapper();
		PublicSchemaMetadataDTO schemaDTO = m.readValue(metadataString, PublicSchemaMetadataDTO.class);
		return convertToExternal.convert(
				createSchemaFullFrontend(
						m.writeValueAsString((SchemaDTO)convertToInternal.convert(schemaDTO)), 
						contentURL, 
						file,
						action,
						target,
						skipProcessing));
	}

	@Tag(name = "Frontend")	
	@Operation(summary = "Create schema by uploading metadata and files in one multipart request")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/frontend/schemaFull", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public SchemaInfoDTO createSchemaFullFrontend(@RequestParam("metadata") String metadataString,
			@RequestParam(name = "contentURL", required = false) String contentURL,
			@RequestParam(name = "file", required = false) MultipartFile file,
			@RequestParam(name = "action", required = false) CONTENT_ACTION action,
			@RequestParam(name = "target", required = false) String target,
			@RequestParam(name = "skipProcessing", required = false, defaultValue = "false") boolean skipProcessing) throws Exception {

		if (contentURL == null && file == null) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
					"Either file or contentURL parameter must be supplied.");
		}
		ObjectMapper objMapper = new ObjectMapper();
		SchemaDTO schemaDTO = null;
		try {
			schemaDTO = objMapper.readValue(metadataString, SchemaDTO.class);
			schemaDTO.setSourceURL(contentURL);
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
				fileBytes = validateFileUpload(FileUtils.readFileToByteArray(tempFile), schemaDTO.getFormat(), skipProcessing);
				contentType = "application/octet-stream"; // TODO: fix this
			} else {
				fileBytes = validateFileUpload(file.getBytes(), schemaDTO.getFormat(), skipProcessing);
				contentType = file.getContentType();
			}

		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
		}
		
		SchemaInfoDTO dto = null;
		try {
			dto = createSchemaFrontend(schemaDTO, action, target, skipProcessing);
			final String PID = dto.getID();

			if (!schemaDTO.getOrganizations().isEmpty()) {
				Collection<UUID> orgs = schemaDTO.getOrganizations();
				check(authorizationManager.hasRightToAnyOrganization(orgs));
			}
			addFileToSchema(PID, schemaDTO.getFormat(), fileBytes, contentURL, contentType, skipProcessing);	
		}catch(Exception ex) {
			ex.printStackTrace();
			// revert any possible metadata changes
			if(dto != null) {
				try {
					jenaService.deleteFromSchema(dto.getID());
				} catch (Exception _ex) {
					//logger.error(_ex.getMessage(), _ex);
				}
				try {
					openSearchIndexer.deleteSchemaFromIndex(dto.getID());
				} catch (Exception _ex) {
					//logger.error(_ex.getMessage(), _ex);
				}				
			}
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage());
			
		}		
		return dto;

	}

	@Tag(name = "Schema")
	@Operation(summary = "Modify schema")
	@io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new schema node")
	@ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
	@PatchMapping(path = "/schema/{pid}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public PublicSchemaMetadataInfoDTO updateModel(@RequestBody PublicSchemaMetadataDTO schemaDTO, @PathVariable String pid) {
		return updateModel(schemaDTO, pid, null);
	}
	
	@Hidden
	@Operation(summary = "Modify schema")
	@io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new schema node")
	@ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
	@PatchMapping(path = "/schema/{pid}/{suffix}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public PublicSchemaMetadataInfoDTO updateModel(@RequestBody PublicSchemaMetadataDTO schemaDTO, @PathVariable String pid,
			@PathVariable String suffix) {
		return convertToExternal.convert(
					updateModelFrontend(
						(SchemaDTO)convertToInternal.convert(schemaDTO), pid, suffix)
					);				
	}	
	
	@Tag(name = "Frontend")	
	@Operation(summary = "Modify schema")
	@io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new schema node")
	@ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
	@PatchMapping(path = "/frontend/schema/{pid}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO updateModelFrontend(@RequestBody SchemaDTO schemaDTO, @PathVariable String pid) {
		return updateModelFrontend(schemaDTO, pid, null);
	}

	@Hidden
	@ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
	@PatchMapping(path = "/frontend/schema/{pid}/{suffix}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO updateModelFrontend(@RequestBody SchemaDTO schemaDTO, @PathVariable String pid,
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
			var ownerMapper = groupManagementService.mapOwner();
			SchemaInfoDTO prevSchema = mapper.mapToSchemaDTO(pid, oldModel, false, false, userMapper, ownerMapper);
			schemaDTO = mergeSchemaMetadata(prevSchema, schemaDTO, CONTENT_ACTION.update);
			checkVisibility(schemaDTO);
			checkState(prevSchema, schemaDTO.getState());
			Model jenaModel = null;
			if (prevSchema.getState() == MSCRState.DRAFT && schemaDTO.getState() == MSCRState.PUBLISHED) {
				try {
					String handle = PIDService.mint(PIDType.HANDLE, MSCRType.SCHEMA, pid);
					jenaModel = mapper.mapToUpdateJenaModel(pid, handle, schemaDTO, oldModel, userProvider.getUser(), true, prevSchema.getState() != schemaDTO.getState());
				} catch (Exception ex) {
					throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
							"Exception while geting a new handle for the schema." + ex.getMessage());
				}
			} else {
				jenaModel = mapper.mapToUpdateJenaModel(pid, null, schemaDTO, oldModel, userProvider.getUser(), true, prevSchema.getState() != schemaDTO.getState());
			}

			jenaService.putToSchema(pid, jenaModel);

			// if state change must update the prev index model too 
			if(schemaDTO.getState() != prevSchema.getState()) {
				if(prevSchema.getRevisionOf() != null && !"".equals(prevSchema.getRevisionOf())) {
					openSearchIndexer.updateSchemaToIndex(
						mapper.mapToIndexModel(prevSchema.getRevisionOf(), 
								jenaService.getSchema(prevSchema.getRevisionOf())
						)
						
					);
					
				}
			}
			var indexModel = mapper.mapToIndexModel(pid, jenaModel);
			openSearchIndexer.updateSchemaToIndex(indexModel);
			
			
			
			return mapper.mapToSchemaDTO(pid, jenaModel, false, false, userMapper, ownerMapper);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}

	}
	
	@Tag(name = "Schema")	
	@Operation(summary = "Get a schema metadata")
	@ApiResponse(responseCode = "200", description = "")
	@GetMapping(value = "/schema/{pid}", produces = APPLICATION_JSON_VALUE)
	public PublicSchemaMetadataInfoDTO getSchemaMetadata(@PathVariable(name = "pid") String pid) {
		return getSchemaMetadata(pid, null);
	
	}	
	
	@Hidden
	@GetMapping(value = "/schema/{pid}/{suffix}", produces = APPLICATION_JSON_VALUE)
	public PublicSchemaMetadataInfoDTO getSchemaMetadata(
			@PathVariable String pid,
			@PathVariable String suffix) {
		return convertToExternal.convert(
				getSchemaMetadataFrontend(pid, suffix, "false", "false")
				);	
	}	

	@Tag(name = "Frontend")
	@Operation(summary = "Get a schema metadata in internal format")
	@ApiResponse(responseCode = "200", description = "")
	@GetMapping(value = "/frontend/schema/{pid}", produces = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO getSchemaMetadataFrontend(@PathVariable(name = "pid") String pid,
			@RequestParam(name = "includeVersionInfo", defaultValue = "false") String includeVersionInfo,
			@RequestParam(name = "includeVariantInfo", defaultValue = "false") String includeVariantInfo) {
		return getSchemaMetadataFrontend(pid, null, includeVersionInfo, includeVariantInfo);
	}

	@Hidden
	@GetMapping(value = "/frontend/schema/{pid}/{suffix}", produces = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO getSchemaMetadataFrontend(
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
			var ownerMapper = groupManagementService.mapOwner();
			return mapper.mapToSchemaDTO(pid, jenaModel, Boolean.parseBoolean(includeVersionInfo),
					Boolean.parseBoolean(includeVariantInfo), userMapper, ownerMapper);
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}
	}
	
	@Tag(name = "Schema")	
    @Operation(summary = "Delete schema metadata and content")
    @SecurityRequirement(name = "Bearer Authentication")
    @ApiResponse(responseCode = "200", description = "")
    @DeleteMapping(value = "/schema/{pid}")
    public ResponseEntity<DeleteResponseDTO> deleteSchema(@PathVariable String pid){
    	return deleteSchemaFrontend(pid, null);
    }
    
    @Hidden
    @SecurityRequirement(name = "Bearer Authentication")
    @DeleteMapping(value = "/schema/{pid}/{suffix}")
    public ResponseEntity<DeleteResponseDTO> deleteSchema(
    		@PathVariable String pid, 
    		@PathVariable(name = "suffix") String suffix){
    	return deleteSchemaFrontend(pid, suffix);
    	
    }
    
	@Tag(name = "Frontend")	
    @Operation(summary = "Delete schema metadata and content")
    @SecurityRequirement(name = "Bearer Authentication")
    @ApiResponse(responseCode = "200", description = "")
    @DeleteMapping(value = "/frontend/schema/{pid}")
    public ResponseEntity<DeleteResponseDTO> deleteSchemaFrontend(@PathVariable String pid){
    	return deleteSchemaFrontend(pid, null);
    }
    
    @Hidden
    @SecurityRequirement(name = "Bearer Authentication")
    @DeleteMapping(value = "/frontend/schema/{pid}/{suffix}")
    public ResponseEntity<DeleteResponseDTO> deleteSchemaFrontend(
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
			var ownerMapper = groupManagementService.mapOwner();
			SchemaInfoDTO prevSchema = mapper.mapToSchemaDTO(internalID, model, false, false, userMapper, ownerMapper);
			if(prevSchema.getState() == MSCRState.DRAFT) {
				jenaService.deleteFromSchema(internalID);
				if(jenaService.doesSchemaExist(internalID+":content")) {
					jenaService.deleteFromSchema(internalID+":content");
				}				
				storageService.deleteAllSchemaFiles(internalID);				
				openSearchIndexer.deleteSchemaFromIndex(internalID);
			}
			else {
				checkState(prevSchema, MSCRState.REMOVED);
				SchemaDTO schemaDTO = new SchemaDTO();
				schemaDTO.setState(MSCRState.REMOVED);
				schemaDTO = mergeSchemaMetadata(prevSchema, schemaDTO, CONTENT_ACTION.delete);
				var jenaModel = mapper.mapToUpdateJenaModel(pid, null, schemaDTO, ModelFactory.createDefaultModel(), userProvider.getUser(), false, true);
				var indexModel = mapper.mapToIndexModel(internalID, jenaModel);
				jenaService.updateSchema(internalID, jenaModel);
				if(jenaService.doesSchemaExist(internalID+":content")) {
					jenaService.deleteFromSchema(internalID+":content");
				}								
				storageService.deleteAllSchemaFiles(internalID);
				openSearchIndexer.updateSchemaToIndex(indexModel);
				
			}	
			// only one
			if((prevSchema.getRevisionOf() == null || prevSchema.getRevisionOf().equals("")) && (prevSchema.getHasRevisions() == null || prevSchema.getHasRevisions().size() == 1) ) {
				// do nothing
			}
			// handle existing versions 
			// case - latest version was deleted = isrevision and !hasrevision			
			else if(prevSchema.getRevisionOf() != null && !prevSchema.getRevisionOf().equals("") && (prevSchema.getHasRevisions() == null || prevSchema.getHasRevisions().size() == 1)) {
				// update the new latest 				
				String newLatestID = prevSchema.getRevisionOf();
				var latestModel = jenaService.getSchema(newLatestID);				
				var indexModel = mapper.mapToIndexModel(newLatestID, latestModel);
				openSearchIndexer.updateSchemaToIndex(indexModel);
			}
			// case - first version was deleted with revisions
			else if(prevSchema.getRevisionOf() == null && prevSchema.getHasRevisions() != null && !prevSchema.getHasRevisions().isEmpty()) {
				// remove revision of from the nextVersion
				String nextRevision = prevSchema.getHasRevisions().get(0); // should hold always with the condition above
				var versionModel = jenaService.getSchema(nextRevision);		
				Resource versionResource = versionModel.getResource(nextRevision);
				versionResource.removeAll(MSCR.PROV_wasRevisionOf);				
				jenaService.putToSchema(nextRevision, versionModel);
				var indexModel = mapper.mapToIndexModel(nextRevision, versionModel);
				openSearchIndexer.updateSchemaToIndex(indexModel);				
			}
			// case - in the middle
			else {
				String prevRevision = prevSchema.getRevisionOf();
				String nextRevision = prevSchema.getHasRevisions().get(0);
				
				var versionModel = jenaService.getSchema(nextRevision);		
				Resource versionResource = versionModel.getResource(nextRevision);
				versionResource.removeAll(MSCR.PROV_wasRevisionOf);				
				versionResource.addProperty(MSCR.PROV_wasRevisionOf, versionModel.createResource(prevRevision));
				jenaService.putToSchema(nextRevision, versionModel);
				var indexModel = mapper.mapToIndexModel(nextRevision, versionModel);
				openSearchIndexer.updateSchemaToIndex(indexModel);				
			}						
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}
		return ResponseEntity.ok(new DeleteResponseDTO("ok", pid));
    }

    @Tag(name = "Schema")
	@Operation(summary = "Get original file version of the schema (if available)", description = "If the result is only one file it is returned as is, but if the content includes multiple files they a returned as a zip file.")
	@ApiResponse(responseCode = "200", description = "")
	@GetMapping(path = "/schema/{pid}/original")
	public ResponseEntity<byte[]> exportOriginalFile(@PathVariable("pid") String pid) {
		return exportOriginalFileFrontend(pid, null);
	}
	
	@Hidden
	@GetMapping(path = "/schema/{pid}/{suffix}/original")
	public ResponseEntity<byte[]> exportOriginalFile(
			@PathVariable String pid,
			@PathVariable String suffix) {
		return exportOriginalFileFrontend(pid, suffix);
	}
	
    @Tag(name = "Frontend")
	@Operation(summary = "Get original file version of the schema (if available)", description = "If the result is only one file it is returned as is, but if the content includes multiple files they a returned as a zip file.")
	@ApiResponse(responseCode = "200", description = "")
	@GetMapping(path = "/frontend/schema/{pid}/original")
	public ResponseEntity<byte[]> exportOriginalFileFrontend(@PathVariable("pid") String pid) {
		return exportOriginalFileFrontend(pid, null);
	}	

	@Hidden
	@GetMapping(path = "/frontend/schema/{pid}/{suffix}/original")
	public ResponseEntity<byte[]> exportOriginalFileFrontend(
			@PathVariable String pid,
			@PathVariable String suffix) {
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);
			var ownerMapper = groupManagementService.mapOwner();
			SchemaInfoDTO schemaInfo = mapper.mapToSchemaDTO(pid, jenaService.getSchema(pid), null, ownerMapper);
			
			List<StoredFile> files = storageService.retrieveAllSchemaFiles(pid);
			return handleFileDownload(files, schemaInfo.getLabel().get("en") + "-" + schemaInfo.getVersionLabel(), schemaInfo.getFormat().name());
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}

	}
	
	@Tag(name = "Schema")
	@Operation(summary = "Download schema related file with a given id.")
	@ApiResponse(responseCode = "200")
	@GetMapping(path = "/schema/{pid}/files/{fileID}")
	public ResponseEntity<byte[]> downloadFile(@PathVariable String pid, @PathVariable String fileID,
			@RequestParam(name = "download", defaultValue = "false") String download) {
		return downloadFileFrontend(pid, null, fileID, download);
	}	
	
	@Hidden
	@GetMapping(path = "/schema/{pid}/{suffix}/files/{fileID}")
	public ResponseEntity<byte[]> downloadFile(
			@PathVariable String pid,
			@PathVariable String suffix, @PathVariable String fileID,
			@RequestParam(name = "download", defaultValue = "false") String download) {
		return downloadFileFrontend(pid, suffix, fileID, download);
	}

	@Tag(name = "Frontend")
	@Operation(summary = "Download schema related file with a given id.")
	@ApiResponse(responseCode = "200")
	@GetMapping(path = "/frontend/schema/{pid}/files/{fileID}")
	public ResponseEntity<byte[]> downloadFileFrontend(@PathVariable String pid, @PathVariable String fileID,
			@RequestParam(name = "download", defaultValue = "false") String download) {
		return downloadFileFrontend(pid, null, fileID, download);
	}

	@Hidden
	@GetMapping(path = "/frontend/schema/{pid}/{suffix}/files/{fileID}")
	public ResponseEntity<byte[]> downloadFileFrontend(
			@PathVariable String pid,
			@PathVariable String suffix, @PathVariable String fileID,
			@RequestParam(name = "download", defaultValue = "false") String download) {
		if (suffix != null) {
			pid = pid + "/" + suffix;
		}
		try {
			pid = PIDService.mapToInternal(pid);
			var ownerMapper = groupManagementService.mapOwner();
			SchemaInfoDTO schemaInfo = mapper.mapToSchemaDTO(pid, jenaService.getSchema(pid), null, ownerMapper);
			StoredFile file = storageService.retrieveFile(pid, Long.parseLong(fileID), MSCRType.SCHEMA);
			if (file == null) {
				throw new ResourceNotFoundException(pid + "@file=" + fileID);
			}
			return handleFileDownload(List.of(file), download, schemaInfo.getLabel().get("en") + "-" + schemaInfo.getVersionLabel(), schemaInfo.getFormat().name());
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}

	}

	@Tag(name = "Schema")
	@Operation(summary = "Delete file")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path = "/schema/{pid}/files/{fileID}", produces = APPLICATION_JSON_VALUE)
	public ResponseEntity<DeleteResponseDTO> deleteFile(@PathVariable String pid, @PathVariable Long fileID) {
		return deleteFileFrontend(pid, null, fileID);
	}
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path = "/schema/{pid}/{suffix}/files/{fileID}", produces = APPLICATION_JSON_VALUE)
	public ResponseEntity<DeleteResponseDTO> deleteFile(
			@PathVariable String pid, 
			@PathVariable String suffix,
			@PathVariable Long fileID) {
		return deleteFileFrontend(pid, suffix, fileID);
	}

	@Tag(name = "Frontend")
	@Operation(summary = "Delete file")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path = "/frontend/schema/{pid}/files/{fileID}", produces = APPLICATION_JSON_VALUE)
	public ResponseEntity<DeleteResponseDTO> deleteFileFrontend(@PathVariable String pid, @PathVariable Long fileID) {
		return deleteFileFrontend(pid, null, fileID);
	}
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path = "/frontend/schema/{pid}/{suffix}/files/{fileID}", produces = APPLICATION_JSON_VALUE)
	public ResponseEntity<DeleteResponseDTO> deleteFileFrontend(
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
		return ResponseEntity.ok(new DeleteResponseDTO("ok", pid + ":" + fileID));

	}

	@Tag(name = "Schema")
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
			if(jenaService.doesSchemaExist(pid+":content")) {
				var model = jenaService.getSchema(pid+":content");		
				StreamingResponseBody responseBody = httpResponseOutputStream -> {
					model.write(httpResponseOutputStream, "TURTLE");
				};
				return ResponseEntity.status(HttpStatus.OK).body(responseBody);
				
			}
			else {
				return ResponseEntity.noContent().build();
			}
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}
	}

	
	@Tag(name = "Frontend")
	@Operation(summary = "Search DTR type API")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@GetMapping(path = "/frontend/searchBasicInfoTypes", produces = "application/json")
	public ResponseEntity<String> dtrSearch(@RequestParam(name="query") String query, @RequestParam(name="page") int page, @RequestParam(name="pageSize") int pageSize) {
		try {
			if(userProvider.getUser().isAnonymous()) {
				throw new Exception("Authentication required.");
			}			
			String json = schemaService.dtrSearchBasicInfoTypes("name", query, page, pageSize);
			return new ResponseEntity<String>(json, HttpStatus.OK);
		} catch (Exception e) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Could not search DTR instance. " + e.getMessage());
		}
	}	

	@Tag(name = "Frontend")
	@Operation(summary = "Update property")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PatchMapping(path = "/frontend/schema/{schemaID}/properties", produces = "application/json")
	public UpdateResponseDTO updateProperty(@PathVariable(name = "schemaID") String schemaID, @RequestParam(name="target") String target, @RequestParam(name="datatype", defaultValue = "", required = false) String datatype, @RequestParam(name="valuesFrom", defaultValue = "", required = false) String valuesFrom) {
		return updateProperty(null, schemaID, target, datatype, valuesFrom);
	}	
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@PatchMapping(path = "/frontend/schema/{prefix}/{schemaID}/properties", produces = "application/json")
	public UpdateResponseDTO updateProperty(@PathVariable String prefix, @PathVariable String schemaID, @RequestParam String target, @RequestParam(name="datatype", defaultValue = "", required = false) String datatype, @RequestParam(name="valuesFrom", defaultValue = "", required = false) String valuesFrom) {
		if (prefix != null) {
			schemaID = prefix + "/" + schemaID;
		}
		try {
			schemaID = PIDService.mapToInternal(schemaID);
			Model model = jenaService.getSchema(schemaID);
			Model contentModel = jenaService.getSchema(schemaID+":content");
			check(authorizationManager.hasRightToModelMSCR(schemaID, model));
			if(model == null) {
				throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Schema " + schemaID + " not found");
			}
			// only available when format is MSCR
			String format = MapperUtils.propertyToString(model.getResource(schemaID), MSCR.format);
			if(!format.equals(SchemaFormat.MSCR.name())) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Schema format must be MSCR.");
			}
			String resourcePrefix = schemaID+"#root-Root-";
			String localName = target.substring((resourcePrefix).length());
			String encodedLocalName = URLEncoder.encode(localName).replaceAll("%2F", "/");
			String encodedTarget = resourcePrefix + encodedLocalName;
			Resource propResource = contentModel.getResource(encodedTarget);
			if(propResource == null) {
				throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Property " + target + " not in schema " + schemaID);
			}
			if(!datatype.equals("")) {
				Model propModel = schemaService.fetchAndMapDTRType(datatype);
				Resource datatypeResource = propModel.listSubjectsWithProperty(RDF.type).next();
				jenaService.putToSchema(datatypeResource.getURI(), propModel);
				schemaService.updatePropertyDataTypeFromDTR(contentModel, encodedTarget, datatypeResource.getURI());
				jenaService.putToSchema(schemaID+":content", contentModel);
				return new UpdateResponseDTO("Property " + target + " updated with data type " + datatype , schemaID);		
			}
			else if(!valuesFrom.equals("")) {
				if(!valuesFrom.equals("clear") && !jenaService.doesSchemaExist(valuesFrom)) {
					throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Given vocabulary does not exist in the system. Vocabulary URI:" + valuesFrom);
				}
				schemaService.updateValuesFrom(contentModel, encodedTarget, valuesFrom);
				jenaService.putToSchema(schemaID+":content", contentModel);
				return new UpdateResponseDTO("Property " + target + " updated with valuesFrom " + valuesFrom , schemaID);
			}
			return new UpdateResponseDTO("Nothing to do", schemaID);
			
			
		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}		
	}	
	
	@Tag(name = "Frontend")
	@Operation(summary = "Update data type of a SHACL property")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PatchMapping(path = "/frontend/dtr/schema/{schemaID}/properties", produces = "application/json")
	public UpdateResponseDTO updateDTRProperty(@PathVariable(name = "schemaID") String schemaID, @RequestParam(name="target") String target, @RequestParam(name="datatype", defaultValue = "", required = false) String datatype, @RequestParam(name="valuesFrom", defaultValue = "", required = false) String valuesFrom) {
		return updateProperty(null, schemaID, target, datatype, valuesFrom);
	}	
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@PatchMapping(path = "/frontend/dtr/schema/{prefix}/{schemaID}/properties", produces = "application/json")
	public UpdateResponseDTO updateDTRProperty(@PathVariable String prefix, @PathVariable String schemaID, @RequestParam String target, @RequestParam(name="datatype", defaultValue = "", required = false) String datatype, @RequestParam(name="valuesFrom", defaultValue = "", required = false) String valuesFrom) {
		return updateProperty(prefix, schemaID, target, datatype, valuesFrom);
	}
	
	@Tag(name = "Frontend")
	@Operation(summary = "Update root resource")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PatchMapping(path = "/frontend/schema/{schemaID}/rootResource", produces = "application/json")
	public UpdateResponseDTO updateRootResource(@PathVariable(name = "schemaID") String schemaID, @RequestParam(required = false, name="value") String rootResource) {
		return updateRootResource(null, schemaID, rootResource);
	}	
	
	@Hidden
	@SecurityRequirement(name = "Bearer Authentication")
	@PatchMapping(path = "/frontend/schema/{prefix}/{schemaID}/rootResource", produces = "application/json")
	public UpdateResponseDTO updateRootResource(@PathVariable String prefix, @PathVariable String schemaID, @RequestParam(required = false, name="value") String rootResource) {
		if (prefix != null) {
			schemaID = prefix + "/" + schemaID;
		}
		try {
			schemaID = PIDService.mapToInternal(schemaID);
			Model model = jenaService.getSchema(schemaID);			
			if(model == null) {
				throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Schema " + schemaID + " not found");
			}			
			check(authorizationManager.hasRightToModelMSCR(schemaID, model));

			String format = MapperUtils.propertyToString(model.getResource(schemaID), MSCR.format);
			if(!(format.equals(SchemaFormat.JSONSCHEMA.name()) || format.equals(SchemaFormat.XSD.name()) || format.equals(SchemaFormat.SKOSRDF.name()) || format.equals(SchemaFormat.MSCR.name()) )) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Update root resource endpoint is only available for JSONSchema, XSD, SKOSRDF and MSCR formats.");
			}
			// update metadata 
			schemaService.updateRootResourceMetadata(rootResource, schemaID, model);			
			jenaService.putToSchema(schemaID, model);
			return new UpdateResponseDTO("Updated root resource", schemaID);

		} catch (RuntimeException rex) {
			throw rex;
		} catch (Exception ex) {
			throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, ex.getMessage());
		}		
	}

}