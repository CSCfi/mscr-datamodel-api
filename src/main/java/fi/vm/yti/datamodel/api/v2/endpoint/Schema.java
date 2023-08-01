package fi.vm.yti.datamodel.api.v2.endpoint;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.dto.SchemaDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.MimeTypes;
import fi.vm.yti.datamodel.api.v2.mapper.SchemaMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.service.JSONValidationService;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.PIDService;
import fi.vm.yti.datamodel.api.v2.service.SchemaService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFile;
import fi.vm.yti.datamodel.api.v2.service.ValidationRecord;
import fi.vm.yti.datamodel.api.v2.service.impl.PostgresStorageService;
import fi.vm.yti.security.AuthenticatedUserProvider;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipOutputStream;
import java.util.zip.ZipEntry;

@RestController
@RequestMapping("v2")
@Tag(name = "Schema")
//@Validated
public class Schema {

	private static final Logger logger = LoggerFactory.getLogger(Schema.class);

	private final AuthorizationManager authorizationManager;

	private final OpenSearchIndexer openSearchIndexer;

	private final JenaService jenaService;

	private final SchemaMapper mapper;

	private final SchemaService schemaService;

	private final PIDService PIDService;
	
	private final StorageService storageService;	
	
	private final AuthenticatedUserProvider userProvider;

	public Schema(JenaService jenaService,
            AuthorizationManager authorizationManager,
            OpenSearchIndexer openSearchIndexer,
            SchemaMapper schemaMapper,
            SchemaService schemaService,
            PIDService PIDService,
            PostgresStorageService storageService,
            AuthenticatedUserProvider userProvider) {
		
		this.jenaService = jenaService;
		this.openSearchIndexer = openSearchIndexer;
		this.authorizationManager = authorizationManager;
		this.mapper = schemaMapper;
		this.schemaService = schemaService;
		this.PIDService = PIDService;
		this.storageService = storageService;
		this.userProvider = userProvider;
	}

	@Operation(summary = "Create schema")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schema", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO createSchema(@RequestBody SchemaDTO schemaDTO) {
		logger.info("Create Schema {}", schemaDTO);
		check(authorizationManager.hasRightToAnyOrganization(schemaDTO.getOrganizations()));		
		final String PID = PIDService.mint(PIDType.HANDLE);

		var jenaModel = mapper.mapToJenaModel(PID, schemaDTO);
		jenaService.putToSchema(PID, jenaModel);
		
		var indexModel = mapper.mapToIndexModel(PID, jenaModel);
        openSearchIndexer.createSchemaToIndex(indexModel);

        return mapper.mapToSchemaDTO(PID, jenaService.getSchema(PID));
		
	}
    
	@Operation(summary = "Upload and associate a schema description file to an existing schema")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/schema/{pid}/upload", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public SchemaInfoDTO uploadSchemaFile(@PathVariable String pid, @RequestParam("contentType") String contentType,
			@RequestParam("file") MultipartFile file) throws Exception {
		Model metadataModel = jenaService.getSchema(pid);
		SchemaInfoDTO schemaDTO = mapper.mapToSchemaDTO(pid, metadataModel);
		check(authorizationManager.hasRightToModel(pid, metadataModel));

		try {
			byte[] fileInBytes = file.getBytes();
			if (schemaDTO.getFormat() == SchemaFormat.JSONSCHEMA) {
				ValidationRecord validationRecord = JSONValidationService.validateJSONSchema(fileInBytes);

				boolean isValidJSONSchema = validationRecord.isValid();
				List<String> validationMessages = validationRecord.validationOutput();

				if (isValidJSONSchema) {
					Model schemaModel = schemaService.transformJSONSchemaToInternal(pid, fileInBytes);
					schemaModel.add(metadataModel);
					jenaService.updateSchema(pid, schemaModel);
					storageService.storeSchemaFile(pid, contentType, file.getBytes());
				} else {
					String exceptionOutput = String.join("\n", validationMessages);
					throw new Exception(exceptionOutput);
				}

			} else {
				throw new RuntimeException(String.format("Unsupported schema description format: %s not supported",
						schemaDTO.getFormat()));
			}

		} catch (Exception ex) {
			throw new RuntimeException("Error occured while ingesting file based schema description", ex);
		}
		return mapper.mapToSchemaDTO(pid, metadataModel);
	}
  
    @Operation(summary = "Modify schema")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new schema node")
    @ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
    @PostMapping(path = "/schema/{pid}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public void updateModel(@RequestBody SchemaDTO schemaDTO,
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
    public SchemaInfoDTO getSchemaMetadata(@PathVariable String pid){
    	var jenaModel = jenaService.getSchema(pid);
    	return mapper.mapToSchemaDTO(pid, jenaModel);
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