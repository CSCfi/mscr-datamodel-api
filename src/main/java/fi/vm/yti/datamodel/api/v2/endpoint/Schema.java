package fi.vm.yti.datamodel.api.v2.endpoint;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.util.List;
import java.util.Set;

import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
import fi.vm.yti.datamodel.api.v2.mapper.SchemaMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.PIDService;
import fi.vm.yti.datamodel.api.v2.service.SchemaService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFile;
import fi.vm.yti.datamodel.api.v2.service.impl.PostgresStorageService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("v2")
@Tag(name="Schema")
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
	
	public Schema(JenaService jenaService,
            AuthorizationManager authorizationManager,
            OpenSearchIndexer openSearchIndexer,
            SchemaMapper schemaMapper,
            SchemaService schemaService,
            PIDService PIDService,
            PostgresStorageService storageService) {
		
		this.jenaService = jenaService;
		this.openSearchIndexer = openSearchIndexer;
		this.authorizationManager = authorizationManager;
		this.mapper = schemaMapper;
		this.schemaService = schemaService;
		this.PIDService = PIDService;
		this.storageService = storageService;
		
	}

    @Operation(summary = "Create schema")
    @ApiResponse(responseCode = "200", description = "")
    @SecurityRequirement(name ="Bearer Authentication")
	@PutMapping(path="/schema", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public SchemaInfoDTO createSchema(@RequestBody SchemaDTO schemaDTO) {
		logger.info("Create Schema {}", schemaDTO);
		check(authorizationManager.hasRightToAnyOrganization(Set.of(schemaDTO.getOrganization())));		
		final String PID = PIDService.mint(PIDType.HANDLE);

		var jenaModel = mapper.mapToJenaModel(PID, schemaDTO);
		jenaService.putToSchema(PID, jenaModel);
		
		//  TODO: add to opensearch indexing for schemas
		
		return mapper.mapToSchemaDTO(PID, jenaService.getSchema(PID));
		
	}


    
    @Operation(summary = "Get a schema metadata")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(value = "/schema/{pid}", produces = APPLICATION_JSON_VALUE)
    public SchemaInfoDTO getSchemaMetadata(@PathVariable String pid){
    	var jenaModel = jenaService.getSchema(pid);
    	return mapper.mapToSchemaDTO(pid, jenaModel);
    }
    
    @Operation(summary = "Upload and associate a schema description file to an existing schema")
    @ApiResponse(responseCode = "200", description = "")
    @SecurityRequirement(name ="Bearer Authentication")
    @PutMapping(path="/schema/{pid}/upload", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
    public SchemaInfoDTO uploadSchemaFile(@PathVariable String pid, @RequestParam("contentType") String contentType, @RequestParam("file") MultipartFile file) {
    	var metadataModel = jenaService.getSchema(pid);
    	SchemaInfoDTO schemaDTO = mapper.mapToSchemaDTO(pid, metadataModel);    	
		check(authorizationManager.hasRightToAnyOrganization(Set.of(schemaDTO.getOrganization())));		
    	
    	try {
    		// transform to the internal format
    		if(schemaDTO.getFormat() != SchemaFormat.JSONSCHEMA) {
    			throw new RuntimeException("Unsupported schema description format");
    		}
    		Model schemaModel = schemaService.transformJSONSchemaToInternal(pid, file.getBytes());
    		schemaModel.add(metadataModel);
    		jenaService.updateSchema(pid, schemaModel);
    		// store the original file    		
    		storageService.storeSchemaFile(pid, contentType, file.getBytes());
    		
    		
    	}catch(Exception ex) {
    		throw new RuntimeException("Error occured while ingesting file based schema description", ex);
    	}
    	return mapper.mapToSchemaDTO(pid, metadataModel);
    }
    
    
    @Operation(summary = "Get original file version of the schema (if available)", description = "If the result is only one file it is returned as is, but if the content includes multiple files they a returned as a zip file.")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(path = "/schema/{pid}/original")
    public ResponseEntity<byte[]> exportOriginalFile(@PathVariable String pid) {
    	List<StoredFile> files = storageService.retrieveAllSchemaFiles(pid);
    	if(files.size() == 1) {
    		StoredFile file = files.get(0);
			return ResponseEntity.ok()
					.contentType(org.springframework.http.MediaType.parseMediaTypes(file.contentType()).get(0))
					.body(file.data());					
    	}
    	else {
    		return null;
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
