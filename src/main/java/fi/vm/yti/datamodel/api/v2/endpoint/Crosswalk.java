package fi.vm.yti.datamodel.api.v2.endpoint;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkDTO;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkFormat;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.mapper.CrosswalkMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.PIDService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFile;
import fi.vm.yti.datamodel.api.v2.service.impl.PostgresStorageService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("v2")
@Tag(name="Crosswalk")
public class Crosswalk {
	private static final Logger logger = LoggerFactory.getLogger(Crosswalk.class);

    private final AuthorizationManager authorizationManager;
    private final OpenSearchIndexer openSearchIndexer;
	private final PIDService PIDService;
	private final StorageService storageService;
    private final JenaService jenaService;
	private final CrosswalkMapper mapper;
	
	public Crosswalk(AuthorizationManager authorizationManager,
            OpenSearchIndexer openSearchIndexer,
            PIDService PIDService,
            PostgresStorageService storageService,
            JenaService jenaService,
            CrosswalkMapper mapper) {
		this.openSearchIndexer = openSearchIndexer;
		this.authorizationManager = authorizationManager;
		this.PIDService = PIDService;
		this.storageService = storageService;		
		this.jenaService = jenaService;
		this.mapper = mapper;
	}
	
	@Operation(summary = "Create crosswalk")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path="/crosswalk", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public CrosswalkInfoDTO createCrosswalk(@RequestBody CrosswalkDTO dto) {
		logger.info("Create Crosswalk {}", dto);
		check(authorizationManager.hasRightToAnyOrganization(Set.of(dto.getOrganization())));		
		final String PID = PIDService.mint(PIDType.HANDLE);

		Model jenaModel = mapper.mapToJenaModel(PID, dto);
		jenaService.putToCrosswalk(PID, jenaModel);
		
		//  TODO: add to opensearch indexing for crosswalks
		
		return mapper.mapToCrosswalkDTO(PID, jenaService.getCrosswalk(PID));
	}
	
	
	@Operation(summary = "Upload and associate a crosswalk description file to an existing crosswalk")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/crosswalk/{pid}/upload", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public CrosswalkInfoDTO uploadSCrosswalkFile(@PathVariable String pid, @RequestParam("contentType") String contentType,
			@RequestParam("file") MultipartFile file) throws Exception {
		Model metadataModel = jenaService.getCrosswalk(pid);
		CrosswalkInfoDTO dto = mapper.mapToCrosswalkDTO(pid, metadataModel);
		check(authorizationManager.hasRightToModel(pid, metadataModel));
		
		try {
			if(EnumSet.of(CrosswalkFormat.CSV, CrosswalkFormat.MSCR, CrosswalkFormat.SSSOM, CrosswalkFormat.XSLT).contains(dto.getFormat())) {
				storageService.storeCrosswalkFile(pid, contentType, file.getBytes());
			}
			else {
				throw new Exception("Unsupported crosswalk description format. Supported formats are: " + String.join(", ", Arrays.toString(CrosswalkFormat.values()) ));
			}
		
		
		} catch (Exception ex) {
			throw new RuntimeException("Error occured while ingesting file based crosswalk description", ex);
		}
		return mapper.mapToCrosswalkDTO(pid, metadataModel);
		
	}
	
    @Operation(summary = "Get a crosswalk metadata")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(value = "/crosswalk/{pid}", produces = APPLICATION_JSON_VALUE)
    public CrosswalkInfoDTO getSchemaMetadata(@PathVariable String pid){
    	var jenaModel = jenaService.getSchema(pid);
    	return mapper.mapToCrosswalkDTO(pid, jenaModel);
    }
    
    @Operation(summary = "Get original file version of the crosswalk (if available)", description = "If the result is only one file it is returned as is, but if the content includes multiple files they a returned as a zip file.")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(path = "/crosswalk/{pid}/original")
    public ResponseEntity<byte[]> exportOriginalFile(@PathVariable String pid) {
    	List<StoredFile> files = storageService.retrieveAllCrosswalkFiles(pid);
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
}
