package fi.vm.yti.datamodel.api.v2.endpoint;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.datamodel.api.security.AuthorizationManager;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkDTO;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkFormat;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.CrosswalkMapper;
import fi.vm.yti.datamodel.api.v2.opensearch.index.OpenSearchIndexer;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.PIDService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFile;
import fi.vm.yti.datamodel.api.v2.service.impl.PostgresStorageService;
import fi.vm.yti.security.AuthenticatedUserProvider;
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
	private final AuthenticatedUserProvider userProvider;
	
	public Crosswalk(AuthorizationManager authorizationManager,
            OpenSearchIndexer openSearchIndexer,
            PIDService PIDService,
            PostgresStorageService storageService,
            JenaService jenaService,
            CrosswalkMapper mapper,
            AuthenticatedUserProvider userProvider) {
		this.openSearchIndexer = openSearchIndexer;
		this.authorizationManager = authorizationManager;
		this.PIDService = PIDService;
		this.storageService = storageService;		
		this.jenaService = jenaService;
		this.mapper = mapper;
		this.userProvider = userProvider;
	}
	
	private CrosswalkInfoDTO createCrosswalkMetadata(CrosswalkDTO dto) {
		check(authorizationManager.hasRightToAnyOrganization(dto.getOrganizations()));		
		final String PID = PIDService.mint(PIDType.HANDLE);

		Model jenaModel = mapper.mapToJenaModel(PID, dto);
		jenaService.putToCrosswalk(PID, jenaModel);
		
		var indexModel = mapper.mapToIndexModel(PID, jenaModel);
        openSearchIndexer.createCrosswalkToIndex(indexModel);

		
		return mapper.mapToCrosswalkDTO(PID, jenaService.getCrosswalk(PID));
	}
	
	private CrosswalkInfoDTO addFileToCrosswalk(String pid, String contentType, MultipartFile file) {
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
	
	@Operation(summary = "Create crosswalk")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path="/crosswalk", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public CrosswalkInfoDTO createCrosswalk(@RequestBody CrosswalkDTO dto) {
		logger.info("Create Crosswalk {}", dto);
		return createCrosswalkMetadata(dto);
	}
	
	
	@Operation(summary = "Upload and associate a crosswalk description file to an existing crosswalk")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/crosswalk/{pid}/upload", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public CrosswalkInfoDTO uploadSCrosswalkFile(@PathVariable String pid, @RequestParam("contentType") String contentType,
			@RequestParam("file") MultipartFile file) throws Exception {
		return addFileToCrosswalk(pid, contentType, file);
		
	}
	
	@Operation(summary = "Create crosswalk by uploading metadata and files in one multipart request")
	@ApiResponse(responseCode = "200", description = "")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path = "/crosswalkFull", produces = APPLICATION_JSON_VALUE, consumes = "multipart/form-data")
	public CrosswalkInfoDTO createSchemaFull(@RequestParam("metadata") String metadataString,
			@RequestParam("file") MultipartFile file) {
		
		CrosswalkDTO dto = null;
		try {
			ObjectMapper mapper = new ObjectMapper();
			dto = mapper.readValue(metadataString, CrosswalkDTO.class);

		}catch(Exception ex) {
			ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Could not parse metadata string." + ex.getMessage());
			
		}
		logger.info("Create Crosswalk {}", dto);
		CrosswalkInfoDTO infoDto = createCrosswalkMetadata(dto);
		return addFileToCrosswalk(infoDto.getPID(), file.getContentType(), file);
		
	}	
	
    @Operation(summary = "Modify crosswalk")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new crosswalk node")
    @ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
    @PostMapping(path = "/crosswalk/{pid}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public void updateModel(@RequestBody CrosswalkDTO dto,
                            @PathVariable String pid) {
        logger.info("Updating crosswalk {}", dto);

        var oldModel = jenaService.getCrosswalk(pid);
        if(oldModel == null){
            throw new ResourceNotFoundException(pid);
        }

        check(authorizationManager.hasRightToModel(pid, oldModel));

        var jenaModel = mapper.mapToUpdateJenaModel(pid, dto, oldModel, userProvider.getUser());

        jenaService.putToCrosswalk(pid, jenaModel);


        var indexModel = mapper.mapToIndexModel(pid, jenaModel);
        openSearchIndexer.updateCrosswalkToIndex(indexModel);
    }	
	
    @Operation(summary = "Get a crosswalk metadata")
    @ApiResponse(responseCode = "200", description = "")
    @GetMapping(value = "/crosswalk/{pid}", produces = APPLICATION_JSON_VALUE)
    public CrosswalkInfoDTO getCrosswalkMetadata(@PathVariable String pid){
    	var jenaModel = jenaService.getCrosswalk(pid);
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
