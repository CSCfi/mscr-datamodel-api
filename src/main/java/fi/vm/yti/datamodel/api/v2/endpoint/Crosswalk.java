package fi.vm.yti.datamodel.api.v2.endpoint;

import static fi.vm.yti.security.AuthorizationException.check;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
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
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.endpoint.error.ResourceNotFoundException;
import fi.vm.yti.datamodel.api.v2.mapper.CrosswalkMapper;
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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("v2")
@Tag(name="Crosswalk")
@Validated
public class Crosswalk {
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
	
	private CrosswalkInfoDTO createCrosswalkMetadata(CrosswalkDTO dto) {
		check(authorizationManager.hasRightToAnyOrganization(dto.getOrganizations()));		
		final String PID = PIDService.mint(PIDType.HANDLE);

		Model jenaModel = mapper.mapToJenaModel(PID, dto, userProvider.getUser());
		jenaService.putToCrosswalk(PID, jenaModel);
		
		var indexModel = mapper.mapToIndexModel(PID, jenaModel);
        openSearchIndexer.createCrosswalkToIndex(indexModel);

        var userMapper = groupManagementService.mapUser();
		
		return mapper.mapToCrosswalkDTO(PID, jenaService.getCrosswalk(PID), userMapper);
	}
	
	private CrosswalkInfoDTO addFileToCrosswalk(String pid, String contentType, MultipartFile file) {
		Model metadataModel = jenaService.getCrosswalk(pid);
		var hasRightsToModel = authorizationManager.hasRightToModel(pid, metadataModel);
		check(hasRightsToModel);
        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;

		CrosswalkInfoDTO dto = mapper.mapToCrosswalkDTO(pid, metadataModel, userMapper);
		
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
		return mapper.mapToCrosswalkDTO(pid, metadataModel, userMapper);
	}
	
	@Operation(summary = "Create crosswalk")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path="/crosswalk", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public CrosswalkInfoDTO createCrosswalk(@ValidCrosswalk @RequestBody CrosswalkDTO dto) {
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
	public CrosswalkInfoDTO createSchemaFull(@ValidCrosswalk @RequestParam("metadata") CrosswalkDTO dto,
			@RequestParam("file") MultipartFile file) {
		logger.info("Create Crosswalk {}", dto);
		CrosswalkInfoDTO infoDto = createCrosswalkMetadata(dto);
		return addFileToCrosswalk(infoDto.getPID(), file.getContentType(), file);
		
	}	
	
    @Operation(summary = "Modify crosswalk")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(description = "The JSON data for the new crosswalk node")
    @ApiResponse(responseCode = "200", description = "The JSON of the update model, basically the same as the request body.")
    @SecurityRequirement(name = "Bearer Authentication")
    @PostMapping(path = "/crosswalk/{pid}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
    public void updateModel(@ValidCrosswalk @RequestBody CrosswalkDTO dto,
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
		var hasRightsToModel = authorizationManager.hasRightToModel(pid, jenaModel);
        var userMapper = hasRightsToModel ? groupManagementService.mapUser() : null;

    	return mapper.mapToCrosswalkDTO(pid, jenaModel, userMapper);
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
    
	@Operation(summary = "Create a mapping")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@PutMapping(path="/crosswalk/{pid}/mapping", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public MappingDTO createMapping(@ValidMapping @RequestBody MappingDTO dto, @PathVariable String pid) {
		logger.info("Create Mapping {} for crosswalk {}", dto, pid);
        var crosswalkModel = jenaService.getCrosswalk(pid);
        if(crosswalkModel == null){
            throw new ResourceNotFoundException(pid);
        }

        check(authorizationManager.hasRightToModel(pid, crosswalkModel));
        
		final String mappingPID = PIDService.mintPartIdentifier(pid);

		Model mappingModel = mappingMapper.mapToJenaModel(mappingPID, dto, pid);
		jenaService. putToCrosswalk(mappingPID, mappingModel);
		Resource crosswalkResource = crosswalkModel.getResource(pid);
		crosswalkResource.addProperty(MSCR.mappings, ResourceFactory.createResource(mappingPID));
		jenaService.putToCrosswalk(pid, crosswalkModel);
		return mappingMapper.mapToMappingDTO(mappingPID, mappingModel);		
	}
	
	@Operation(summary = "Get a mapping")
	@ApiResponse(responseCode = "200")	
	@GetMapping(path="/crosswalk/{pid}/mapping/{mappingPID}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public MappingDTO getMapping(@PathVariable String pid, @PathVariable String mappingPID) {
		logger.info("Get Mapping {} for crosswalk {}", mappingPID, pid);
		// TODO: check that crosswalk exists
		var mappingModel = jenaService.getCrosswalk(mappingPID);
        if(mappingModel == null){
            throw new ResourceNotFoundException(mappingPID);
        }
		
		return mappingMapper.mapToMappingDTO(mappingPID, mappingModel);		
	}
	
	@Operation(summary = "Delete a mapping")
	@ApiResponse(responseCode = "200")
	@SecurityRequirement(name = "Bearer Authentication")
	@DeleteMapping(path="/crosswalk/{pid}/mapping/{mappingPID}", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public void deleteMapping(@PathVariable String pid, @PathVariable String mappingPID) {
		logger.info("Delete Mapping {} for crosswalk {}", mappingPID, pid);
		// TODO: check that crosswalk exists
		var crosswalkModel = jenaService.getCrosswalk(pid);
        if(crosswalkModel == null){
            throw new ResourceNotFoundException(pid);
        }
        crosswalkModel.remove(crosswalkModel.getResource(pid), MSCR.mappings, crosswalkModel.getResource(mappingPID));
		jenaService.deleteFromCrosswalk(mappingPID);		
				
	}
		
	@Operation(summary = "Get a mappings for a crosswalk")
	@ApiResponse(responseCode = "200")	
	@GetMapping(path="/crosswalk/{pid}/mapping", produces = APPLICATION_JSON_VALUE, consumes = APPLICATION_JSON_VALUE)
	public List<MappingDTO> getMappings(@PathVariable String pid) {
		logger.info("Get Mappings for crosswalk {}", pid);
		// TODO: check that crosswalk exists
		var crosswalkModel = jenaService.getCrosswalk(pid);
		List<MappingDTO> mappings = new ArrayList<MappingDTO>();
		NodeIterator i = crosswalkModel.listObjectsOfProperty(crosswalkModel.getResource(pid), MSCR.mappings);
		while(i.hasNext()) {
			Resource mappingResource = i.next().asResource();			
			MappingDTO dto = mappingMapper.mapToMappingDTO(mappingResource.getURI(), crosswalkModel);
			mappings.add(dto);
		}
		
		return mappings;
	}	
}
