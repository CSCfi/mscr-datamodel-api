package fi.vm.yti.datamodel.api.v2.endpoint;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.jena.rdf.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MSCRCommonMetadata;
import fi.vm.yti.datamodel.api.v2.dto.MSCRState;
import fi.vm.yti.datamodel.api.v2.dto.MSCRVisibility;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import fi.vm.yti.datamodel.api.v2.mapper.MimeTypes;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFile;


public abstract class BaseMSCRController {

	private static final Logger logger = LoggerFactory.getLogger(BaseMSCRController.class);
	@io.swagger.v3.oas.annotations.media.Schema(name = "Content actions", description = "")
	public enum CONTENT_ACTION { create, copyOf, revisionOf, mscrCopyOf, update, delete }
	

	protected void validateActionParams(Object dto, CONTENT_ACTION action, String target) {
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
	protected ResponseEntity<byte[]> handleFileDownload(List<StoredFile> files, String pid, String format) {
		return handleFileDownload(files, "true", pid, format);
	}
	protected ResponseEntity<byte[]> handleFileDownload(List<StoredFile> files, String download, String pid, String format) {
    	if (files.isEmpty()) {
    		return ResponseEntity.notFound().build();   				
    	}
    	
    	if (files.size() == 1) {
    		StoredFile file = files.get(0);
    		if("false".equals(download)) {
        		return ResponseEntity.ok()    				
        				.contentType(MediaType.parseMediaTypes(file.contentType()).get(0))
        				.body(file.data());		    	
    		}
    		else {
        		return ResponseEntity.ok()    				
        				.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + pid + getFileExtensionFromFormat(format))
        				.contentType(MediaType.parseMediaTypes(file.contentType()).get(0))
        				.body(file.data());		    	
    			
    		}
    	}
    	else {
    		ByteArrayOutputStream baos = new ByteArrayOutputStream();
    		ZipOutputStream zipOut = new ZipOutputStream(baos);
    		try {
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
    		}catch(Exception ex) {
    			logger.error(ex.getMessage());
    			return ResponseEntity.internalServerError().build();    		
    		}    		
    	}		
	}
	
	private String getFileExtensionFromFormat(String format) {
		
		switch (format) {
		case "CSV": {			
			return ".csv";
		}
		case "XSD": {			
			return ".xsd";
		}
		case "JSONSCHEMA": {			
			return ".json";
		}
		case "RDFS": {			
			return ".ttl";
		}
		case "SHACL": {			
			return ".ttl";
		}
		case "OWL": {			
			return ".ttl";
		}
		case "SKOSRDF": {			
			return ".ttl";
		}
		case "PDF": {			
			return ".pdf";
		}
		case "XML": {			
			return ".xml";
		}
		case "MSCR": {			
			return ".ttl";
		}
		case "ENUM": {			
			return ".json";
		}
		case "XSLT": {			
			return ".xslt";
		}
		case "SSSOM": {			
			return ".csv";
		}		
		default:
			throw new IllegalArgumentException("Unexpected value: " + format);
		}
	}
	
	protected String generateFilename(String PID, String contentType) {		
		if(contentType != null && !contentType.equals("") && contentType.indexOf("/") > 0) {
			return PID + "." + contentType.substring(contentType.lastIndexOf("/") + 1);
		}
		return PID;
	}
	
	protected void checkVisibility(MSCRCommonMetadata dto) {
		if(dto != null && dto.getState() != MSCRState.DRAFT && dto.getVisibility() != MSCRVisibility.PUBLIC) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Only DRAFT content can have non public visibility");
		}
		
	}
	
	protected void checkState(MSCRCommonMetadata prev, MSCRCommonMetadata next) {
		if(next == null) {
			return;
		}
		MSCRState newState = next.getState();
		checkState(prev, newState);
	}
	
	protected void checkState(MSCRCommonMetadata prev, MSCRState newState) {
		if(prev == null) {
			// content
			if(newState != MSCRState.DRAFT && newState != MSCRState.PUBLISHED && newState != MSCRState.DEPRECATED) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Newly created content must be in one of the following states: DRAFT, PUBLISHED, DEPRECATED");
			}
		}
		if(prev != null && prev.getState() != newState) {
			if(prev.getState() == MSCRState.DRAFT && newState != MSCRState.PUBLISHED) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid state change. Allowed transition: DRAFT -> PUBLIShED");
			}
			if(prev.getState() == MSCRState.PUBLISHED && (newState != MSCRState.INVALID && newState != MSCRState.DEPRECATED)) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid state change. Allowed transitions: PUBLISHED -> INVALID, PUBLISHED -> DEPRECATED");
			}		
			if(prev.getState() == MSCRState.DEPRECATED && newState != MSCRState.REMOVED) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid state change. Allowed transition: DEPRECATED -> REMOVED");
			}
			if(prev.getState() == MSCRState.INVALID && newState != MSCRState.REMOVED) {
				throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid state change. Allowed transition: INVALID -> REMOVED");
			}	
		}
	}
	
	protected boolean isEditable(Model model, String pid) {
		var modelResource = model.getResource(pid);
		var state = MapperUtils.propertyToString(modelResource,  MSCR.state);
		if(MSCRState.DRAFT.name().equals(state)) {
			return true;
		}
		return false;
		
	}
	
}
