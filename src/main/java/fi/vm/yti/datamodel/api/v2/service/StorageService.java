package fi.vm.yti.datamodel.api.v2.service;

import java.util.List;

import fi.vm.yti.datamodel.api.v2.dto.MSCRType;

public interface StorageService {

	public int storeSchemaFile(String schemaPID, String contentType, byte[] data);
	public int storeCrosswalkFile(String schemaPID, String contentType, byte[] data);
	
	public StoredFile retrieveFile(String pid, long fileID, MSCRType type);
	
	public List<StoredFile> retrieveAllSchemaFiles(String pid);
	public List<StoredFile> retrieveAllCrosswalkFiles(String pid);
	
	public List<StoredFileMetadata> retrieveAllSchemaFilesMetadata(String pid);
	public List<StoredFileMetadata> retrieveAllCrosswalkFilesMetadata(String pid);
	
	public record StoredFile(String contentType, byte[] data, long fileID, MSCRType type) {}
	public record StoredFileMetadata(String contentType, int dataSize, long fileID, MSCRType type) {}

}
