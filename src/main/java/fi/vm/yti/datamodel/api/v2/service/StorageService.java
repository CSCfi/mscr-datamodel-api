package fi.vm.yti.datamodel.api.v2.service;

import java.util.List;

import fi.vm.yti.datamodel.api.v2.dto.MSCRType;

public interface StorageService {

	public int storeSchemaFile(String schemaPID, String contentType, byte[] data);
	public int storeCrosswalkFile(String schemaPID, String contentType, byte[] data);
	
	public StoredFile retrieveFile(String pid, long fileID, MSCRType type);
	public List<StoredFile> retrieveAllFiles(String pid, MSCRType type);
	public List<StoredFileMetadata> retrieveAllFilesMetadata(String pid, MSCRType type);
	
	public record StoredFile(String contentType, byte[] data, long fileID, MSCRType type) {}
	public record StoredFileMetadata(String contentType, int dataSize, long fileID, MSCRType type) {}

}
