package fi.vm.yti.datamodel.api.v2.dto;

public class FileMetadata {
	private String contentType;
	private int size;
	private long fileID;
	private String filename;
	
	
	public FileMetadata(String contentType, int size, long fileID, String filename) {
		this.contentType = contentType;
		this.size = size;
		this.fileID = fileID;
		this.filename = filename;
	}
	
	public String getContentType() {
		return this.contentType;
	}

	public long getFileID() {
		return this.fileID;
	}
	
	public int getSize() {
		return this.size;
	}
	
	public String getFilename() {
		return this.filename;
	}
	
}
