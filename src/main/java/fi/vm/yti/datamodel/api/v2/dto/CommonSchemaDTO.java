package fi.vm.yti.datamodel.api.v2.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CommonSchemaDTO extends MSCRModelDTO {

	private String namespace;
	private String sourceURL;
	private SchemaFormat format;
	private SchemaFormat originalFormat;
	public String getNamespace() {
		return namespace;
	}
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	public String getSourceURL() {
		return sourceURL;
	}
	public void setSourceURL(String sourceURL) {
		this.sourceURL = sourceURL;
	}
	public SchemaFormat getFormat() {
		return format;
	}
	public void setFormat(SchemaFormat format) {
		this.format = format;
	}
	public SchemaFormat getOriginalFormat() {
		return originalFormat;
	}
	public void setOriginalFormat(SchemaFormat originalFormat) {
		this.originalFormat = originalFormat;
	}
	
	public String getMediaType() {
		return getFormat().name();
	}
	
	public void setMediaType(String format) {
		setFormat(SchemaFormat.valueOf(format));
	}
	
}
