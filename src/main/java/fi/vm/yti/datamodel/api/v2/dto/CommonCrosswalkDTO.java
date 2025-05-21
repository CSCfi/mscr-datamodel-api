package fi.vm.yti.datamodel.api.v2.dto;



public class CommonCrosswalkDTO extends MSCRModelDTO  {

	private CrosswalkFormat format;	
	private String sourceSchema;
	private String targetSchema;
	private String sourceURL;
	public CrosswalkFormat getFormat() {
		return format;
	}
	public void setFormat(CrosswalkFormat format) {
		this.format = format;
	}
	public String getSourceSchema() {
		return sourceSchema;
	}
	public void setSourceSchema(String sourceSchema) {
		this.sourceSchema = sourceSchema;
	}
	public String getTargetSchema() {
		return targetSchema;
	}
	public void setTargetSchema(String targetSchema) {
		this.targetSchema = targetSchema;
	}
	public String getSourceURL() {
		return sourceURL;
	}
	public void setSourceURL(String sourceURL) {
		this.sourceURL = sourceURL;
	}
	
	public String getMediaType() {
		return getFormat().name();
	}
	
	public void setMediaType(String format) {
		setFormat(CrosswalkFormat.valueOf(format));
	}
}
