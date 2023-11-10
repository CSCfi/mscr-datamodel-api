package fi.vm.yti.datamodel.api.v2.dto;

import com.fasterxml.jackson.databind.JsonNode;

public class CrosswalkEditorSchemaDTO {

	private SchemaInfoDTO metadata;
	private JsonNode content;
	
	public CrosswalkEditorSchemaDTO() {}

	public SchemaInfoDTO getMetadata() {
		return metadata;
	}

	public void setMetadata(SchemaInfoDTO metadata) {
		this.metadata = metadata;
	}

	public JsonNode getContent() {
		return content;
	}

	public void setContent(JsonNode content) {
		this.content = content;
	}
	
	
	
}
