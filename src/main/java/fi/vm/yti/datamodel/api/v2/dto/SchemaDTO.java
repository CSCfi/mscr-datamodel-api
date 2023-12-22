package fi.vm.yti.datamodel.api.v2.dto;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class SchemaDTO extends DataModelDTO implements MSCRCommonMetadata {

	private MSCRState state;
	private MSCRVisibility visibility;

	private SchemaFormat format;
	private String namespace;
	private String versionLabel;
	
	public MSCRVisibility getVisibility() {
		return visibility;
	}
	public void setVisibility(MSCRVisibility visibility) {
		this.visibility = visibility;
	}
	
	public MSCRState getState() {
		return state;
	}

	public void setState(MSCRState state) {
		this.state = state;
	}

	public SchemaFormat getFormat() {
		return format;
	}

	public void setFormat(SchemaFormat type) {
		this.format = type;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getVersionLabel() {
		return versionLabel;
	}

	public void setVersionLabel(String versionLabel) {
		this.versionLabel = versionLabel;
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
