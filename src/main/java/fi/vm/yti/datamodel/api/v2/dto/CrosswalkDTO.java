package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class CrosswalkDTO {

	private CrosswalkFormat format;
	private String aggregationKey;
	private Status status;	
	private Map<String, String> label = Map.of();
	private Map<String, String> description = Map.of();
	private Set<String> languages = Set.of();
	private UUID organization;
	
	public CrosswalkFormat getFormat() {
		return format;
	}

	public void setFormat(CrosswalkFormat type) {
		this.format = type;
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public Map<String, String> getLabel() {
		return label;
	}

	public void setLabel(Map<String, String> label) {
		this.label = label;
	}

	public Map<String, String> getDescription() {
		return description;
	}

	public void setDescription(Map<String, String> description) {
		this.description = description;
	}

	public Set<String> getLanguages() {
		return languages;
	}

	public void setLanguages(Set<String> languages) {
		this.languages = languages;
	}

	public UUID getOrganization() {
		return organization;
	}

	public void setOrganization(UUID organization) {
		this.organization = organization;
	}
	
    public String getAggregationKey() {
		return aggregationKey;
	}

	public void setAggregationKey(String aggregationKey) {
		this.aggregationKey = aggregationKey;
	}

	@Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
