package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Map;

public class Variant {
	private String pid;
	private Map<String, String> label;
	private String versionLabel;
	private String aggregationKey;

	public Variant(String pid, Map<String, String> label, String versionLabel, String aggregationKey) {
		this.pid = pid;
		this.label = label;
		this.versionLabel  = versionLabel;
		this.aggregationKey = aggregationKey;

	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public Map<String, String> getLabel() {
		return label;
	}

	public void setLabel(Map<String, String> label) {
		this.label = label;
	}

	public String getVersionLabel() {
		return versionLabel;
	}

	public void setVersionLabel(String versionLabel) {
		this.versionLabel = versionLabel;
	}

	public String getAggregationKey() {
		return aggregationKey;
	}

	public void setAggregationKey(String aggregationKey) {
		this.aggregationKey = aggregationKey;
	}
	
	
}
