package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Date;
import java.util.Map;

public class Revision {
	private String pid;
	private Date created;
	private Map<String, String> label;
	private String versionLabel;
	
	public Revision(String pid, java.util.Date created, Map<String, String> label, String versionLabel) {
		this.pid = pid;
		this.created = created;
		this.label = label;
		this.versionLabel  = versionLabel;
	}
	public String getPid() {
		return pid;
	}
	public void setPid(String pid) {
		this.pid = pid;
	}
	public Date getCreated() {
		return created;
	}
	public void setCreated(Date created) {
		this.created = created;
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
	
	
	
}
