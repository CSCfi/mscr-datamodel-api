package fi.vm.yti.datamodel.api.v2.dto;

import java.util.ArrayList;
import java.util.List;


public class SchemaPart {
	private String path;		
	private boolean resolved;
	private List<SchemaPart> hasPart = new ArrayList<SchemaPart>();
	
	public SchemaPart(String path, boolean resolved) {
		this.resolved = resolved;
		this.path = path;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public boolean isResolved() {
		return resolved;
	}

	public void setResolved(boolean resolved) {
		this.resolved = resolved;
	}

	public List<SchemaPart> getHasPart() {
		return hasPart;
	}

	public void setHasPart(List<SchemaPart> hasPart) {
		this.hasPart = hasPart;
	}		
}
