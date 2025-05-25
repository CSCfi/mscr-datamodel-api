package fi.vm.yti.datamodel.api.v2.dto;

import java.util.Set;
import java.util.UUID;

public interface MSCRCommonMetadata {

	public MSCRVisibility getVisibility();
	public MSCRState getState();
	public MSCRSubType getSubType();
	public Set<String> getOwner();
	public void setOwner(Set<String> owners);
}
