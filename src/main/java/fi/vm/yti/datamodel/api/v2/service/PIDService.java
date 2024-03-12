package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;

public interface PIDService {

	public String mint(PIDType pidType, MSCRType contentType, String id) throws Exception;
	public String mintPartIdentifier(String pid);
	
	public String mapToInternal(String id) throws Exception;
}
