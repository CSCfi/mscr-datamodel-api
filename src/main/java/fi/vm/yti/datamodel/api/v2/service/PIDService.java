package fi.vm.yti.datamodel.api.v2.service;

import fi.vm.yti.datamodel.api.v2.dto.PIDType;

public interface PIDService {

	public String mint(PIDType type);
}
