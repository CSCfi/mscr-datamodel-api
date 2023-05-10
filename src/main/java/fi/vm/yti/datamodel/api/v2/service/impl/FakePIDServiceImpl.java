package fi.vm.yti.datamodel.api.v2.service.impl;

import java.util.UUID;

import org.springframework.stereotype.Service;

import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.service.PIDService;

@Service
public class FakePIDServiceImpl implements PIDService {
	
	public String mint(PIDType type) {
		return "urn:IAMNOTAPID:" + UUID.randomUUID();
	}
}
