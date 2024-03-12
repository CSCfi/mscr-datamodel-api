package fi.vm.yti.datamodel.api.v2.service.impl;

import java.util.UUID;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.PIDType;
import fi.vm.yti.datamodel.api.v2.service.PIDService;


@Profile("local")
@Service
public class FakeHandleServiceImpl implements PIDService {

	@Override
	public String mint(PIDType pidType, MSCRType contentType, String id) throws Exception {
		return "21.T13999/dev-" + UUID.randomUUID();
	}

	@Override
	public String mintPartIdentifier(String pid) {
		return pid + "@mapping=" + UUID.randomUUID();
	}

	@Override
	public String mapToInternal(String id) throws Exception {
		return id;
	}

}
