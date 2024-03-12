package fi.vm.yti.datamodel.api.v2.service.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@Import({
	PIDMSServiceImpl.class,
})
class PIDMSServiceImplTest {

	@Autowired
	private PIDMSServiceImpl service;
	
//	@Test
	void testMapToInternal() throws Exception {
		String pid = service.mapToInternal("21.T13999/EOSC-202403000242012");
		System.out.println(pid);
	}

}
