package fi.vm.yti.datamodel.api.v2.endpoint.serializer;

import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import fi.vm.yti.datamodel.api.v2.dto.CrosswalkDTO;

@Component
public class CrosswalkConverter implements Converter<String, CrosswalkDTO> {

	@Override
	public CrosswalkDTO convert(String value) {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.readValue(value, CrosswalkDTO.class);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
				
	}



}
