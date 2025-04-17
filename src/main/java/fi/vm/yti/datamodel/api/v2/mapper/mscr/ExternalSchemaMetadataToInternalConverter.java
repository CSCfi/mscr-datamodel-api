package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import java.util.Map;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import fi.vm.yti.datamodel.api.v2.dto.InternalSchemaMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.MSCRState;
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.MSCRVisibility;
import fi.vm.yti.datamodel.api.v2.dto.ModelType;
import fi.vm.yti.datamodel.api.v2.dto.PublicSchemaMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;

@Component
public class ExternalSchemaMetadataToInternalConverter extends AbstractExternalMetadataToInternalConverter implements Converter<PublicSchemaMetadataDTO, InternalSchemaMetadataDTO> {

	@Override
	public InternalSchemaMetadataDTO convert(PublicSchemaMetadataDTO s) {
		SchemaDTO p = new SchemaDTO();		
		convert(s, p);		
		p.setFormat(SchemaFormat.valueOf(s.getFormat()));
		p.setNamespace(s.getNamespace());		
		p.setType(ModelType.SCHEMA);
		return p;
	}



	
}
