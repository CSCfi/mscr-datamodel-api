package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import fi.vm.yti.datamodel.api.v2.dto.CrosswalkDTO;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkFormat;
import fi.vm.yti.datamodel.api.v2.dto.InternalCrosswalkMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.InternalSchemaMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.ModelType;
import fi.vm.yti.datamodel.api.v2.dto.PublicCrosswalkMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.PublicSchemaMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;

@Component
public class ExternalCrosswalkMetadataToInternalConverter extends AbstractExternalMetadataToInternalConverter implements Converter<PublicCrosswalkMetadataDTO, InternalCrosswalkMetadataDTO> {

	@Override
	public InternalCrosswalkMetadataDTO convert(PublicCrosswalkMetadataDTO s) {
		CrosswalkDTO p = new CrosswalkDTO();
		convert(s, p);
		p.setFormat(CrosswalkFormat.valueOf(s.getFormat()));
		p.setSourceSchema(s.getSourceSchema());
		p.setTargetSchema(s.getTargetSchema());
		p.setType(ModelType.CROSSWALK);		
		return p;
	}



}
