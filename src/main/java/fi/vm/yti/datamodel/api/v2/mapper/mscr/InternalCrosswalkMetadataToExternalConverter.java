package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import fi.vm.yti.datamodel.api.v2.dto.CrosswalkInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.FileMetadata;
import fi.vm.yti.datamodel.api.v2.dto.InternalSchemaMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.PublicCrosswalkMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.PublicCrosswalkMetadataInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.PublicSchemaMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.PublicSchemaMetadataInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;

@Component
public class InternalCrosswalkMetadataToExternalConverter extends AbstractInternalMetadataToExternalConverter implements Converter<CrosswalkInfoDTO, PublicCrosswalkMetadataDTO> {

	
	@Override
	public PublicCrosswalkMetadataInfoDTO convert(CrosswalkInfoDTO s) {
		
		PublicCrosswalkMetadataInfoDTO p = new PublicCrosswalkMetadataInfoDTO();
		
		convert(p, s);
		p.setInternalID(s.getID());
		p.setHandle(s.getHandle());
		p.setFormat(s.getFormat().name());
		p.setType("CROSSWALK");
		p.setSourceSchema(s.getSourceSchema());
		p.setTargetSchema(s.getTargetSchema());
		
		Set<FileMetadata> fm = s.getFileMetadata();
		if(fm != null) {
			Iterator<FileMetadata> fmi = fm.iterator();
			while(fmi.hasNext()) {
				p.setDownloadUrl("/datamodel-api/v2/schema/%s/files/%d?download=true".formatted(s.getID(), fmi.next().getFileID()));
			}
		}
		
		
		return p;
	}

	
}
