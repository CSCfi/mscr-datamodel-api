package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import fi.vm.yti.datamodel.api.v2.dto.FileMetadata;
import fi.vm.yti.datamodel.api.v2.dto.InternalSchemaMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.PublicSchemaMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.PublicSchemaMetadataInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;

@Component
public class InternalSchemaMetadataToExternalConverter extends AbstractInternalMetadataToExternalConverter implements Converter<SchemaInfoDTO, PublicSchemaMetadataDTO> {

	
	@Override
	public PublicSchemaMetadataInfoDTO convert(SchemaInfoDTO s) {
		
		PublicSchemaMetadataInfoDTO p = new PublicSchemaMetadataInfoDTO();
		
		convert(p, s);
		p.setInternalID(s.getID());
		p.setHandle(s.getHandle());
		p.setFormat(s.getFormat().name());
		p.setNamespace(s.getNamespace());
		p.setType("SCHEMA");
		
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
