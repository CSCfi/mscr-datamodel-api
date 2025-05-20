package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import java.util.Map;
import java.util.Set;

import fi.vm.yti.datamodel.api.v2.dto.InternalMSCRMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.MSCRState;
import fi.vm.yti.datamodel.api.v2.dto.MSCRVisibility;
import fi.vm.yti.datamodel.api.v2.dto.PublicMSCRMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;

public class AbstractExternalMetadataToInternalConverter {

	protected void convert(PublicMSCRMetadataDTO s, InternalMSCRMetadataDTO p) {
		p.setContact(s.getContactPoint());
		p.setDctContributors(s.getContributor());
		p.setDctCreators(s.getCreator());
		p.setCreated(s.getDateSubmitted());
		p.setDescription(Map.of("en", s.getDescription()));
		p.setDomain(s.getDomain());
				
		p.setDctIdentifiers(s.getIdentifier());
		p.setDctIssued(s.getIssued());
		p.setDcatKeywords(s.getKeyword());
		p.setLanguages(Set.of(s.getLanguage()));
		p.setDctLicense(s.getLicense());
		p.setModified(s.getModified());
		
		p.setDctPublisher(s.getPublisher());
		p.setDctRelations(s.getRelation());
		p.setSourceURL(s.getSource());
		p.setLabel(Map.of("en", s.getTitle()));
		p.setVersionLabel(s.getVersionLabel());
		p.setVisibility(MSCRVisibility.valueOf(s.getVisibility()));
		p.setState(MSCRState.valueOf(s.getStatus()));
	}
}
