package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import java.util.Iterator;
import java.util.Set;

import fi.vm.yti.datamodel.api.v2.dto.FileMetadata;
import fi.vm.yti.datamodel.api.v2.dto.InternalMSCRMetadataDTO;
import fi.vm.yti.datamodel.api.v2.dto.PublicMSCRMetadataDTO;

public abstract class AbstractInternalMetadataToExternalConverter {

	protected void convert(PublicMSCRMetadataDTO p, InternalMSCRMetadataDTO s) {
		
		p.setContactPoint(s.getContact());
		p.setContributor(s.getDctContributors());
		p.setCreator(s.getDctCreators());
		p.setDateSubmitted(s.getCreated());
		p.setDescription(s.getDescription().get("en"));
		p.setDomain(s.getDomain());
		p.setIdentifier(s.getDctIdentifiers());
		p.setIssued(s.getDctIssued());
		p.setKeyword(s.getDcatKeywords());
		p.setLanguage(s.getLanguages().iterator().next());
		p.setLicense(s.getDctLicense());
		p.setModified(s.getModified());

		p.setPublisher(s.getDctPublisher());
		p.setRelation(s.getDctRelations());
		p.setSource(s.getSourceURL());
		p.setTitle(s.getLabel().get("en"));
		p.setVersionLabel(s.getVersionLabel());
		p.setVisibility(s.getVisibility().name());
		p.setStatus(s.getState().name());
		p.setPreviousVersion(null);
				
	}
}

