package fi.vm.yti.datamodel.api.v2.mapper;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.SchemaDO;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MSCRModelDTO;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexSchema;

public abstract class MSCRMapper {
	
	protected void mapToIndexModel(Resource resource, IndexSchema indexModel) {
        indexModel.setIdentifiers(MapperUtils.arrayPropertyToList(resource, DCTerms.identifier));
        indexModel.setLicense(MapperUtils.propertyToString(resource, DCTerms.license));		
	}
	
	protected void mapToMSCRModelDTO(MSCRModelDTO dto, Resource modelResource, String id) {
		// filter out internal identifier
		dto.setDctIdentifiers(MapperUtils.arrayPropertyToList(modelResource, DCTerms.identifier).stream().filter(p -> !p.equals(id)).toList());
		dto.setDctCreators(MapperUtils.arrayPropertyToList(modelResource, DCTerms.creator));
		dto.setDomain(MapperUtils.propertyToString(modelResource, MSCR.domain));
		dto.setDctLicense(MapperUtils.propertyToString(modelResource, DCTerms.license));
		dto.setDctPublisher(MapperUtils.propertyToString(modelResource, DCTerms.publisher));

	}
	
	protected void mapToUpdateJenaModel(MSCRModelDTO dto, Resource modelResource) {
		modelResource.removeAll(DCTerms.identifier);
		modelResource.removeAll(DCTerms.creator);
		modelResource.removeAll(MSCR.domain);
		modelResource.removeAll(DCTerms.license);
		modelResource.removeAll(DCTerms.publisher);
		mapToJenaModel(dto, modelResource);
	}
	protected void mapToJenaModel(MSCRModelDTO dto, Resource modelResource) {
		// dct / schema fields
		if(dto.getDctIdentifiers() != null) {
			for(String value : dto.getDctIdentifiers()) {
				modelResource.addLiteral(DCTerms.identifier, value);
			}
		}
		if(dto.getDctCreators() != null) {
			for(String value : dto.getDctCreators()) {
				modelResource.addLiteral(DCTerms.creator, value);
			}
		}
		if(dto.getDomain() != null) {
			modelResource.addLiteral(MSCR.domain, dto.getDomain());
		}
		if(dto.getDctLicense() != null) {
			modelResource.addLiteral(DCTerms.license, dto.getDctLicense());
		}
		if(dto.getDctPublisher() != null) {
			modelResource.addLiteral(DCTerms.publisher, dto.getDctPublisher());
		}			
		
	}
}
