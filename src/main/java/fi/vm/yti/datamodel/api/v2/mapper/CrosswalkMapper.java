package fi.vm.yti.datamodel.api.v2.mapper;

import java.util.Calendar;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import fi.vm.yti.datamodel.api.v2.dto.CrosswalkDTO;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkFormat;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.DCAP;
import fi.vm.yti.datamodel.api.v2.dto.Iow;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.Status;

@Service
public class CrosswalkMapper {
	private final Logger log = LoggerFactory.getLogger(CrosswalkMapper.class);
	
	public Model mapToJenaModel(String PID, CrosswalkDTO dto) {
		log.info("Mapping CrosswalkDTO to Jena Model");
		var model = ModelFactory.createDefaultModel();
		var modelUri = PID;
		// TODO: type of application profile?
		model.setNsPrefixes(ModelConstants.PREFIXES);
		Resource type = MSCR.SCHEMAGROUP;
		var creationDate = new XSDDateTime(Calendar.getInstance());
		var modelResource = model.createResource(modelUri).addProperty(RDF.type, type)
				.addProperty(OWL.versionInfo, dto.getStatus().name()).addProperty(DCTerms.identifier, PID)
				.addProperty(DCTerms.modified, ResourceFactory.createTypedLiteral(creationDate))
				.addProperty(DCTerms.created, ResourceFactory.createTypedLiteral(creationDate));

		dto.getLanguages().forEach(lang -> modelResource.addProperty(DCTerms.language, lang));

		modelResource.addProperty(Iow.contentModified, ResourceFactory.createTypedLiteral(creationDate));

		modelResource.addProperty(DCAP.preferredXMLNamespacePrefix, PID);
		modelResource.addProperty(DCAP.preferredXMLNamespace, modelResource);

		MapperUtils.addLocalizedProperty(dto.getLanguages(), dto.getLabel(), modelResource, RDFS.label,
				model);
		MapperUtils.addLocalizedProperty(dto.getLanguages(), dto.getDescription(), modelResource,
				RDFS.comment, model);

		addOrg(dto, modelResource);

		// addInternalNamespaceToDatamodel(modelDTO, modelResource, model);
		// addExternalNamespaceToDatamodel(modelDTO, model, modelResource);

		String prefix = MapperUtils.getMSCRPrefix(PID);
		model.setNsPrefix(prefix, modelUri + "#");
		
		modelResource.addProperty(MSCR.format, dto.getFormat().toString());
		

		return model;
	}
	
	private void addOrg(CrosswalkDTO dto, Resource modelResource) {
		// TODO: Add org exists checks
		var orgRes = ResourceFactory.createResource(ModelConstants.URN_UUID + dto.getOrganization().toString());
		modelResource.addProperty(DCTerms.contributor, orgRes);
	}

	public CrosswalkInfoDTO mapToCrosswalkDTO(String PID, Model model) {
		var dto = new CrosswalkInfoDTO();
		dto.setPID(PID);

		var modelResource = model.getResource(PID);

		var status = Status.valueOf(MapperUtils.propertyToString(modelResource, OWL.versionInfo));
		dto.setStatus(status);

		// Language
		dto.setLanguages(MapperUtils.arrayPropertyToSet(modelResource, DCTerms.language));

		// Label
		dto.setLabel(MapperUtils.localizedPropertyToMap(modelResource, RDFS.label));

		// Description
		dto.setDescription(MapperUtils.localizedPropertyToMap(modelResource, RDFS.comment));

		dto.setOrganization(MapperUtils.getUUID(MapperUtils.propertyToString(modelResource, DCTerms.contributor)));

		var created = modelResource.getProperty(DCTerms.created).getLiteral().getString();
		var modified = modelResource.getProperty(DCTerms.modified).getLiteral().getString();
		dto.setCreated(created);
		dto.setModified(modified);

		
		dto.setFormat(CrosswalkFormat.valueOf(MapperUtils.propertyToString(modelResource, MSCR.format)));
				
		return dto;
	}
	

}