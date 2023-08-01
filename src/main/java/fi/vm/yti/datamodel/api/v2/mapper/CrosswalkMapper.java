package fi.vm.yti.datamodel.api.v2.mapper;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.SKOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import fi.vm.yti.datamodel.api.v2.dto.CrosswalkDTO;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkFormat;
import fi.vm.yti.datamodel.api.v2.dto.CrosswalkInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.DCAP;
import fi.vm.yti.datamodel.api.v2.dto.FileMetadata;
import fi.vm.yti.datamodel.api.v2.dto.Iow;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.ModelType;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexCrosswalk;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFile;
import fi.vm.yti.datamodel.api.v2.service.impl.PostgresStorageService;
import fi.vm.yti.security.YtiUser;

@Service
public class CrosswalkMapper {
	private final Logger log = LoggerFactory.getLogger(CrosswalkMapper.class);
	private final StorageService storageService;
	private final CoreRepository coreRepository;
    private final String defaultNamespace;
    
	public CrosswalkMapper(
			CoreRepository coreRepository,
			PostgresStorageService storageService,
			@Value("${defaultNamespace}") String defaultNamespace) {
		this.coreRepository = coreRepository;
		this.storageService = storageService;
		this.defaultNamespace = defaultNamespace;
	}
	
	
	public Model mapToJenaModel(String PID, CrosswalkDTO dto) {
		log.info("Mapping CrosswalkDTO to Jena Model");
		var model = ModelFactory.createDefaultModel();
		var modelUri = PID;
		// TODO: type of application profile?
		model.setNsPrefixes(ModelConstants.PREFIXES);
		Resource type = MSCR.CROSSWALK;
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

		addOrgsToModel(dto, modelResource);

		// addInternalNamespaceToDatamodel(modelDTO, modelResource, model);
		// addExternalNamespaceToDatamodel(modelDTO, model, modelResource);

		String prefix = MapperUtils.getMSCRPrefix(PID);
		model.setNsPrefix(prefix, modelUri + "#");
		
		modelResource.addProperty(MSCR.format, dto.getFormat().toString());
		
		modelResource.addProperty(MSCR.sourceSchema, ResourceFactory.createResource(dto.getSourceSchema()));
		modelResource.addProperty(MSCR.targetSchema, ResourceFactory.createResource(dto.getTargetSchema()));

		return model;
	}
	
    private void addOrgsToModel(CrosswalkDTO modelDTO, Resource modelResource) {
        var organizationsModel = coreRepository.getOrganizations();
        modelDTO.getOrganizations().forEach(org -> {
            var orgUri = ModelConstants.URN_UUID + org;
            var queryRes = ResourceFactory.createResource(orgUri);
            if(organizationsModel.containsResource(queryRes)){
                modelResource.addProperty(DCTerms.contributor, organizationsModel.getResource(orgUri));
            }
        });
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

		var organizations = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.contributor);
		dto.setOrganizations(OrganizationMapper.mapOrganizationsToDTO(organizations, coreRepository.getOrganizations()));

		var created = modelResource.getProperty(DCTerms.created).getLiteral().getString();
		var modified = modelResource.getProperty(DCTerms.modified).getLiteral().getString();
		dto.setCreated(created);
		dto.setModified(modified);

		
		dto.setFormat(CrosswalkFormat.valueOf(MapperUtils.propertyToString(modelResource, MSCR.format)));
		
		List<StoredFile> retrievedSchemaFiles = storageService.retrieveAllCrosswalkFiles(PID);
		Set<FileMetadata> fileMetadatas = new HashSet<>();
		retrievedSchemaFiles.forEach(file -> {
			fileMetadatas.add(new FileMetadata(file.contentType(), file.data().length, file.fileID()));
		});
		dto.setFileMetadata(fileMetadatas);

		
		return dto;
	}
	
	public Model mapToUpdateJenaModel(String pid, CrosswalkDTO dto, Model model, YtiUser user) {
        var updateDate = new XSDDateTime(Calendar.getInstance());
        var modelResource = model.getResource(pid);
        var modelType = MapperUtils.getModelTypeFromResource(modelResource);

        //update languages before getting and using the languages for localized properties
        if(dto.getLanguages() != null){
            modelResource.removeAll(DCTerms.language);
            dto.getLanguages().forEach(lang -> modelResource.addProperty(DCTerms.language, lang));
        }

        var langs = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.language);

        var status = dto.getStatus();
        if (status != null) {
            MapperUtils.updateStringProperty(modelResource, OWL.versionInfo, status.name());
        }

        MapperUtils.updateLocalizedProperty(langs, dto.getLabel(), modelResource, RDFS.label, model);
        MapperUtils.updateLocalizedProperty(langs, dto.getDescription(), modelResource, RDFS.comment, model);
        MapperUtils.updateStringProperty(modelResource, Iow.contact, dto.getContact());
        MapperUtils.updateLocalizedProperty(langs, dto.getDocumentation(), modelResource, Iow.documentation, model);

        if(dto.getGroups() != null){
            modelResource.removeAll(DCTerms.isPartOf);
            var groupModel = coreRepository.getServiceCategories();
            dto.getGroups().forEach(group -> {
                var groups = groupModel.listResourcesWithProperty(SKOS.notation, group);
                if (groups.hasNext()) {
                    modelResource.addProperty(DCTerms.isPartOf, groups.next());
                }
            });
        }

        if(dto.getOrganizations() != null){
            modelResource.removeAll(DCTerms.contributor);
            addOrgsToModel(dto, modelResource);
        }



        modelResource.removeAll(DCTerms.modified);
        modelResource.addProperty(DCTerms.modified, ResourceFactory.createTypedLiteral(updateDate));
        modelResource.removeAll(Iow.modifier);
        modelResource.addProperty(Iow.modifier, user.getId().toString());
        return model;
		

	}	
	
    public IndexCrosswalk mapToIndexModel(String pid, Model model){
    	var resource = model.getResource(pid);
        var indexModel = new IndexCrosswalk();
        indexModel.setId(pid);
        indexModel.setStatus(Status.valueOf(resource.getProperty(OWL.versionInfo).getString()));
        indexModel.setModified(resource.getProperty(DCTerms.modified).getString());
        indexModel.setCreated(resource.getProperty(DCTerms.created).getString());
        var contentModified = resource.getProperty(Iow.contentModified);
        if(contentModified != null) {
            indexModel.setContentModified(contentModified.getString());
        }
        indexModel.setType(MSCRType.CROSSWALK);

        indexModel.setPrefix(pid);
        indexModel.setLabel(MapperUtils.localizedPropertyToMap(resource, RDFS.label));
        indexModel.setComment(MapperUtils.localizedPropertyToMap(resource, RDFS.comment));
        var contributors = new ArrayList<UUID>();
        resource.listProperties(DCTerms.contributor).forEach(contributor -> {
            var value = contributor.getObject().toString();
            contributors.add(MapperUtils.getUUID(value));
        });
        indexModel.setContributor(contributors);
        var isPartOf = MapperUtils.arrayPropertyToList(resource, DCTerms.isPartOf);
        var serviceCategories = coreRepository.getServiceCategories();
        var groups = isPartOf.stream().map(serviceCat -> MapperUtils.propertyToString(serviceCategories.getResource(serviceCat), SKOS.notation)).collect(Collectors.toList());
        indexModel.setIsPartOf(groups);
        indexModel.setLanguage(MapperUtils.arrayPropertyToList(resource, DCTerms.language));

        if(resource.hasProperty(MSCR.sourceSchema)) {
        	indexModel.setSourceSchema(resource.getPropertyResourceValue(MSCR.sourceSchema).getURI());
        }
        if(resource.hasProperty(MSCR.targetSchema)) {
        	indexModel.setTargetSchema(resource.getPropertyResourceValue(MSCR.targetSchema).getURI());
        }
        
        return indexModel;
    }	

}