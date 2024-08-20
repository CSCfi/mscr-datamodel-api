package fi.vm.yti.datamodel.api.v2.mapper;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
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
import org.jetbrains.annotations.NotNull;
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
import fi.vm.yti.datamodel.api.v2.dto.MSCRState;
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.MSCRVisibility;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.OwnerDTO;
import fi.vm.yti.datamodel.api.v2.dto.ResourceCommonDTO;
import fi.vm.yti.datamodel.api.v2.dto.Revision;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexCrosswalk;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFileMetadata;
import fi.vm.yti.datamodel.api.v2.service.impl.PostgresStorageService;
import fi.vm.yti.security.YtiUser;

@Service
public class CrosswalkMapper {
	private final Logger log = LoggerFactory.getLogger(CrosswalkMapper.class);
	private final StorageService storageService;
	private final CoreRepository coreRepository;
    private final String defaultNamespace;
    private final JenaService jenaService;
    private final DateFormat timestampFormat = new SimpleDateFormat("YYYY-MM-DD'T'HH:MM:SSZ");
	
	public CrosswalkMapper(
			CoreRepository coreRepository,
			PostgresStorageService storageService,
			@Value("${defaultNamespace}") String defaultNamespace,
			JenaService jenaService) {
		this.coreRepository = coreRepository;
		this.storageService = storageService;
		this.defaultNamespace = defaultNamespace;
		this.jenaService = jenaService;
	}
	
	
	public Model mapToJenaModel(String PID, String handle, CrosswalkDTO dto, final String revisionOf, final String aggregationKey, @NotNull YtiUser user) {
		log.info("Mapping CrosswalkDTO to Jena Model");
		var model = ModelFactory.createDefaultModel();
		var modelUri = PID;
		// TODO: type of application profile?
		model.setNsPrefixes(ModelConstants.PREFIXES);
		Resource type = MSCR.CROSSWALK;
		var creationDate = new XSDDateTime(Calendar.getInstance());
		var modelResource = model.createResource(modelUri).addProperty(RDF.type, type)
				.addProperty(OWL.versionInfo, dto.getStatus().name()).addProperty(DCTerms.identifier, PID);

		dto.getLanguages().forEach(lang -> modelResource.addProperty(DCTerms.language, lang));

		modelResource.addProperty(Iow.contentModified, ResourceFactory.createTypedLiteral(creationDate));

		modelResource.addProperty(DCAP.preferredXMLNamespacePrefix, PID);
		modelResource.addProperty(DCAP.preferredXMLNamespace, modelResource);

		MapperUtils.addLocalizedProperty(dto.getLanguages(), dto.getLabel(), modelResource, RDFS.label,
				model);
		MapperUtils.addLocalizedProperty(dto.getLanguages(), dto.getDescription(), modelResource,
				RDFS.comment, model);

		addOrgsToModel(dto, modelResource);
		MapperUtils.addCreationMetadata(modelResource, user);
		MapperUtils.addOptionalStringProperty(modelResource, Iow.contact, dto.getContact());

		// addInternalNamespaceToDatamodel(modelDTO, modelResource, model);
		// addExternalNamespaceToDatamodel(modelDTO, model, modelResource);

		String prefix = "";
		model.setNsPrefix(prefix, modelUri + "#");
		
		modelResource.addProperty(MSCR.format, dto.getFormat().toString());
		
        modelResource.removeAll(MSCR.versionLabel);
		modelResource.addProperty(MSCR.versionLabel, dto.getVersionLabel());
		
		modelResource.addProperty(MSCR.state, ResourceFactory.createStringLiteral(dto.getState().name()));
		modelResource.addProperty(MSCR.visibility, ResourceFactory.createStringLiteral(dto.getVisibility().name()));

		var orgs = dto.getOrganizations();
		if(orgs != null && !orgs.isEmpty()) {
			orgs.forEach(org -> {
                modelResource.addProperty(MSCR.owner, org.toString());
	        });
		}
		else {
			modelResource.addProperty(MSCR.owner, user.getId().toString());
		}
		
		if(aggregationKey != null) {
			if(jenaService.doesCrosswalkExist(revisionOf)) {				
				modelResource.addProperty(MSCR.PROV_wasRevisionOf, ResourceFactory.createResource(revisionOf));
				modelResource.addProperty(MSCR.aggregationKey, ResourceFactory.createResource(aggregationKey));
			}
			else {
				throw new RuntimeException("Could not find the target of crosswalk revision with a pid " + revisionOf);
			}
		}
		else {
			modelResource.addProperty(MSCR.aggregationKey, ResourceFactory.createResource(PID));
		}
		
		modelResource.addProperty(MSCR.sourceSchema, ResourceFactory.createResource(dto.getSourceSchema()));
		modelResource.addProperty(MSCR.targetSchema, ResourceFactory.createResource(dto.getTargetSchema()));
		
		if(handle != null) {
			modelResource.addProperty(MSCR.handle, model.createLiteral(handle));
		}
		if(dto.getSourceURL() != null) {
			modelResource.addProperty(MSCR.sourceURL, model.createResource(dto.getSourceURL()));
		}
		
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

	public CrosswalkInfoDTO mapToFrontendCrosswalkDTO(String PID, Model model, Consumer<OwnerDTO> ownerMapper) {
		var dto = new CrosswalkInfoDTO();
		dto.setPID(PID);

		var modelResource = model.getResource(PID);		
		// Label
		dto.setLabel(MapperUtils.localizedPropertyToMap(modelResource, RDFS.label));

		// Description
		dto.setDescription(MapperUtils.localizedPropertyToMap(modelResource, RDFS.comment));		
		dto.setPID(PID);
		dto.setFormat(CrosswalkFormat.valueOf(MapperUtils.propertyToString(modelResource, MSCR.format)));
		
		var organizations = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.contributor);
		dto.setOrganizations(OrganizationMapper.mapOrganizationsToDTO(organizations, coreRepository.getOrganizations()));
		
		dto.setContact(MapperUtils.propertyToString(modelResource, Iow.contact));
		
		dto.setVersionLabel(MapperUtils.propertyToString(modelResource, MSCR.versionLabel));
		dto.setAggregationKey(MapperUtils.propertyToString(modelResource, MSCR.aggregationKey));
		var state = MSCRState.valueOf(MapperUtils.propertyToString(modelResource,  MSCR.state));
		dto.setState(state);
		var visibility = MSCRVisibility.valueOf(MapperUtils.propertyToString(modelResource,  MSCR.visibility));
		dto.setVisibility(visibility);
		
		Set<String> ownerIds = MapperUtils.arrayPropertyToSet(modelResource, MSCR.owner);
		dto.setOwner(ownerIds);
				
		dto.setOwnerMetadata(MapperUtils.mapOwnerInfo(ownerIds, ownerMapper));
		
		dto.setSourceSchema(MapperUtils.propertyToString(modelResource, MSCR.sourceSchema));
		dto.setTargetSchema(MapperUtils.propertyToString(modelResource, MSCR.targetSchema));

		return dto;
	}
	
	public CrosswalkInfoDTO mapToCrosswalkDTO(String PID, Model model, Consumer<ResourceCommonDTO> userMapper, Consumer<OwnerDTO> ownerMapper) {
		return mapToCrosswalkDTO(PID, model, false, userMapper, ownerMapper);
	}
	public CrosswalkInfoDTO mapToCrosswalkDTO(String PID, Model model, boolean includeVersionData, Consumer<ResourceCommonDTO> userMapper, Consumer<OwnerDTO> ownerMapper) {
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

        MapperUtils.mapCreationInfo(dto, modelResource, userMapper);
        dto.setContact(MapperUtils.propertyToString(modelResource, Iow.contact));

		List<StoredFileMetadata> retrievedSchemaFiles = storageService.retrieveAllCrosswalkFilesMetadata(PID);
		Set<FileMetadata> fileMetadatas = new HashSet<>();
		retrievedSchemaFiles.forEach(file -> {
			fileMetadatas.add(new FileMetadata(file.contentType(), file.dataSize(), file.fileID(), file.filename(), timestampFormat.format(file.timestamp())));
		});
		dto.setFileMetadata(fileMetadatas);
		
		dto.setFormat(CrosswalkFormat.valueOf(MapperUtils.propertyToString(modelResource, MSCR.format)));
		
		dto.setVersionLabel(MapperUtils.propertyToString(modelResource, MSCR.versionLabel));
		
		dto.setAggregationKey(MapperUtils.propertyToString(modelResource, MSCR.aggregationKey));
		
		var state = MSCRState.valueOf(MapperUtils.propertyToString(modelResource,  MSCR.state));
		dto.setState(state);

		var visibility = MSCRVisibility.valueOf(MapperUtils.propertyToString(modelResource,  MSCR.visibility));
		dto.setVisibility(visibility);
		
		Set<String> ownerIds = MapperUtils.arrayPropertyToSet(modelResource, MSCR.owner);
		dto.setOwner(ownerIds);
				
		dto.setOwnerMetadata(MapperUtils.mapOwnerInfo(ownerIds, ownerMapper));
		
		dto.setSourceSchema(MapperUtils.propertyToString(modelResource, MSCR.sourceSchema));
		dto.setTargetSchema(MapperUtils.propertyToString(modelResource, MSCR.targetSchema));

		if(modelResource.hasProperty(MSCR.PROV_wasRevisionOf)) {
			dto.setRevisionOf(MapperUtils.propertyToString(modelResource, MSCR.PROV_wasRevisionOf));
		}
		if(modelResource.hasProperty(MSCR.hasRevision)) {
			dto.setHasRevisions(MapperUtils.arrayPropertyToList(modelResource, MSCR.hasRevision));
		}
		
		if(includeVersionData) {
			// query for revisions and variants here		
			List<Revision> revs = new ArrayList<Revision>();
 			var revisionsModel = jenaService.constructWithQueryCrosswalks(MapperUtils.getRevisionsQuery(dto.getAggregationKey()));			
			revisionsModel.listSubjects().forEach(res -> {
				revs.add(MapperUtils.mapToRevision(res));				
			});
			
			List<Revision> orderedRevs = revs.stream()
					.sorted((Revision r1, Revision r2) -> r1.getCreated().compareTo(r2.getCreated()))
					.collect(Collectors.toList());
			dto.setRevisions(orderedRevs);
					
		}	
		if(modelResource.hasProperty(MSCR.handle)) {
			dto.setHandle(MapperUtils.propertyToString(modelResource, MSCR.handle));	
		}
		if(modelResource.hasProperty(MSCR.sourceURL)) {
			dto.setSourceURL(MapperUtils.propertyToString(modelResource, MSCR.sourceURL));
		}
		return dto;
	}
	
	public Model mapToUpdateJenaModel(String pid, String handle, CrosswalkDTO dto, Model model, YtiUser user) {
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
        
        modelResource.removeAll(MSCR.format);
		modelResource.addProperty(MSCR.format, dto.getFormat().toString());
        
        var state = dto.getState();
        if (state != null) {
            MapperUtils.updateStringProperty(modelResource, MSCR.state, state.name());
        }
        var visibility = dto.getVisibility();
        if (visibility != null) {
            MapperUtils.updateStringProperty(modelResource, MSCR.visibility, visibility.name());
        }
		modelResource.removeAll(MSCR.owner);
		var orgs = dto.getOrganizations();
		if(orgs != null && !orgs.isEmpty()) {
			orgs.forEach(org -> {
                modelResource.addProperty(MSCR.owner, org.toString());
	        });
		}
		else {
			modelResource.addProperty(MSCR.owner, user.getId().toString());
		}        

        modelResource.removeAll(MSCR.versionLabel);
		modelResource.addProperty(MSCR.versionLabel, dto.getVersionLabel());

		if(handle != null) {
			model.addLiteral(modelResource, MSCR.handle, model.createLiteral(handle));
			
		}
		
        return model;
		

	}	
	public IndexCrosswalk mapToIndexModel(String pid, Model model) {
		return mapToIndexModel(pid, model, null);
		
	}
	
    public IndexCrosswalk mapToIndexModel(String pid, Model model, Model revisionsModel) {    	
    	var resource = model.getResource(pid);
        var indexModel = new IndexCrosswalk();
        indexModel.setId(pid);
        indexModel.setStatus(Status.valueOf(resource.getProperty(OWL.versionInfo).getString()));
        indexModel.setModified(resource.getProperty(DCTerms.modified).getString());
        if(resource.getProperty(DCTerms.created) != null) {
            indexModel.setCreated(resource.getProperty(DCTerms.created).getString());        	
        }
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
        indexModel.setOwner(MapperUtils.arrayPropertyToList(resource, MSCR.owner));
		indexModel.setOrganizations(MapperUtils.mapToListOrganizations(contributors, coreRepository.getOrganizations()));
	
        var isPartOf = MapperUtils.arrayPropertyToList(resource, DCTerms.isPartOf);
        var serviceCategories = coreRepository.getServiceCategories();
        var groups = isPartOf.stream().map(serviceCat -> MapperUtils.propertyToString(serviceCategories.getResource(serviceCat), SKOS.notation)).collect(Collectors.toList());
        indexModel.setIsPartOf(groups);
        indexModel.setLanguage(MapperUtils.arrayPropertyToList(resource, DCTerms.language));

        indexModel.setState(MSCRState.valueOf(resource.getProperty(MSCR.state).getString()));
        indexModel.setVisibility(MSCRVisibility.valueOf(resource.getProperty(MSCR.visibility).getString()));        
        indexModel.setFormat(MapperUtils.propertyToString(resource, MSCR.format));
        
        indexModel.setAggregationKey(MapperUtils.propertyToString(resource, MSCR.aggregationKey));
        indexModel.setRevisionOf(MapperUtils.propertyToString(resource, MSCR.PROV_wasRevisionOf));
        
        indexModel.setHasRevision(null);
        List<Revision> revs = new ArrayList<Revision>();
        if(revisionsModel == null) {
        	revisionsModel = jenaService.constructWithQueryCrosswalks(MapperUtils.getRevisionsQuery(indexModel.getAggregationKey()));
        }
        Resource aggregationResource = resource.getPropertyResourceValue(MSCR.aggregationKey);
        if(aggregationResource != null) {
            revisionsModel.listSubjects().forEach(res -> {
    			revs.add(MapperUtils.mapToRevision(res));				
    		});
    		List<Revision> orderedRevs = revs.stream()
    				.sorted((Revision r1, Revision r2) -> r1.getCreated().compareTo(r2.getCreated()))
    				.collect(Collectors.toList());
    		//indexModel.setRevisions(orderedRevs); 
    		if(orderedRevs.size() > 0) {
        		Revision latestRev = orderedRevs.get(orderedRevs.size() - 1);
        		if(latestRev.getPid().equals(pid)) {
        			indexModel.setHasRevision("false");
        			indexModel.setNumberOfRevisions(orderedRevs.size());
        		}
        		else {
        			indexModel.setHasRevision("true");
        		}
    			
    		}
        }        
        indexModel.setVersionLabel(MapperUtils.propertyToString(resource, MSCR.versionLabel));
        
       
        if(resource.hasProperty(MSCR.sourceSchema)) {
        	indexModel.setSourceSchema(resource.getPropertyResourceValue(MSCR.sourceSchema).getURI());
        }
        if(resource.hasProperty(MSCR.targetSchema)) {
        	indexModel.setTargetSchema(resource.getPropertyResourceValue(MSCR.targetSchema).getURI());
        }
        indexModel.setHandle(MapperUtils.propertyToString(resource, MSCR.handle));
        indexModel.setSourceURL(MapperUtils.propertyToString(resource, MSCR.sourceURL));
        
        return indexModel;
    }

	public CrosswalkDTO mapToCrosswalkDTO(CrosswalkInfoDTO source) {		
		CrosswalkDTO s = new CrosswalkDTO();
		s.setStatus(source.getStatus());
		s.setState(source.getState());
		s.setVisibility(source.getVisibility());
		s.setLabel(source.getLabel());
		s.setDescription(source.getDescription());
		s.setLanguages(source.getLanguages());
		s.setOrganizations(source.getOrganizations().stream().map(org ->  UUID.fromString(org.getId())).collect(Collectors.toSet()));
		s.setVersionLabel(source.getVersionLabel());
		s.setFormat(source.getFormat());		
		s.setSourceSchema(source.getSourceSchema());
		s.setTargetSchema(source.getTargetSchema());
		return s;
	} 

}