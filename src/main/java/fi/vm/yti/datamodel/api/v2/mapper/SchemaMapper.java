package fi.vm.yti.datamodel.api.v2.mapper;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.jena.arq.querybuilder.ConstructBuilder;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.query.Query;
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
import org.springframework.stereotype.Service;

import fi.vm.yti.datamodel.api.v2.dto.DCAP;
import fi.vm.yti.datamodel.api.v2.dto.FileMetadata;
import fi.vm.yti.datamodel.api.v2.dto.Iow;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MSCRState;
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.MSCRVisibility;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.ResourceCommonDTO;
import fi.vm.yti.datamodel.api.v2.dto.Revision;
import fi.vm.yti.datamodel.api.v2.dto.SchemaDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.dto.Variant;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexSchema;
import fi.vm.yti.datamodel.api.v2.repository.CoreRepository;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFileMetadata;
import fi.vm.yti.datamodel.api.v2.service.impl.PostgresStorageService;
import fi.vm.yti.datamodel.api.v2.utils.SparqlUtils;
import fi.vm.yti.security.YtiUser;

@Service
public class SchemaMapper {

	private final Logger log = LoggerFactory.getLogger(SchemaMapper.class);
	private final StorageService storageService;
	private final CoreRepository coreRepository;
	private final JenaService jenaService;

	public SchemaMapper(
			CoreRepository coreRepository,
			PostgresStorageService storageService,
			JenaService jenaService) {
		this.coreRepository = coreRepository;
		this.storageService = storageService;
		this.jenaService = jenaService;
	}
	public Model mapToJenaModel(String PID, SchemaDTO schemaDTO, YtiUser user) {
		return mapToJenaModel(PID, schemaDTO, null, null, user);
	}

	public Model mapToJenaModel(String PID, SchemaDTO schemaDTO, final String revisionOf, final String aggregationKey, YtiUser user) {
		log.info("Mapping SchemaDTO to Jena Model");
		var model = ModelFactory.createDefaultModel();
		var modelUri = PID;
		// TODO: type of application profile?
		model.setNsPrefixes(ModelConstants.PREFIXES);
		Resource type = MSCR.SCHEMA;
		var creationDate = new XSDDateTime(Calendar.getInstance());
		var modelResource = model.createResource(modelUri).addProperty(RDF.type, type)
				.addProperty(OWL.versionInfo, schemaDTO.getStatus().name()).addProperty(DCTerms.identifier, PID);

		schemaDTO.getLanguages().forEach(lang -> modelResource.addProperty(DCTerms.language, lang));

		modelResource.addProperty(Iow.contentModified, ResourceFactory.createTypedLiteral(creationDate));

		modelResource.addProperty(DCAP.preferredXMLNamespacePrefix, PID);
		modelResource.addProperty(DCAP.preferredXMLNamespace, modelResource);

		MapperUtils.addLocalizedProperty(schemaDTO.getLanguages(), schemaDTO.getLabel(), modelResource, RDFS.label,
				model);
		MapperUtils.addLocalizedProperty(schemaDTO.getLanguages(), schemaDTO.getDescription(), modelResource,
				RDFS.comment, model);

		addOrgsToModel(schemaDTO, modelResource);
		MapperUtils.addCreationMetadata(modelResource, user);
		
		// addInternalNamespaceToDatamodel(modelDTO, modelResource, model);
		// addExternalNamespaceToDatamodel(modelDTO, model, modelResource);

		String prefix = MapperUtils.getMSCRPrefix(PID);
		model.setNsPrefix(prefix, modelUri + "#");

		modelResource.addProperty(MSCR.format, schemaDTO.getFormat().toString());
				
		
		modelResource.addProperty(MSCR.namespace, ResourceFactory.createResource(schemaDTO.getNamespace()));
		modelResource.addProperty(MSCR.versionLabel, schemaDTO.getVersionLabel());
		if(aggregationKey != null) {
			if(jenaService.doesSchemaExist(revisionOf)) {				
				modelResource.addProperty(MSCR.PROV_wasRevisionOf, ResourceFactory.createResource(revisionOf));
				modelResource.addProperty(MSCR.aggregationKey, ResourceFactory.createResource(aggregationKey));
			}
		}
		else {
			modelResource.addProperty(MSCR.aggregationKey, ResourceFactory.createResource(PID));
		}
		modelResource.addProperty(MSCR.state, ResourceFactory.createStringLiteral(schemaDTO.getState().name()));
		modelResource.addProperty(MSCR.visibility, ResourceFactory.createStringLiteral(schemaDTO.getVisibility().name()));
		var orgs = schemaDTO.getOrganizations();
		if(orgs != null && !orgs.isEmpty()) {
			orgs.forEach(org -> {
                modelResource.addProperty(MSCR.owner, org.toString());
	        });
		}
		else {
			modelResource.addProperty(MSCR.owner, user.getId().toString());
		}
		return model;
	}
	
	public Model mapToUpdateJenaModel(String pid, SchemaDTO dto, Model model, YtiUser user) {
        var updateDate = new XSDDateTime(Calendar.getInstance());
        var modelResource = model.getResource(pid);

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
        var state = dto.getState();
        if (state != null) {
        	MapperUtils.updateStringProperty(modelResource, MSCR.state, state.name());
        }
        	
        var visibility = dto.getVisibility();
        if (visibility != null) {
        	MapperUtils.updateStringProperty(modelResource, MSCR.visibility, visibility.name());
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

		
        modelResource.removeAll(MSCR.namespace);
		modelResource.addProperty(MSCR.namespace, ResourceFactory.createResource(dto.getNamespace()));
		
        modelResource.removeAll(MSCR.versionLabel);
		modelResource.addProperty(MSCR.versionLabel, dto.getVersionLabel());
		
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
		
		return model;
	}

	public SchemaInfoDTO mapToSchemaDTO(String PID, Model model, Consumer<ResourceCommonDTO> userMapper) {
		return mapToSchemaDTO(PID, model, false, userMapper);
	}
	
	public SchemaInfoDTO mapToSchemaDTO(String PID, Model model, boolean includeVersionData, Consumer<ResourceCommonDTO> userMapper) {

		var schemaInfoDTO = new SchemaInfoDTO();
		schemaInfoDTO.setPID(PID);

		var modelResource = model.getResource(PID);

		var status = Status.valueOf(MapperUtils.propertyToString(modelResource, OWL.versionInfo));
		schemaInfoDTO.setStatus(status);

		// Language
		schemaInfoDTO.setLanguages(MapperUtils.arrayPropertyToSet(modelResource, DCTerms.language));

		// Label
		schemaInfoDTO.setLabel(MapperUtils.localizedPropertyToMap(modelResource, RDFS.label));

		// Description
		schemaInfoDTO.setDescription(MapperUtils.localizedPropertyToMap(modelResource, RDFS.comment));

		var organizations = MapperUtils.arrayPropertyToSet(modelResource, DCTerms.contributor);
		schemaInfoDTO.setOrganizations(OrganizationMapper.mapOrganizationsToDTO(organizations, coreRepository.getOrganizations()));

        MapperUtils.mapCreationInfo(schemaInfoDTO, modelResource, userMapper);

		List<StoredFileMetadata> retrievedSchemaFiles = storageService.retrieveAllSchemaFilesMetadata(PID);
		Set<FileMetadata> fileMetadatas = new HashSet<>();
		retrievedSchemaFiles.forEach(file -> {
			fileMetadatas.add(new FileMetadata(file.contentType(), file.dataSize(), file.fileID()));
		});
		schemaInfoDTO.setFileMetadata(fileMetadatas);

		schemaInfoDTO.setPID(PID);
		schemaInfoDTO.setFormat(SchemaFormat.valueOf(MapperUtils.propertyToString(modelResource, MSCR.format)));
		
		schemaInfoDTO.setNamespace(MapperUtils.propertyToString(modelResource, MSCR.namespace));
		schemaInfoDTO.setVersionLabel(MapperUtils.propertyToString(modelResource, MSCR.versionLabel));
		
		if(modelResource.hasProperty(MSCR.PROV_wasRevisionOf)) {
			schemaInfoDTO.setRevisionOf(MapperUtils.propertyToString(modelResource, MSCR.PROV_wasRevisionOf));
		}
		if(modelResource.hasProperty(MSCR.hasRevision)) {
			schemaInfoDTO.setHasRevisions(MapperUtils.arrayPropertyToList(modelResource, MSCR.hasRevision));
		}
		schemaInfoDTO.setAggregationKey(MapperUtils.propertyToString(modelResource, MSCR.aggregationKey));
		
		if(includeVersionData) {
			// query for revisions and variants here		
			List<Revision> revs = new ArrayList<Revision>();
 			var revisionsModel = jenaService.constructWithQuerySchemas(MapperUtils.getRevisionsQuery(schemaInfoDTO.getAggregationKey()));			
			revisionsModel.listSubjects().forEach(res -> {
				revs.add(MapperUtils.mapToRevision(res));				
			});
			
			List<Revision> orderedRevs = revs.stream()
					.sorted((Revision r1, Revision r2) -> r1.getCreated().compareTo(r2.getCreated()))
					.collect(Collectors.toList());
			schemaInfoDTO.setRevisions(orderedRevs);
			
			List<Variant> variants = new ArrayList<Variant>();
			try {
	 			var variantsModel = jenaService.constructWithQuerySchemas(MapperUtils.getSchemaVariantsQuery(PID, schemaInfoDTO.getNamespace()));			
	 			variantsModel.listSubjects().forEach(res -> {
					variants.add(MapperUtils.mapToVariant(res));				
				});
				
			}catch(Exception ex) {
				log.error("Querying schema variants failed.", ex);
			}
			Map<String, List<Variant>> v2 = variants.stream()
					.collect(
							Collectors.groupingBy(Variant::getAggregationKey));
			schemaInfoDTO.setVariants(variants);
			schemaInfoDTO.setVariants2(v2);
		
		}
		var state = MSCRState.valueOf(MapperUtils.propertyToString(modelResource,  MSCR.state));
		schemaInfoDTO.setState(state);
		var visibility = MSCRVisibility.valueOf(MapperUtils.propertyToString(modelResource,  MSCR.visibility));
		schemaInfoDTO.setVisibility(visibility);
		
		schemaInfoDTO.setOwner(MapperUtils.arrayPropertyToSet(modelResource, MSCR.owner));
		

		return schemaInfoDTO;
	}
	
	

	/**
	 * Add organization to a schema
	 * 
	 * @param modelDTO      Payload to get organizations from
	 * @param modelResource Model resource to add orgs to
	 */
    private void addOrgsToModel(SchemaDTO modelDTO, Resource modelResource) {
        var organizationsModel = coreRepository.getOrganizations();
        modelDTO.getOrganizations().forEach(org -> {
            var orgUri = ModelConstants.URN_UUID + org;
            var queryRes = ResourceFactory.createResource(orgUri);
            if(organizationsModel.containsResource(queryRes)){
                modelResource.addProperty(DCTerms.contributor, organizationsModel.getResource(orgUri));
            }
        });
    }

    public IndexSchema mapToIndexModel(String pid, Model model){
    	return mapToIndexModel(pid, model, null);
    }
    /**
     * Map a DataModel to a DataModelDocument
     * @param prefix Prefix of model
     * @param model Model
     * @return Index model
     */
    public IndexSchema mapToIndexModel(String pid, Model model, Model revisionsModel){
        var resource = model.getResource(pid);
        var indexModel = new IndexSchema();
        indexModel.setId(pid);
        indexModel.setStatus(Status.valueOf(resource.getProperty(OWL.versionInfo).getString()));
        indexModel.setModified(resource.getProperty(DCTerms.modified).getString());
        indexModel.setCreated(resource.getProperty(DCTerms.created).getString());
        var contentModified = resource.getProperty(Iow.contentModified);
        if(contentModified != null) {
            indexModel.setContentModified(contentModified.getString());
        }
        indexModel.setType(MSCRType.SCHEMA);
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

        indexModel.setFormat(MapperUtils.propertyToString(resource, MSCR.format));
        indexModel.setAggregationKey(MapperUtils.propertyToString(resource, MSCR.aggregationKey));
        indexModel.setRevisionOf(MapperUtils.propertyToString(resource, MSCR.PROV_wasRevisionOf));
        indexModel.setHasRevision(MapperUtils.propertyToString(resource, MSCR.hasRevision));
        
        List<Revision> revs = new ArrayList<Revision>();
        if(revisionsModel == null) {
        	revisionsModel = jenaService.constructWithQuerySchemas(MapperUtils.getRevisionsQuery(indexModel.getAggregationKey()));
        }
        Resource aggregationResource = resource.getPropertyResourceValue(MSCR.aggregationKey);
        if(aggregationResource != null) {
            revisionsModel.listSubjectsWithProperty(MSCR.aggregationKey, aggregationResource) .forEach(res -> {
    			revs.add(MapperUtils.mapToRevision(res));				
    		});
    		List<Revision> orderedRevs = revs.stream()
    				.sorted((Revision r1, Revision r2) -> r1.getCreated().compareTo(r2.getCreated()))
    				.collect(Collectors.toList());
    		indexModel.setRevisions(orderedRevs); 
    		indexModel.setNumberOfRevisions(orderedRevs.size());	
        }
        indexModel.setState(MSCRState.valueOf(resource.getProperty(MSCR.state).getString()));
        indexModel.setVisibility(MSCRVisibility.valueOf(resource.getProperty(MSCR.visibility).getString()));
        
        return indexModel;
    }     
	
	public Query getAggregationKeyFromPrevQuery(String prevVersionPID) {
		
		var builder = new ConstructBuilder();
		Resource prevVersion = ResourceFactory.createResource(prevVersionPID);
        SparqlUtils.addConstructProperty("?version", builder, MSCR.aggregationKey, "?aggregationKey");
        builder.addWhere(prevVersion, MSCR.aggregationKey, "?aggregationKey");
        return builder.build();
	}    
      
        
	public SchemaDTO mapToSchemaDTO(SchemaInfoDTO source) {		
		SchemaDTO s = new SchemaDTO();
		s.setStatus(source.getStatus());
		s.setState(source.getState());
		s.setVisibility(source.getVisibility());
		s.setLabel(source.getLabel());
		s.setDescription(source.getDescription());
		s.setLanguages(source.getLanguages());
		s.setNamespace(source.getNamespace());
		s.setOrganizations(source.getOrganizations().stream().map(org ->  UUID.fromString(org.getId())).collect(Collectors.toSet()));
		s.setVersionLabel(source.getVersionLabel());
		s.setFormat(source.getFormat());		
		return s;
	} 
	



}
