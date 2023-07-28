package fi.vm.yti.datamodel.api.v2.mapper;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.jena.arq.querybuilder.ConstructBuilder;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.query.Query;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
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
import fi.vm.yti.datamodel.api.v2.dto.MSCRType;
import fi.vm.yti.datamodel.api.v2.dto.ModelConstants;
import fi.vm.yti.datamodel.api.v2.dto.SchemaDTO;
import fi.vm.yti.datamodel.api.v2.dto.SchemaFormat;
import fi.vm.yti.datamodel.api.v2.dto.SchemaInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.Status;
import fi.vm.yti.datamodel.api.v2.endpoint.error.MappingError;
import fi.vm.yti.datamodel.api.v2.opensearch.index.IndexSchema;
import fi.vm.yti.datamodel.api.v2.service.JenaService;
import fi.vm.yti.datamodel.api.v2.service.StorageService;
import fi.vm.yti.datamodel.api.v2.service.StorageService.StoredFile;
import fi.vm.yti.datamodel.api.v2.service.impl.PostgresStorageService;
import fi.vm.yti.datamodel.api.v2.utils.SparqlUtils;
import fi.vm.yti.security.YtiUser;

@Service
public class SchemaMapper {

	private final Logger log = LoggerFactory.getLogger(SchemaMapper.class);
	private final StorageService storageService;
	private final JenaService jenaService;

	public SchemaMapper(
			JenaService jenaService,
			PostgresStorageService storageService) {
		this.jenaService = jenaService;
		this.storageService = storageService;
	}
	public Model mapToJenaModel(String PID, SchemaDTO schemaDTO) {
		return mapToJenaModel(PID, schemaDTO, null, null);
	}

	public Model mapToJenaModel(String PID, SchemaDTO schemaDTO, final String revisionOf, final String aggregationKey) {
		log.info("Mapping SchemaDTO to Jena Model");
		var model = ModelFactory.createDefaultModel();
		var modelUri = PID;
		// TODO: type of application profile?
		model.setNsPrefixes(ModelConstants.PREFIXES);
		Resource type = MSCR.SCHEMA;
		var creationDate = new XSDDateTime(Calendar.getInstance());
		var modelResource = model.createResource(modelUri).addProperty(RDF.type, type)
				.addProperty(OWL.versionInfo, schemaDTO.getStatus().name()).addProperty(DCTerms.identifier, PID)
				.addProperty(DCTerms.modified, ResourceFactory.createTypedLiteral(creationDate))
				.addProperty(DCTerms.created, ResourceFactory.createTypedLiteral(creationDate));

		schemaDTO.getLanguages().forEach(lang -> modelResource.addProperty(DCTerms.language, lang));

		modelResource.addProperty(Iow.contentModified, ResourceFactory.createTypedLiteral(creationDate));

		modelResource.addProperty(DCAP.preferredXMLNamespacePrefix, PID);
		modelResource.addProperty(DCAP.preferredXMLNamespace, modelResource);

		MapperUtils.addLocalizedProperty(schemaDTO.getLanguages(), schemaDTO.getLabel(), modelResource, RDFS.label,
				model);
		MapperUtils.addLocalizedProperty(schemaDTO.getLanguages(), schemaDTO.getDescription(), modelResource,
				RDFS.comment, model);

		addOrgsToModel(schemaDTO, modelResource);

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

        MapperUtils.updateLocalizedProperty(langs, dto.getLabel(), modelResource, RDFS.label, model);
        MapperUtils.updateLocalizedProperty(langs, dto.getDescription(), modelResource, RDFS.comment, model);
        MapperUtils.updateStringProperty(modelResource, Iow.contact, dto.getContact());
        MapperUtils.updateLocalizedProperty(langs, dto.getDocumentation(), modelResource, Iow.documentation, model);

        if(dto.getGroups() != null){
            modelResource.removeAll(DCTerms.isPartOf);
            var groupModel = jenaService.getServiceCategories();
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
		
		return model;
	}

	public SchemaInfoDTO mapToSchemaDTO(String PID, Model model) {
		return mapToSchemaDTO(PID, model, false);
	}
	
	public SchemaInfoDTO mapToSchemaDTO(String PID, Model model, boolean includeVersionData) {

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
		schemaInfoDTO.setOrganizations(OrganizationMapper.mapOrganizationsToDTO(organizations, jenaService.getOrganizations()));

		var created = modelResource.getProperty(DCTerms.created).getLiteral().getString();
		var modified = modelResource.getProperty(DCTerms.modified).getLiteral().getString();
		schemaInfoDTO.setCreated(created);
		schemaInfoDTO.setModified(modified);

		List<StoredFile> retrievedSchemaFiles = storageService.retrieveAllSchemaFiles(PID);
		Set<FileMetadata> fileMetadatas = new HashSet<>();
		retrievedSchemaFiles.forEach(file -> {
			fileMetadatas.add(new FileMetadata(file.contentType(), file.data().length, file.fileID()));
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
			List<SchemaRevision> revs = new ArrayList<SchemaRevision>();
 			var revisionsModel = jenaService.constructWithQuerySchemas(getSchemaRevisionsQuery(schemaInfoDTO.getAggregationKey()));			
			revisionsModel.listSubjects().forEach(res -> {
				revs.add(mapToSchemaRevision(res));				
			});
			List<SchemaRevision> orderedRevs = revs.stream()
					.sorted((SchemaRevision r1, SchemaRevision r2) -> r1.created.compareTo(r2.created))
					.collect(Collectors.toList());
			schemaInfoDTO.setRevisions(orderedRevs);
			
			List<SchemaVariant> variants = new ArrayList<SchemaVariant>();
			try {
	 			var variantsModel = jenaService.constructWithQuerySchemas(getSchemaVariantsQuery(PID, schemaInfoDTO.getNamespace()));			
	 			variantsModel.listSubjects().forEach(res -> {
					variants.add(mapToSchemaVariant(res));				
				});
				
			}catch(Exception ex) {
				log.error("Querying schema variants failed.", ex);
			}
			Map<String, List<SchemaVariant>> v2 = variants.stream()
					.collect(
							Collectors.groupingBy(SchemaVariant::aggregationKey));
			schemaInfoDTO.setVariants(variants);
			schemaInfoDTO.setVariants2(v2);

		
		}

		return schemaInfoDTO;
	}
	
	

	/**
	 * Add organization to a schema
	 * 
	 * @param modelDTO      Payload to get organizations from
	 * @param modelResource Model resource to add orgs to
	 */
    private void addOrgsToModel(SchemaDTO modelDTO, Resource modelResource) {
        var organizationsModel = jenaService.getOrganizations();
        modelDTO.getOrganizations().forEach(org -> {
            var orgUri = ModelConstants.URN_UUID + org;
            var queryRes = ResourceFactory.createResource(orgUri);
            if(organizationsModel.containsResource(queryRes)){
                modelResource.addProperty(DCTerms.contributor, organizationsModel.getResource(orgUri));
            }
        });
    }

    /**
     * Map a DataModel to a DataModelDocument
     * @param prefix Prefix of model
     * @param model Model
     * @return Index model
     */
    public IndexSchema mapToIndexModel(String pid, Model model){
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
        var types = resource.listProperties(RDF.type).mapWith(Statement::getResource).toList();
        if(types.contains(MSCR.SCHEMA)){
            indexModel.setType(MSCRType.SCHEMA);
        }else{
            throw new MappingError("RDF:type not supported for schema model");
        }
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
        var serviceCategories = jenaService.getServiceCategories();
        var groups = isPartOf.stream().map(serviceCat -> MapperUtils.propertyToString(serviceCategories.getResource(serviceCat), SKOS.notation)).collect(Collectors.toList());
        indexModel.setIsPartOf(groups);
        indexModel.setLanguage(MapperUtils.arrayPropertyToList(resource, DCTerms.language));

        return indexModel;
    }
    
    private Query getSchemaRevisionsQuery(String aggregationKey) {
    	var b = new ConstructBuilder();
    	var r = "?resource";
    	
   
    	SparqlUtils.addConstructProperty(r, b, RDFS.label, "?label");
    	SparqlUtils.addConstructProperty(r, b, DCTerms.created, "?created");
    	SparqlUtils.addConstructProperty(r, b, MSCR.versionLabel, "?versionLabel");    	
    	b.addWhere(r, MSCR.aggregationKey, ResourceFactory.createResource(aggregationKey));
    	Query q =  b.build();
		
    	return q;
  			
    }
    
    private Query  getSchemaVariantsQuery(String pid, String namespace) throws Exception {
    	var b = new ConstructBuilder();
    	var r = "?resource";
    	var pidResource = ResourceFactory.createResource(pid);
    	SparqlUtils.addConstructProperty(r, b, RDFS.label, "?label");
    	SparqlUtils.addConstructProperty(r, b, DCTerms.created, "?created");
    	SparqlUtils.addConstructProperty(r, b, MSCR.versionLabel, "?versionLabel");
    	SparqlUtils.addConstructProperty(r, b, MSCR.aggregationKey, "?aggregationKey2");
    	
    	b.addWhere(pidResource, MSCR.namespace, "?ns");
    	b.addWhere(pidResource, MSCR.aggregationKey, "?aggregationKey");
    	b.addWhere(r, MSCR.namespace, "?ns");
    	
    	b.addFilter("?aggregationKey != ?aggregationKey2");
    	
    	Query q = b.build();
    	
    	return q;
    }    
    
	
	public Query getAggregationKeyFromPrevQuery(String prevVersionPID) {
		
		var builder = new ConstructBuilder();
		Resource prevVersion = ResourceFactory.createResource(prevVersionPID);
        SparqlUtils.addConstructProperty("?version", builder, MSCR.aggregationKey, "?aggregationKey");
        builder.addWhere(prevVersion, MSCR.aggregationKey, "?aggregationKey");
        return builder.build();
	}    
    
    private SchemaRevision mapToSchemaRevision(Resource rev) {
    	var pid = rev.getURI();
    	var label = MapperUtils.localizedPropertyToMap(rev, RDFS.label);
    	var versionLabel = MapperUtils.propertyToString(rev, MSCR.versionLabel); 
    	var created = ((XSDDateTime)rev.getProperty(DCTerms.created).getLiteral().getValue()).asCalendar().getTime();    	
    	return new SchemaRevision(pid, created, label, versionLabel);
    }
    
    private SchemaVariant mapToSchemaVariant(Resource rev) {
    	var pid = rev.getURI();
    	var label = MapperUtils.localizedPropertyToMap(rev, RDFS.label);
    	var versionLabel = MapperUtils.propertyToString(rev, MSCR.versionLabel);
    	var aggregationKey = MapperUtils.propertyToString(rev, MSCR.aggregationKey);
    	return new SchemaVariant(pid, label, versionLabel, aggregationKey);
    }    
    
    public record SchemaRevision(String pid, java.util.Date created, Map<String, String> label, String versionLabel) {} 
    public record SchemaVariant(String pid, Map<String, String> label, String versionLabel, String aggregationKey) {}
    
	public SchemaDTO mapToSchemaDTO(SchemaInfoDTO source) {		
		SchemaDTO s = new SchemaDTO();
		s.setStatus(source.getStatus());
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
