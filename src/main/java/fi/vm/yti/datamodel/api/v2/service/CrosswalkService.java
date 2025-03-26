package fi.vm.yti.datamodel.api.v2.service;

import java.io.StringReader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.SKOS;
import org.springframework.stereotype.Service;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.CSVReaderHeaderAwareBuilder;

import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.dto.NodeInfo;
import fi.vm.yti.datamodel.api.v2.mapper.MapperUtils;
import fi.vm.yti.datamodel.api.v2.mapper.MappingMapper;

@Service
public class CrosswalkService {

	
	private final MappingMapper mappingMapper;
	
	public CrosswalkService(MappingMapper mappingMapper) {
		this.mappingMapper = mappingMapper;
	}
	
	public Model transformSSSOMToInternal(String handle, String pid, byte[] fileInBytes, String sourcePID, String sourceFormat, Model sourceModel, String targetPID, String targetFormat, Model targetModel) throws Exception {
		Model m = ModelFactory.createDefaultModel();
		StringReader sr = new StringReader(new String(fileInBytes, StandardCharsets.UTF_8));
		
		CSVParser parser = new CSVParserBuilder().withSeparator(';').build();
		CSVReaderHeaderAware reader = new CSVReaderHeaderAwareBuilder(sr).withCSVParser(parser).build();
		
		Resource crosswalkResource = m.createResource(pid);
		Map<String, String> line = null;
		while ((line = reader.readMap()) != null) {
            String subjectId = line.getOrDefault("subject_id", null);
            String subjectLabel = line.getOrDefault("subject_label", "");
            String subjectType = line.getOrDefault("subject_type", null);
            String objectId = line.getOrDefault("object_id", null);
            String objectLabel = line.getOrDefault("object_label", "");
            String objectType = line.getOrDefault("object_type", null);
            String predicateId = line.getOrDefault("predicate_id", "http://www.w3.org/2004/02/skos/core#exactMatch");
            String comment = line.getOrDefault("comment", null);
            
            
            Resource subjectResource = null;
            Resource objectResource = null;
            
            if(subjectId == null || (subjectType != null && subjectType.equals("rdfs literal"))) {
            	// source format must be ENUM 
            	if(!sourceFormat.equals("ENUM")) {
            		throw new RuntimeException("subject id is null or subject type is rdfs literal, but source schema is not of type ENUM.");
            	}
            	subjectResource = ResourceFactory.createResource(sourcePID + "/" + URLEncoder.encode(subjectLabel));
            }
            else {            	
            	subjectResource = ResourceFactory.createResource(subjectId);
            }
        	if(!sourceModel.containsResource(subjectResource)) {
        		throw new RuntimeException("Subject id " + (subjectId != null ? subjectId : subjectLabel) + " not found in the source schema.");
        	}
            
            if(objectId == null || (objectType != null && objectType.equals("rdfs literal"))) {
            	if(!targetFormat.equals("ENUM")) {
            		throw new RuntimeException("object id is null or object type is rdfs literal, but target schema is not of type ENUM.");
            	}            	
            	objectResource = ResourceFactory.createResource(targetPID + "/" + URLEncoder.encode(objectLabel));
            }
            else {
            	objectResource = ResourceFactory.createResource(objectId);
            	
            }                          
        	if(!targetModel.containsResource(objectResource)) {
        		throw new RuntimeException("Object id " + (objectId != null ? objectId : objectLabel)  + " not found in the target schema.");
        	}
            
    		String mappingID = UUID.randomUUID().toString();
    		String mappingPID = pid + "@mapping=" + mappingID;
    		if(handle != null) {
    			mappingPID = handle + "@mapping=" + mappingID;

    		}
        	MappingDTO dto = new MappingDTO();
            
            List<NodeInfo> sources = new ArrayList<NodeInfo>();
            NodeInfo source = new NodeInfo();
            source.setId(subjectResource.getURI());
            source.setUri(subjectResource.getURI());
            String originalSubjectLabel = sourceModel.getResource(subjectResource.getURI()).getRequiredProperty(SKOS.prefLabel).getString();
            source.setLabel(originalSubjectLabel);
            sources.add(source);
            
            List<NodeInfo> targets = new ArrayList<NodeInfo>();
            NodeInfo target = new NodeInfo();
            target.setId(objectResource.getURI());
            target.setUri(objectResource.getURI());
            String originalObjectLabel = targetModel.getResource(objectResource.getURI()).getRequiredProperty(SKOS.prefLabel).getString();

            target.setLabel(originalObjectLabel);
            targets.add(target);
            
            dto.setSource(sources);
            dto.setTarget(targets);
            dto.setPredicate(predicateId);
            dto.setNotes(comment);
            m.add(mappingMapper.mapToJenaModel(mappingPID, mappingID, dto, pid));
            crosswalkResource.addProperty(MSCR.mappings, ResourceFactory.createResource(mappingPID));
                       
		}	
		return m;
	}
	
}
