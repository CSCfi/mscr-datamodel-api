package fi.vm.yti.datamodel.api.v2.service;

import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.springframework.stereotype.Service;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.CSVReaderHeaderAwareBuilder;

import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.dto.NodeInfo;
import fi.vm.yti.datamodel.api.v2.mapper.MappingMapper;

@Service
public class CrosswalkService {

	
	private final MappingMapper mappingMapper;
	
	public CrosswalkService(MappingMapper mappingMapper) {
		this.mappingMapper = mappingMapper;
	}
	
	public Model transformSSSOMToInternal(String pid, byte[] fileInBytes, Model sourceModel, Model targetModel) throws Exception {
		Model m = ModelFactory.createDefaultModel();
		StringReader sr = new StringReader(new String(fileInBytes, StandardCharsets.UTF_8));
		
		CSVParser parser = new CSVParserBuilder().withSeparator('\t').build();
		CSVReaderHeaderAware reader = new CSVReaderHeaderAwareBuilder(sr).withCSVParser(parser).build();
		
		Map<String, String> line = null;
		while ((line = reader.readMap()) != null) {
            String subjectId = line.getOrDefault("subject_id", null);
            String subjectLabel = line.getOrDefault("subject_label", "");
            String objectId = line.getOrDefault("object_id", null);
            String objectLabel = line.getOrDefault("object_label", "");
            String predicateId = line.get("predicate_id");
            String comment = line.getOrDefault("comment", null);
            
            Resource subjectResource = ResourceFactory.createResource(subjectId);
            Resource objectResource = ResourceFactory.createResource(objectId);
            
            if(sourceModel.containsResource(subjectResource) && targetModel.containsResource(objectResource)) {
                String mappingPID = pid + "@mapping=" + UUID.randomUUID();
                MappingDTO dto = new MappingDTO();
                
                List<NodeInfo> sources = new ArrayList<NodeInfo>();
                NodeInfo source = new NodeInfo();
                source.setId(subjectId);
                source.setUri(subjectId);
                source.setLabel(subjectLabel);
                sources.add(source);
                
                List<NodeInfo> targets = new ArrayList<NodeInfo>();
                NodeInfo target = new NodeInfo();
                target.setId(objectId);
                target.setUri(objectId);
                target.setLabel(objectLabel);
                targets.add(target);
                
                dto.setSource(sources);
                dto.setTarget(targets);
                dto.setPredicate(predicateId);
                dto.setNotes(comment);
                m.add(mappingMapper.mapToJenaModel(mappingPID, dto, pid));            	
            }
            
        }
		
		return m;
		
	}

	
	
}
