package fi.vm.yti.datamodel.api.v2.mapper;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Service;

import fi.vm.yti.datamodel.api.v2.dto.Iow;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.MappingDTO;
import fi.vm.yti.datamodel.api.v2.dto.MappingFilterDTO;
import fi.vm.yti.datamodel.api.v2.dto.MappingInfoDTO;
import fi.vm.yti.datamodel.api.v2.dto.NodeInfo;
import fi.vm.yti.datamodel.api.v2.dto.OneOfDTO;
import fi.vm.yti.datamodel.api.v2.dto.ProcessingInfo;

@Service
public class MappingMapper {

	private Resource mapProcessingInfoToModel(ProcessingInfo pi, Model model) {
		Resource s = model.createResource();
		s.addProperty(MSCR.id, pi.getId());
		Bag params = model.createBag();
		for(String key : pi.getParams().keySet()) {
			Object value = pi.getParams().get(key);
			Resource item = model.createResource();
			item.addLiteral(MSCR.key, key);
			item.addLiteral(MSCR.value, value);
			params.add(item);			
		}
		s.addProperty(MSCR.processingParams, params);
		return s;
	}
	
	private ProcessingInfo mapProcessingInfoToDTO(Resource r) {
		ProcessingInfo pi = new ProcessingInfo();
		pi.setId(MapperUtils.propertyToString(r, MSCR.id));
		Bag bag = (Bag)r.getPropertyResourceValue(MSCR.processingParams);
		NodeIterator i = bag.iterator();
		Map<String, Object> params = new HashMap<String, Object>();
		while(i.hasNext()) {
			Resource paramResource = (Resource) i.next();
			params.put(
					MapperUtils.propertyToString(paramResource, MSCR.key),
					paramResource.getProperty(MSCR.value).getObject().asLiteral().getValue()
					);						
		}
		pi.setParams(params);
		return pi;
	}
	
	private Resource mapNodeInfoToModel(NodeInfo ni, Model model) {
		Resource sourceResource = model.createResource();
		sourceResource.addLiteral(MSCR.id, ni.getId());
		sourceResource.addLiteral(MSCR.label, ni.getLabel());
		if(ni.getProcessing() != null) {
			Resource p = mapProcessingInfoToModel(ni.getProcessing(), model);
			sourceResource.addProperty(MSCR.processing, p);
		}
		if(ni.getUri() != null) {
			Resource nodeUri = model.createResource(ni.getUri());
			sourceResource.addProperty(MSCR.uri, nodeUri);
		}
		return sourceResource;

	}

	private List<NodeInfo> mapNodeInfoToDTO(Seq items, Model model) {
		List<NodeInfo> infos = new ArrayList<NodeInfo>();
		NodeIterator ni = items.iterator();
		while(ni.hasNext()) {
			Resource item = (Resource)ni.next();
			NodeInfo nodeInfo = new NodeInfo();
			nodeInfo.setId(MapperUtils.propertyToString(item, MSCR.id));
			nodeInfo.setLabel(MapperUtils.propertyToString(item, MSCR.label));
			if(item.hasProperty(MSCR.processing)) {
				nodeInfo.setProcessing(mapProcessingInfoToDTO(item.getPropertyResourceValue(MSCR.processing)));
			}
			if(item.hasProperty(MSCR.uri)) {
				nodeInfo.setUri(MapperUtils.propertyToString(item, MSCR.uri));
			}

			infos.add(nodeInfo);
		}
		
		return infos;
		

	}
		
	private Resource mapMappingFilterToModel(MappingFilterDTO f, Model model) {
		Resource r = model.createResource();
		r.addLiteral(MSCR.path, f.getPath());
		r.addLiteral(MSCR.operator, f.getOperator());
		r.addLiteral(MSCR.value, f.getValue());		
		return r;
	}
	
	private Resource mapOneOfToModel(OneOfDTO o, Model model) {
		Resource oneOfResource = model.createResource();
		if(o.getFilter() != null) {
			oneOfResource.addProperty(MSCR.filter, mapMappingFilterToModel(o.getFilter(), model));	
		}
		o.getMappings().forEach(m -> oneOfResource.addProperty(MSCR.mappings, mapMappingToModel(m, model, model.createResource())));		
		return oneOfResource;		
	}
	
	private Resource mapMappingToModel(MappingDTO m, Model model, Resource mappingResource) {
		MapperUtils.addLiteral(mappingResource, MSCR.id, m.getId());	
		if(m.getSource() != null && m.getSource().size() > 0) {
			Seq items = model.createSeq();
			m.getSource().forEach(_r -> {
				Resource r = mapNodeInfoToModel(_r, model);
				items.add(r);
			});	
			mappingResource.addProperty(MSCR.source, items);
		}
		if(m.getTarget() != null && m.getTarget().size() > 0) {
			Seq items = model.createSeq();
			m.getTarget().forEach(_r -> {
				Resource r = mapNodeInfoToModel(_r, model);
				items.add(r);
			});	
			mappingResource.addProperty(MSCR.target, items);
		}
		if(m.getProcessing() != null) {
			mapProcessingInfoToModel(m.getProcessing(), model);
		}		
		mappingResource.addProperty(MSCR.predicate, ResourceFactory.createResource(m.getPredicate()));
		if(m.getOneOf() != null && m.getOneOf().size() > 0) {
			m.getOneOf().forEach(o -> mappingResource.addProperty(MSCR.oneOf, mapOneOfToModel(o, model)));
		}
		
		if(m.getNotes() != null) {
			mappingResource.addProperty(MSCR.notes, m.getNotes());
		}
		
		return mappingResource;		
	}	
	
	public Model mapToJenaModel(String mappingPID, MappingDTO dto, @NotNull String parentPID) {
		var model = ModelFactory.createDefaultModel();		
		var creationDate = new XSDDateTime(Calendar.getInstance());
		var mappingResource = model.createResource(mappingPID)
				.addProperty(RDF.type, MSCR.MAPPING)
				.addProperty(DCTerms.identifier, mappingPID)
				.addProperty(DCTerms.isPartOf, ResourceFactory.createResource(parentPID));


		mappingResource.addProperty(Iow.contentModified, ResourceFactory.createTypedLiteral(creationDate));
		
		mapMappingToModel(dto, model, mappingResource);		
		return model;
	}

	public MappingInfoDTO mapToMappingDTO(String mappingPID, Model model) {
		Resource r = model.getResource(mappingPID);
		
		MappingInfoDTO m = new MappingInfoDTO();
		m.setPID(mappingPID);
		m.setIsPartOf(r.getProperty(DCTerms.isPartOf).getResource().getURI());
		m.setId(MapperUtils.propertyToString(r, MSCR.id));
		
		Seq sources = (Seq)r.getProperty(MSCR.source).getSeq();
		m.setSource(mapNodeInfoToDTO(sources, model));
		Seq targets = (Seq)r.getProperty(MSCR.target).getSeq();
		m.setTarget(mapNodeInfoToDTO(targets, model));

		if(r.hasProperty(MSCR.processing)) {
			m.setProcessing(mapProcessingInfoToDTO(r.getPropertyResourceValue(MSCR.processing)));
		}
		m.setPredicate(MapperUtils.propertyToString(r, MSCR.predicate));
		m.setIsPartOf(MapperUtils.propertyToString(r, DCTerms.isPartOf));
		
		m.setNotes(MapperUtils.propertyToString(r, MSCR.notes));
		return m;
	}

}
