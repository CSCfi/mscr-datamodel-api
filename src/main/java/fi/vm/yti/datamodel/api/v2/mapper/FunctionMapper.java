package fi.vm.yti.datamodel.api.v2.mapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFList;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.vocabulary.RDF;

import fi.vm.yti.datamodel.api.v2.dto.FunctionDTO;
import fi.vm.yti.datamodel.api.v2.dto.MSCR;
import fi.vm.yti.datamodel.api.v2.dto.OutputDTO;
import fi.vm.yti.datamodel.api.v2.dto.ParameterDTO;

public class FunctionMapper {
	
	public static List<FunctionDTO> mapFunctionsToDTO(Model model) {
		
		List<FunctionDTO> list = new ArrayList<FunctionDTO>();
		ResIterator ri = model.listResourcesWithProperty(RDF.type, MSCR.FnO_FUNCTION);
		while(ri.hasNext()) {
			Resource f = ri.next();
			FunctionDTO dto = new FunctionDTO();
			String name = MapperUtils.propertyToString(f, MSCR.FnO_name);
			String uri = f.getURI();
			String desc = "";
			dto.setName(name);
			dto.setUri(uri);
			dto.setDescription(desc);
			
			if(f.hasProperty(MSCR.FnO_expects)) {
				List<ParameterDTO> paramList = new ArrayList<ParameterDTO>();
				ExtendedIterator<RDFNode> i = f.getProperty(MSCR.FnO_expects).getList().iterator();
				while(i.hasNext()) {
					Resource paramResource = (Resource) i.next();
					String paramName = MapperUtils.propertyToString(paramResource, MSCR.FnO_name);
					String paramDatatype = MapperUtils.propertyToString(paramResource, MSCR.FnO_type);
					boolean isRequired = paramResource.getProperty(MSCR.FnO_required).getBoolean();
					ParameterDTO param = new ParameterDTO(paramName, paramDatatype, isRequired);
					paramList.add(param);					
				}
				dto.setParameters(paramList);
			}
			if(f.hasProperty(MSCR.FnO_returns)) {
				List<OutputDTO> outputList = new ArrayList<OutputDTO>();
				ExtendedIterator<RDFNode> i = f.getProperty(MSCR.FnO_returns).getList().iterator();
				while(i.hasNext()) {
					Resource outputResource = (Resource) i.next();
					String outputName = MapperUtils.propertyToString(outputResource, MSCR.FnO_name);
					String outputDatatype = MapperUtils.propertyToString(outputResource, MSCR.FnO_type);
					boolean isRequired = outputResource.getProperty(MSCR.FnO_required).getBoolean();
					OutputDTO param = new OutputDTO(outputName, outputDatatype, isRequired);
					outputList.add(param);					
				}
				dto.setOutputs(outputList);
			}
			list.add(dto);
		}
		return list;
	}

}
