package fi.vm.yti.datamodel.api.v2.transformation;

import java.util.List;

import org.apache.jena.rdf.model.Model;

import fi.vm.yti.datamodel.api.v2.dto.MappingInfoDTO;

public class SPARQLGenerator {

	public String generateRDFtoCSV(List<MappingInfoDTO> mappings, Model sourceModel) {
		String q = "";
		String select = "";
		for(MappingInfoDTO mapping : mappings) {
			select = select + mapping.getTarget().get(0).getLabel() + " ";
			// row iterator as rdf:type 
			
		}
		return q;
	}
}
