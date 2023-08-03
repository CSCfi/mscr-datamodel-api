package fi.vm.yti.datamodel.api.v2.mapper.mscr;

import org.apache.jena.rdf.model.Model;

public interface SchemaMapper {
	
	/*
	 * Used for processing incoming schema description into an internal model
	 * 
	 */
	public Model mapToModel(String PID, byte[] data);
	

}
