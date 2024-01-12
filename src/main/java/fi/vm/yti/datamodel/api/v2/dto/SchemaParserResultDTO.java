package fi.vm.yti.datamodel.api.v2.dto;

public class SchemaParserResultDTO {

	private boolean isOk;
	private String message;
	private SchemaPart tree;	
	




	public boolean isOk() {
		return isOk;
	}



	public void setOk(boolean isOk) {
		this.isOk = isOk;
	}



	public SchemaPart getTree() {
		return tree;
	}



	public void setTree(SchemaPart tree) {
		this.tree = tree;
	}



	public String getMessage() {
		return message;
	}



	public void setMessage(String message) {
		this.message = message;
	}
	
}
