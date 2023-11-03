package fi.vm.yti.datamodel.api.v2.dto;

public class MappingFilterDTO {

	private String path;
	private String operator;
	private Object value;
	private boolean distinctValues = false;
	public MappingFilterDTO() {
		
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public boolean isDistinctValues() {
		return distinctValues;
	}

	public void setDistinctValues(boolean distinctValues) {
		this.distinctValues = distinctValues;
	}
	
	
}

