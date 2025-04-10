package org.openmrs.api.db.hibernate.search;

public class FieldAndValue {
	
	private final String field;
	
	private final Object value;
	
	public FieldAndValue(String field, Object value) {
		this.field = field;
		this.value = value;
	}
	
	public String getField() {
		return field;
	}
	
	public Object getValue() {
		return value;
	}
}
