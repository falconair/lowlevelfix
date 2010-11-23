package com.llfix.util;

final public class FieldAndRequirement {

	final private String tag;
	final private boolean isRequired;

	public FieldAndRequirement(String tag, boolean isRequired) {
		super();
		this.tag = tag;
		this.isRequired = isRequired;
	}

	public String getTag() {
		return tag;
	}

	public boolean isRequired() {
		return isRequired;
	}




}
