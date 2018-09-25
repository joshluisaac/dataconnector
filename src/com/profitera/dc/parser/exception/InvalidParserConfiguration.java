package com.profitera.dc.parser.exception;

public class InvalidParserConfiguration extends RuntimeException {
	String property = null;
	String value = null;
	
	private InvalidParserConfiguration(){};
	
	public InvalidParserConfiguration(String property, String value) {
		this.property = property;
		this.value = value;
	}

	public InvalidParserConfiguration(String property, String value, Throwable cause) {
		super(cause);
		this.property = property;
		this.value = value;
	}

	public String getMessage() {
		return "Configuration key '" + property + "' has invalid value of '" + value + "'"; 
	}
}