package com.profitera.dc.parser.exception;

import com.profitera.util.exception.GenericException;

public class InvalidConfigurationException extends GenericException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public InvalidConfigurationException(String msg){
		super(msg);
	}
	
	public InvalidConfigurationException(Throwable e){
		super(e);
	}
	
	public InvalidConfigurationException(String msg, Throwable e){
		super(msg, e);
	}

	@Override
	public String getErrorCode() {
		if(getCause()!=null && getCause() instanceof GenericException){
			return ((GenericException)getCause()).getErrorCode();
		}
		return "DL_INVALID_CONFIGURATION";
	}
	
}
