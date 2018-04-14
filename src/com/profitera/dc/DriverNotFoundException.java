package com.profitera.dc;

public class DriverNotFoundException extends RuntimeException{

	public DriverNotFoundException(String driverName){
		super("Failed to load database driver " + driverName);
	}
}
