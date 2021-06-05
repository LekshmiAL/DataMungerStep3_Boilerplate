package com.stackroute.datamunger.query;

public class DataTypeDefinitions {

	/*
	 * This class should contain a member variable which is a String array, to hold
	 * the data type for all columns for all data types
	 */
	String[] dataTypes;
	
	/**
	 * constructor
	 * @param dataTypes
	 */
	public DataTypeDefinitions(String[] dataTypes) {
		this.dataTypes = dataTypes;
	}
	/**
	 * @param dataTypes the dataTypes to set
	 */
	public void setDataTypes(String[] dataTypes) {
		this.dataTypes = dataTypes;
	}
	/**
	 * 
	 * @return
	 */
	public String[] getDataTypes() {
		return dataTypes;
	}
}
