package com.stackroute.datamunger.query;

public class Header {

	/*
	 * This class should contain a member variable which is a String array, to hold
	 * the headers.
	 */
	String[] headers;
	
	/**
	 * constructor
	 * @param headers
	 */
	public Header(String[] headers) {
		this.headers = headers;
	}
	/**
	 * @param headers the headers to set
	 */
	public void setHeaders(String[] headers) {
		this.headers = headers;
	}
	/**
	 * 
	 * @return
	 */
	public String[] getHeaders() {
		return headers;
	}

}
