package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {
	public BufferedReader bufferedReader = null;
	public Header header = null;
	String dataRow ="";

	// Parameterized constructor to initialize filename
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		bufferedReader = new BufferedReader(new FileReader(fileName));		
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 * Note: Return type of the method will be Header
	 */
	
	@Override
	public Header getHeader() throws IOException {
		String[] headerNames =  null;
		if(header == null) {
			// read the first line
			String headerLine = bufferedReader.readLine();
			headerNames = headerLine .split(",");
			// populate the header object with the String array containing the header names
			header = new Header(headerNames);
		}
		return header;
	}

	/**
	 * getDataRow() method will be used in the upcoming assignments
	 * @return 
	 */
	
	@Override
	public void getDataRow() {
		try {
			dataRow = bufferedReader.readLine();
		} catch (IOException ioexception) {
			//System.out.println("ioexception in getDataRow");
		}
	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. If a
	 * specific field value can be converted to Integer, the data type of that field
	 * will contain "java.lang.Integer", otherwise if it can be converted to Double,
	 * then the data type of that field will contain "java.lang.Double", otherwise,
	 * the field is to be treated as String. 
	 * Note: Return Type of the method will be DataTypeDefinitions
	 */
	
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		if(dataRow.isBlank()){
			getDataRow();
		}
		String value = "";
		String dataType = "";
		String[] dataStringArray = dataRow.split(",");
		String[] dataTypes = new String[header.getHeaders().length];
		//to find the datatype
		for(int row = 0;row<dataStringArray.length;row++) {
			value = dataStringArray[row];
			try {
				Integer.parseInt(value);
				dataType = "java.lang.Integer";
			}catch(NumberFormatException intNumFormatEx) {
				try {
					Double.parseDouble(value);
					dataType = "java.lang.Double";
				}catch(NumberFormatException doubleNumFormatEx){
					dataType = "java.lang.String";
				}
			}finally {
				dataTypes[row] = dataType;
			}
		}
		int index = 0;
		for(String type:dataTypes) {
			if(type == null || type.isBlank()) {
				dataTypes[index] = "java.lang.String";
			}
			index++;
		}
		DataTypeDefinitions dataTypeDef = new DataTypeDefinitions(dataTypes);
		return dataTypeDef;
	}
}
