package com.profitera.dc.filehandler;
import java.util.HashMap;
import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


public class XMLHandoffFileHandler extends DefaultHandler {

	private boolean started = false;
	private String recordTag = null;
	private Map record = null;
	private IXMLRecordIterator recordIterator = null;
	private String currentElement = null;
	private String currentValue = null;
	
	private XMLHandoffFileHandler(){};
	
	public XMLHandoffFileHandler(String recordTag, IXMLRecordIterator recordIterator){
		this.recordTag = recordTag;
		this.recordIterator = recordIterator;
	}

	public void startDocument() throws SAXException {
	}

	public void endDocument() throws SAXException {
	}

	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		String element = localName;
		if ("".equals(element))
			element = qName;
		if (recordTag == null || recordTag.trim().length() == 0)
			recordTag = element;
		if (element.equalsIgnoreCase(recordTag)){
			started = true;
			record = new HashMap();
		}
		currentElement = element;
	}

	public void characters(char ch[], int start, int length) throws SAXException {
		if (started)
			currentValue = new String(ch, start, length);
	}
	
  public void endElement (String uri, String localName, String qName) throws SAXException {
		String element = localName;
		if ("".equals(element))
			element = qName;
		if (element.equalsIgnoreCase(recordTag)){
			started = false;
			recordIterator.put(record);
		}
		if (started)
			record.put(currentElement, currentValue);
  }
}