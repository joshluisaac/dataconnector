package com.profitera.dc.parserconverter;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.profitera.dc.parser.LoadDefinitionParser;
import com.profitera.dc.parser.XMLConstantKey;



/**
 * @author antoni
 *
 */
public class PropertiesToXMLParser extends LoadDefinitionParser implements XMLConstantKey{
	
	private DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
	private DocumentBuilder domBuilder = null;
	private Document document = null;
	
	private String propFileName;
	private String xmlFileName;
	
	private String tab(int depth){
		String tabs = "";
		for (int i = 0 ; i  < depth ;i++)
			tabs += "    ";
		return tabs;
	}
	private Node xmlNewLineAndTab(int depth){
		return document.createTextNode("\n"+tab(depth));
	}

	private void createXMLDocument() throws IOException, Exception{
		
		
		try{
			Properties props = new Properties();
      props.load(new FileInputStream(propFileName));
			
			// collect all field name
			// copy code from LoadDefintionParser
			List fields = new ArrayList();
			for (Iterator i = props.keySet().iterator(); i.hasNext();) {
		      String key = (String) i.next();
		      if(key.endsWith(LOCATION_SUFFIX))
		        fields.add(key.substring(0, key.length() - LOCATION_SUFFIX.length()));
		    }
			
			InputStream is = new FileInputStream(propFileName);
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			
			Element load = document.createElement(XML_LOAD_KEY);
			document.appendChild(load);
			
			String line = null;
			int separator = 0;
			String key = "";
			String value = "";
			
			HashMap mainElement = new HashMap();
			
			while(true){
				// is under load
				// is under field
				
				line = br.readLine();
				
				if (line == null) break;
				if (line.trim().length() == 0) continue;
				boolean isComment = false;
				if (line.startsWith("#")){
					isComment = true;
					line = line.substring(1);
				}
				separator = line.indexOf("=");
				
				if (separator == -1){
					load.appendChild(document.createComment("#"+line));
					continue;
				}else{
					key = line.substring(0,separator).trim();
					value = line.substring(separator+1,line.length()).trim();
					
				}
				
				if (key.equals(LOAD_TYPE_KEY)){
					appendAttr(load,XML_TYPE_KEY,value,line,isComment,1);
					continue;
				}
				
				if (key.equals(DELIMITER)){
					appendAttr(load,XML_DELIMITER_KEY,value,line,isComment,1);
					continue;
				}
				
				if (key.equals(PAD_LINE_KEY)){
					appendAttr(load,XML_PAD_LINE_KEY,value,line,isComment,1);
					continue;
				}
				
				if (key.equals(TABLE_KEY)){
					appendElement(load,XML_TABLE_KEY,value,line,isComment,1);
					continue;
				}
				
				if (key.equals(GENERATED_KEY_KEY)){
					appendElement(load,XML_GENERATED_KEY_KEY,value,line,isComment,1);
					continue;
				}
				
				if (key.equals(GENERATED_KEY_SEQ_NAME_KEY)){
					appendElement(load,XML_GENERATED_KEY_SEQ_NAME_KEY,value,line,isComment,1);
					continue;
				}
				
				if (key.equals(XML_START_TAG)){
					appendElement(load,XML_XML_START_TAG,value,line,isComment,1);
					continue;
				}
				
				if (key.equals(XQUERY)){
					appendElement(load,XML_XQUERY,value,line,isComment,1);
					continue;
				}
				
				if (key.equals(REFRESH_DATA_KEY) || key.equals(REFRESH_DATA_QUERY)){
					Element node = (Element) mainElement.get(XML_REFRESH_DATA_KEY);
					if (node == null)
						mainElement.put(XML_REFRESH_DATA_KEY,appendElement(load,XML_REFRESH_DATA_KEY,"",line,isComment,1));
				}
				
				if (key.equals(REFRESH_DATA_KEY)){
					Element parent = (Element)mainElement.get(XML_REFRESH_DATA_KEY);
					appendAttr(parent,XML_KEY,value,line,isComment,1);
					continue;
				}

				if (key.equals(REFRESH_DATA_QUERY_TIMEOUT)){
					Element parent = (Element)mainElement.get(XML_REFRESH_DATA_KEY);
					appendAttr(parent,XML_REFRESH_DATA_QUERY_TIMEOUT,value,line,isComment,1);
					continue;
				}

				if (key.equals(REFRESH_DATA_QUERY)){
					Element parent = (Element)mainElement.get(XML_REFRESH_DATA_KEY);
					parent.appendChild(document.createTextNode(value));
					continue;
				}

				if (key.equals(POST_INSERT_INSERTION_KEY) || key.equals(POST_INSERT_UPDATE_KEY) ||
					key.equals(POST_UPDATE_INSERTION_KEY) || key.equals(POST_UPDATE_UPDATE_KEY)){
					Element theNode = (Element) mainElement.get(XML_POST_KEY);
					if (theNode == null)
						mainElement.put(XML_POST_KEY,appendElement(load,XML_POST_KEY,"",line,false,1));
					
				}
				
				if (key.equals(POST_INSERT_INSERTION_KEY)){
					Element parent = (Element) mainElement.get(XML_POST_KEY);
					appendElement(parent,XML_INSERT_INSERTION_KEY,value,line,isComment,2);
					continue;
				}
				
				if (key.equals(POST_INSERT_UPDATE_KEY)){
					Element parent = (Element) mainElement.get(XML_POST_KEY);
					appendElement(parent,XML_INSERT_UPDATE_KEY,value,line,isComment,2);
					continue;
				}
				
				if (key.equals(POST_UPDATE_INSERTION_KEY)){
					Element parent = (Element) mainElement.get(XML_POST_KEY);
					appendElement(parent,XML_UPDATE_INSERTION_KEY,value,line,isComment,2);
					continue;
				}
				
				if (key.equals(POST_UPDATE_UPDATE_KEY)){
					Element parent = (Element) mainElement.get(XML_POST_KEY);
					appendElement(parent,XML_UPDATE_UPDATE_KEY,value,line,isComment,2);
					continue;
				}
				
				if (key.equals(SELECT_QUERY_WHERE_KEY) || key.equals(UPDATE_QUERY_WHERE_KEY)){
					Element theNode = (Element) mainElement.get(XML_QUERY_WHERE_KEY);
					if (theNode == null)
						mainElement.put(XML_QUERY_WHERE_KEY,appendElement(load,XML_QUERY_WHERE_KEY,"",line,false,1));
				}
				
				if(key.equals(SELECT_QUERY_WHERE_KEY)){
					Element parent = (Element) mainElement.get(XML_QUERY_WHERE_KEY);
					appendElement(parent,XML_SELECT_KEY,value,line,isComment,2);
					continue;
				}
				
				if (key.equals(UPDATE_QUERY_WHERE_KEY)){
					Element parent = (Element) mainElement.get(XML_QUERY_WHERE_KEY);
					appendElement(parent,XML_UPDATE_KEY,value,line,isComment,2);
					continue;
				}
				
				if (key.endsWith(LOCATION_SUFFIX)  || key.endsWith(DEFAULT_SUFFIX)  || key.endsWith(HANDLER_SUFFIX)  ||
					key.endsWith(HANDLER_ARGS_SUFFIX)  || key.endsWith(LOOKUP_DATA_TYPE)  || key.endsWith(LOOKUP_QUERY_SUFFIX)  ||
					key.endsWith(LOOKUP_INSERT_SUFFIX)  || key.endsWith(LOOKUP_KEY_GEN_SUFFIX)  || key.endsWith(FILTER_ON_SUFFIX)  ||
					key.endsWith(NULL_DEFINITION_SUFFIX)  || key.endsWith(IS_KEY_SUFFIX)  || key.endsWith(IS_EXTERNAL_SUFFIX)  || 
					key.endsWith(IS_OPTIONAL_SUFFIX)  || key.endsWith(CACHE) ){
					
					String keyName = key.substring(0,key.lastIndexOf("_"));
					
					Element field = (Element) mainElement.get(keyName);
					if (field == null){
						field = (Element)appendElement(load,XML_FIELD_KEY,"",line,false,1);
						Element fieldName = (Element) appendElement(field,XML_FIELD_NAME,keyName,"",false,2);
						
						field.appendChild(fieldName);
						mainElement.put(keyName,field);
					}
					
					
					if (mainElement.get(keyName+XML_HANDLER_SUFFIX) == null &&(key.endsWith(HANDLER_SUFFIX)  || key.endsWith(HANDLER_ARGS_SUFFIX))){
							Element fieldHandler = (Element) appendElement(field,XML_HANDLER_SUFFIX,"",line,false,2);
							mainElement.put(keyName+XML_HANDLER_SUFFIX,fieldHandler);
						}
					
					if (mainElement.get(keyName+XML_LOOKUP_KEY) == null &&(key.endsWith(LOOKUP_DATA_TYPE)  || key.endsWith(LOOKUP_QUERY_SUFFIX)  ||
						key.endsWith(LOOKUP_INSERT_SUFFIX)  || key.endsWith(LOOKUP_KEY_GEN_SUFFIX) )){
						Element fieldLookup = (Element) appendElement(field,XML_LOOKUP_KEY,"",line,false,2);
						mainElement.put(keyName+XML_LOOKUP_KEY,fieldLookup);
					}
					
					if (mainElement.get(keyName+XML_FILTER_ON_SUFFIX) == null && key.endsWith(FILTER_ON_SUFFIX)){
						Element fieldFilter = (Element) appendElement(field,XML_FILTER_ON_SUFFIX,"",line,false,2);
						mainElement.put(keyName+XML_FILTER_ON_SUFFIX,fieldFilter);
					}
					
					if (mainElement.get(keyName+NULL_DEFINITION_SUFFIX) == null && key.endsWith(NULL_DEFINITION_SUFFIX)){
						Element fieldNullDef = (Element) appendElement(field,XML_NULL_DEFINITION_SUFFIX,"",line,false,2);
						mainElement.put(keyName+XML_NULL_DEFINITION_SUFFIX,fieldNullDef);
					}
				}
					
				
				if(key.endsWith(LOCATION_SUFFIX)){
					key = key.substring(0,key.indexOf(LOCATION_SUFFIX));
					Element field = (Element) mainElement.get(key);
					
					Element location = (Element) appendElement(field,XML_LOCATION_SUFFIX,"",line,isComment,2);
					if (location != null){
						String [] values = value.split("-");
						if (values.length > 2){
							appendElement(field,XML_LOCATION_SUFFIX,"",line,true,3);
						}else if (values.length == 2 ){
							appendAttr(location,XML_LOCATION_START,values[0],"",false,3);
							appendAttr(location,XML_LOCATION_END,values[1],"",false,3);
						}else {
							appendAttr(location,XML_LOCATION_START,values[0],"",false,3);
							appendAttr(location,XML_LOCATION_END,"","",false,3);
						}
					}
					continue;
				}
				
				if (key.endsWith(DEFAULT_SUFFIX)){
					key = key.substring(0,key.indexOf(DEFAULT_SUFFIX));
					Element field = (Element) mainElement.get(key);
					appendElement(field,XML_DEFAULT_SUFFIX,value,line,isComment,2);
					continue;
				}
				
				if (key.endsWith(HANDLER_SUFFIX)){
					key = key.substring(0,key.indexOf(HANDLER_SUFFIX));
					Element fieldHandler = (Element) mainElement.get(key+XML_HANDLER_SUFFIX);
					fieldHandler.appendChild(xmlNewLineAndTab(3));
					fieldHandler.appendChild(document.createTextNode(value));
					continue;
				}
				
				if (key.endsWith(HANDLER_ARGS_SUFFIX)){
					key = key.substring(0,key.indexOf(HANDLER_ARGS_SUFFIX));
					Element fieldHandler = (Element) mainElement.get(key+XML_HANDLER_SUFFIX);
					appendElement(fieldHandler,XML_HANDLER_ARGS_SUFFIX,value,line,isComment,3);
					continue;
				}
				
				if (key.endsWith(LOOKUP_INSERT_SUFFIX)){
					key = key.substring(0,key.indexOf(LOOKUP_INSERT_SUFFIX));
					Element fieldLookup = (Element) mainElement.get(key+XML_LOOKUP_KEY);
					appendElement(fieldLookup,XML_INSERT_KEY,value,line,isComment,3);
					continue;
				}
				if (key.endsWith(LOOKUP_DATA_TYPE)){
					key = key.substring(0,key.indexOf(LOOKUP_DATA_TYPE));
					Element fieldLookup = (Element) mainElement.get(key+XML_LOOKUP_KEY);
					appendAttr(fieldLookup,XML_TYPE_KEY,value,line,isComment,3);
					continue;
				}
				if (key.endsWith(LOOKUP_KEY_GEN_SUFFIX)){
					key = key.substring(0,key.indexOf(LOOKUP_KEY_GEN_SUFFIX));
					Element fieldLookup = (Element) mainElement.get(key+XML_LOOKUP_KEY);
					appendElement(fieldLookup,XML_INSERT_KEY_KEY,value,line,isComment,3);
					continue;
				}
				if (key.endsWith(LOOKUP_QUERY_SUFFIX)){	
					key = key.substring(0,key.indexOf(LOOKUP_QUERY_SUFFIX));
					Element fieldLookup = (Element) mainElement.get(key+XML_LOOKUP_KEY);
					appendElement(fieldLookup,XML_QUERY_KEY,value,line,isComment,3);
					continue;
				}
				
				if (key.endsWith(IS_KEY_SUFFIX)){
					key = key.substring(0,key.indexOf(IS_KEY_SUFFIX));
					Element field = (Element) mainElement.get(key);
					appendAttr(field,XML_IS_KEY_SUFFIX,value,line,isComment,2);
					continue;
				}
				
				if (key.endsWith(IS_OPTIONAL_SUFFIX)){
					key = key.substring(0,key.indexOf(IS_OPTIONAL_SUFFIX));
					Element field = (Element) mainElement.get(key);
					appendAttr(field,XML_IS_OPTIONAL_SUFFIX,value,line,isComment,2);
					continue;
				}
				
				if(key.endsWith(IS_EXTERNAL_SUFFIX)){
					key = key.substring(0,key.indexOf(IS_EXTERNAL_SUFFIX));
					Element field = (Element) mainElement.get(key);
					appendAttr(field,XML_IS_EXTERNAL_SUFFIX,value,line,isComment,2);
					continue;
				}
				
				if (key.endsWith(CACHE)){
					key = key.substring(0,key.indexOf(CACHE));
					Element field = (Element) mainElement.get(key);
					appendAttr(field,XML_CACHE,value,line,isComment,2);
					continue;
				}
				
				if (key.endsWith(FILTER_ON_SUFFIX)){
					key = key.substring(0,key.indexOf(FILTER_ON_SUFFIX));
					Element field = (Element) mainElement.get(key+XML_FILTER_ON_SUFFIX);
					String [] vals = value.split(";");
					for(int i=0; i < vals.length; i++)
						appendElement(field,XML_VALUE_KEY,vals[i],line,isComment,4);
					continue;
				}
				
				if (key.endsWith(NULL_DEFINITION_SUFFIX)){
					key = key.substring(0,key.indexOf(NULL_DEFINITION_SUFFIX));
					Element field = (Element) mainElement.get(key+XML_NULL_DEFINITION_SUFFIX);
					String [] vals = value.split(";");
					for(int i=0; i < vals.length; i++)
						appendElement(field,XML_VALUE_KEY,vals[i],line,isComment,4);
					continue;
				}
				
				
				if (key.endsWith(HEADER_NAME_KEY)){
					Element header = (Element) mainElement.get(XML_HEADER_KEY);
					if (header == null){
						header = (Element) appendElement(load,XML_HEADER_KEY,"",line,false,1);
						mainElement.put(XML_HEADER_KEY,header);
					}
					String [] vals = value.split(";");
					for(int i=0; i < vals.length; i++)
						appendElement(header,XML_VALUE_KEY,vals[i],line,isComment,2);
					continue;
				}

				if (key.endsWith(TRAILER_NAME_KEY)){
					Element trailer = (Element) mainElement.get(XML_TRAILER_KEY);
					if (trailer == null){
						trailer = (Element) appendElement(load,XML_TRAILER_KEY,"",line,false,1);
						mainElement.put(XML_TRAILER_KEY,trailer);
					}
					String [] vals = value.split(";");
					for(int i=0; i < vals.length; i++)
						appendElement(trailer,XML_VALUE_KEY,vals[i],line,isComment,2);
					
					continue;
				}
				
				// doesnot fall in any keywords
				
				appendElement(load,"invalid keyword","",line,true,1);
			}
			br.close();
			is.close();
			isr.close();
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	private void appendAttr(Element parent,String name, String value, String full, boolean isComment,int ident){
	  if (parent == null) {
	    return;
	  }
		if (isComment){
			parent.appendChild(xmlNewLineAndTab(ident));
			parent.appendChild(document.createComment("#"+full));
		}else{
			Attr attr = document.createAttribute(name);
			attr.setNodeValue(value);
				
			parent.setAttributeNode(attr);
		}
	}
	
	private Node appendElement(Element parent, String name, String value, String full, boolean isComment,int ident){
		if (isComment){
			parent.appendChild(xmlNewLineAndTab(ident));
			parent.appendChild(document.createComment("#"+full));
			
			return null;
		}
		else{
			Node element = document.createElement(name);
			element.appendChild(document.createTextNode(value));
				
			parent.appendChild(xmlNewLineAndTab(ident));
			parent.appendChild(element);
			
			return element;
		}
	}
	
	public PropertiesToXMLParser(String propFileName) throws IOException,Exception{
		super();
		this.propFileName = propFileName;
		this.xmlFileName = propFileName.substring(0,propFileName.lastIndexOf('.'))+".xml";
		

		// creating Doc Builder
		try {
			domBuilder = domFactory.newDocumentBuilder();
			document = domBuilder.newDocument();
		} catch (ParserConfigurationException pce) {
			System.err.println("Cannot create document builder");
		}
		
		createXMLDocument();
		
		
		try {
            // Prepare the DOM document for writing
            Source source = new DOMSource(document);
    
            // Prepare the output file
            File file = new File(xmlFileName);
            StreamResult result = new StreamResult(file);
            //Result result = new DOMResult(document);
    
            // Write the DOM document to the file
            Transformer xformer = TransformerFactory.newInstance().newTransformer();
            xformer.transform(source, result);
            
            
        } catch (TransformerConfigurationException e) {
        } catch (TransformerException e) {
        }
	
				
	}
	
	public Document getDocument(){
		return document;
	}
	public static void main(String[] args) {
    if (args.length == 0){
      System.out.println("Usage: java " + PropertiesToXMLParser.class.getName() + " <path property file>");
      System.exit(1);
    }
		try{
		new PropertiesToXMLParser(args[0]);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public String getXMLFileName(){
		return xmlFileName;
	}
}
