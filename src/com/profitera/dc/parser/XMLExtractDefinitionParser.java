package com.profitera.dc.parser;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.profitera.dc.InvalidLookupQueryException;
import com.profitera.dc.InvalidRefreshDataQueryException;
import com.profitera.dc.RecordDispenserFactory;
import com.profitera.util.Utilities;
import com.profitera.util.xml.DOMDocumentUtil;

public class XMLExtractDefinitionParser extends ExtractDefinitionParser implements XMLConstantKey{
	
	private DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
	private DocumentBuilder domBuilder = null;
	private Document document = null;
	
	private static final String XML_EXTRACT_KEY = "extract";
	private static final String XML_CONTROL_FILE_KEY = "controlfile";
	private static final String XML_PREFIX_KEY = "prefix";
	private static final String XML_SUFFIX_KEY = "suffix";
	private static final String XML_OUT_TYPE_KEY = "outputtype";
	private static final String XML_XML_ROOT_TAG = "roottag";
	
	
	public XMLExtractDefinitionParser(){
	}
	
	public void parseDefinition(File configFile) throws InvalidLookupQueryException, InvalidRefreshDataQueryException, Exception{
		if(configFile.getName().toLowerCase().endsWith(".xml")){
			//config file is xml format
			domBuilder = domFactory.newDocumentBuilder();
			try {
				document = domBuilder.parse(configFile);
			}catch (SAXException saxe) {
				System.exit(1);
			}
			loadTemplate();
			xmlParseDefinition();
		}else if(configFile.getName().toLowerCase().endsWith(".properties")){
			//config file is properties format
			Properties defBuffer = Utilities.loadFromPath(configFile.getPath());	
			super.parseDefinition(defBuffer);
		}else{
			System.err.println("Fail to read load configuration file "+configFile+"\n"+"configuration file suffix invalid or not defined");
			System.exit(1);
		}
	}
	
	private String getFirstTextNode(Node node){
		Node result = hasNode(node,"#text");
		return (result == null) ? null : result.getNodeValue().trim();
	}
	
	public Node hasNode(Node list, String keyword){
		//NodeList result = ((Element)list).getElementsByTagName(keyword);
		NodeList result = list.getChildNodes();
		for (int i = 0; i < result.getLength(); i++)
			if (result.item(i).getNodeName().equals(keyword)){
				return result.item(i);
				
			}
		return null;
	}
	
	
	public void xmlParseDefinition() throws InvalidLookupQueryException, InvalidRefreshDataQueryException, Exception{
				
		//detect the root tag
		Node root = document.getFirstChild(); 
		if (!root.getNodeName().equalsIgnoreCase(XML_EXTRACT_KEY)) throw new Exception("Should use \"extract\" tag");
		NodeList rawFields = document.getElementsByTagName(XML_FIELD_KEY);
		
		//detect number of field in the root
		int noOfField = 0;
		for(int i =0; i < rawFields.getLength(); i++)
			if (rawFields.item(i).getParentNode() == root && hasNode(rawFields.item(i),XML_LOCATION_SUFFIX) != null)
				noOfField++;
		
		
		//declare node to store the element
		Node extractElement;
		//declare attribute
		NamedNodeMap attr;
		
		
		//root <extract> attribute outputType
		attr=root.getAttributes();
		if(attr.getNamedItem(XML_OUT_TYPE_KEY)!=null){
			outputType=attr.getNamedItem(XML_OUT_TYPE_KEY).getNodeValue().trim();
		}
		if(outputType==null||outputType.length()==0){
			outputType=RecordDispenserFactory.FIXED_WIDTH;
		}
	    if(isDelimitedFormat()){                    
	    	if(attr.getNamedItem(XML_DELIMITER_KEY)!=null){
	    		delimiter=attr.getNamedItem(XML_DELIMITER_KEY).getNodeValue();
	    		if(delimiter == null || delimiter.length() == 0 ){
	    			throw new RuntimeException("Missing delimiter information for delimited output file");
	    		}
	    	}
	    }else if(isXMLFormat()){                 
	    	xmlRootTag = attr.getNamedItem(XML_XML_ROOT_TAG).getNodeValue();
	    	if (xmlRootTag == null || xmlRootTag.length() == 0)
	    		throw new RuntimeException("Missing root tag for xml output file");
	    	xmlStartTag = attr.getNamedItem(XML_XML_START_TAG).getNodeValue();
	    	if (xmlStartTag == null || xmlStartTag.length() == 0)
	    		throw new RuntimeException("Missing record start tag for xml output file");
	    }
		
		//element select query where
		extractElement=hasNode(root,XML_QUERY_WHERE_KEY);
		if (extractElement!=null){
			Node qwNode = hasNode(extractElement,XML_SELECT_KEY);
			if(qwNode != null){
				selectQueryWhere = getFirstTextNode(qwNode);
			}else{
				throw new Exception("Query is not complete : <select> needed");
			}			
		}else{
			throw new Exception("Select Query missing");
		}
		
		//control file with attribute prefix, suffix
		extractElement=hasNode(root,XML_CONTROL_FILE_KEY);
		if(extractElement!=null){
			attr = extractElement.getAttributes();
			if(attr.getNamedItem(XML_PREFIX_KEY)!=null){
				controlFilePrefix = attr.getNamedItem(XML_PREFIX_KEY).getNodeValue().trim();
			}else{
				controlFilePrefix = "";
			}
				
			if(attr.getNamedItem(XML_SUFFIX_KEY)!=null){
				controlFileSuffix = attr.getNamedItem(XML_SUFFIX_KEY).getNodeValue().trim();
			}else{
				controlFileSuffix = "";
			}
		}else{
			controlFileSuffix="";
			controlFilePrefix="";
		}
		// Output header and footer text
		Node outputHeaderNode=hasNode(root, OUTPUT_HEADER);
		Node outputFooterNode=hasNode(root, OUTPUT_FOOTER);
		if (outputHeaderNode!=null){
			 outputHeader = DOMDocumentUtil.getNodeChildText((Element) outputHeaderNode);		
		}
		if (outputFooterNode!=null){
			 outputFooter = DOMDocumentUtil.getNodeChildText((Element) outputFooterNode);		
		}
		
		//initialization
		fields = new ArrayList();
	    lookupQueries = new ArrayList();
	    lookupTypes = new ArrayList();
	    defaultValues = new ArrayList();
	    lookupQueryParams = new ArrayList();
	    startIndexes = new String[noOfField];
	    endIndexes = new String[noOfField];
	    isExternal = new boolean[noOfField];
	    lookupOptional = new boolean[noOfField];
	    lookupMultipleParam = new boolean[noOfField];
	    handlerClasses = new Class[noOfField];
	    handlerArgs = new String[noOfField];
	    filterOns = new String[noOfField];
	    nullDefinitions = new String[noOfField];
	    
	    
	    int fIdx = 0;
	    NodeList fieldList = document.getElementsByTagName(XML_FIELD_KEY);
	    for(int fieldIdx=0;fieldIdx<fieldList.getLength();fieldIdx++ ){
	    	Node fieldNode = fieldList.item(fieldIdx);
	    	if (fieldNode.getParentNode() != root) continue;
	    	
	    	//element <field> attribute isOptional, isExternal
			attr=fieldNode.getAttributes();
			if (attr.getNamedItem(XML_IS_OPTIONAL_SUFFIX) != null )
				lookupOptional[fIdx] = attr.getNamedItem(XML_IS_OPTIONAL_SUFFIX).getNodeValue().trim().toUpperCase().startsWith("T");
			if (attr.getNamedItem(XML_IS_EXTERNAL_SUFFIX)  != null)
				isExternal[fIdx] = attr.getNamedItem(XML_IS_EXTERNAL_SUFFIX).getNodeValue().trim().toUpperCase().startsWith("T");
				    	
	    	//name of the field
	    	extractElement = hasNode(fieldNode,XML_FIELD_NAME);
	    	String fieldName = null;
			if (extractElement != null){
			  fieldName = getFirstTextNode(extractElement);
				fields.add(fieldName);
			}
			else throw new RuntimeException("Cannot find name element tag");
	    	
	    	//location with attribute start, end
	    	extractElement = hasNode(fieldNode,XML_LOCATION_SUFFIX);
			if (extractElement != null){
				NamedNodeMap locationAttr = extractElement.getAttributes();
				
				Node locStart = locationAttr.getNamedItem(XML_LOCATION_START);
				Node locFinish = locationAttr.getNamedItem(XML_LOCATION_END);
				
				if(locStart == null || locFinish == null)
					throw new RuntimeException("Please state the Location of "+extractElement.getNodeName()+"!");
				startIndexes[fIdx] = locStart.getNodeValue();
				endIndexes[fIdx] = locFinish.getNodeValue();
			}
	    	
			//lookupQuery with attribute lookupType
			extractElement = hasNode(fieldNode,XML_LOOKUP_KEY);
			if (extractElement != null){
				attr = extractElement.getAttributes();
				if (attr.getNamedItem(XML_TYPE_KEY) != null){
					lookupTypes.add(getLookupDataType(attr.getNamedItem(XML_TYPE_KEY).getNodeValue().trim()));
				}else{
					lookupTypes.add(getLookupDataType(null));
				}
				
				Node lookupQ = hasNode(extractElement,XML_QUERY_KEY);
				if(lookupQ!=null){
					String lookupQueryV = lookupQ == null? null : getFirstTextNode(lookupQ);
					lookupQueries.add(lookupQueryV);
					List lookupQueryParam = extractFieldsFromLookupQuery(lookupQueryV);
					lookupQueryParams.add(lookupQueryParam);
					lookupMultipleParam[fIdx] = (lookupQueryParam != null && lookupQueryParam.size() > 1);
				}else{
					throw new Exception("lookupQuery not complete : <query> needed for " + fieldName);
				}				
			}else{
				lookupTypes.add(getLookupDataType(null));
				lookupQueries.add(null);
				lookupQueryParams.add(null);
			}
			
			//default
			extractElement = hasNode(fieldNode,XML_DEFAULT_SUFFIX);
			if(extractElement != null){
				defaultValues.add((String)getFirstTextNode(extractElement));
			}else{
				defaultValues.add(null);
			}
			
			//filter
			extractElement = hasNode(fieldNode,XML_FILTER_ON_SUFFIX);
			if(extractElement != null ){
				filterOns[fIdx]= getFirstTextNode(extractElement);
			}
			
			//nullDefinition
			extractElement = hasNode(fieldNode,XML_NULL_DEFINITION_SUFFIX);
			if(extractElement!=null){
				nullDefinitions[fIdx] = getFirstTextNode(extractElement); 
			}
			
			//handler with attribute handlerconfig
			extractElement = hasNode(fieldNode,XML_HANDLER_SUFFIX);
			if(extractElement != null ){
				handlerClasses[fIdx] = getHandlerClass(getFirstTextNode(extractElement));
				
				attr = extractElement.getAttributes();
				if(attr.getNamedItem(XML_HANDLER_ARGS_SUFFIX)!=null){
					handlerArgs[fIdx] = attr.getNamedItem(XML_HANDLER_ARGS_SUFFIX).getNodeValue();
				}
			}else{
				handlerClasses[fIdx] = getHandlerClass(null);
			}
			
			fIdx++;
	    }
	    
	    for (int i = 0; i < fIdx; i++) {
	    	if (isFixedWidthFormat())
	          	recordSize = (recordSize < new Integer(endIndexes[i]).intValue() ? new Integer(endIndexes[i]).intValue() : recordSize);
		}
	    
	}
	
}