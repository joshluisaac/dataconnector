package com.profitera.dc.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.profitera.dc.InvalidLookupQueryException;
import com.profitera.dc.InvalidRefreshDataQueryException;
import com.profitera.dc.RecordDispenserFactory;
import com.profitera.dc.handler.LongHandler;
import com.profitera.dc.handler.PassThroughHandler;
import com.profitera.dc.parser.exception.InvalidParserConfiguration;
import com.profitera.util.Strings;

/**
 * @deprecated use V2LoadDefinitionParser
 * @author antoni
 * 
 */
public class XMLLoadDefinitionParser extends LoadDefinitionParser implements XMLConstantKey {

	private DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
	private DocumentBuilder domBuilder = null;
	private Document document = null;

	public XMLLoadDefinitionParser() {
		loadTemplate();
	}

	private boolean parseConfigAsXML(File configFile) throws IOException, ParserConfigurationException {
		domBuilder = domFactory.newDocumentBuilder();
		try {
			document = domBuilder.parse(configFile);
		} catch (SAXException saxe) {
			return false;
		}
		return true;
	}

	public void parseDefinition(File configFile) throws InvalidLookupQueryException, InvalidRefreshDataQueryException, Exception {
		log.info("Attempting to parse configuration file " + configFile.getAbsolutePath() + " as xml");
		if (parseConfigAsXML(configFile)) {
			log.info("Configuration file " + configFile.getAbsolutePath() + " successfully parsed as xml");
			xmlParseDefinition();
		} else {
			log.warn("Unable parse configuration file " + configFile.getAbsolutePath() + " as xml, probably it's a properties file.");
			log.info("Parse mode is set to PROPERTIES mode");
			Properties defBuffer = new Properties();
			defBuffer.load(new FileInputStream(configFile));
			super.parseDefinition(defBuffer);
		}

	}

	private String getFirstTextNode(Node node) {
		Node result = hasNode(node, "#text");
		return (result == null) ? null : result.getNodeValue().trim();
	}

	public void xmlParseDefinition() throws InvalidLookupQueryException, InvalidRefreshDataQueryException, Exception {
		Element root = document.getDocumentElement();
		if (!root.getNodeName().equalsIgnoreCase(XML_LOAD_KEY))
			throw new Exception("Should use \"load\" tag");
		Attr attributeNode = root.getAttributeNode(XML_FULL_CACHE_KEY);
		if (attributeNode != null) {
			String value = attributeNode.getValue();
			if (value.equals("true")) {
				isFullCacheEnabled = true;
			} else if (value.equals("false")) {
				isFullCacheEnabled = false;
			} else {
				throw new Exception("Load fullcache attribute assigned invalid value: " + value);
			}
		}

		NodeList rawFields = document.getElementsByTagName(XML_FIELD_KEY);

		int noOfField = 0;
		for (int i = 0; i < rawFields.getLength(); i++)
			if (rawFields.item(i).getParentNode() == root && hasNode(rawFields.item(i), XML_LOCATION_SUFFIX) != null)
				noOfField++;

		// initialization
		fields = new ArrayList();
		keyFields = new ArrayList();
		lookupQueries = new ArrayList();
		lookupTypes = new ArrayList();
		lookupInserts = new ArrayList();
		lookupSelectKeys = new ArrayList();
		lookupFullCacheQueries = new ArrayList();
		defaultValues = new ArrayList();
		lookupQueryParams = new ArrayList();

		isExternal = new boolean[noOfField];
		lookupOptional = new boolean[noOfField];
		lookupCached = new boolean[noOfField];
		lookupMultipleParam = new boolean[noOfField];
		handlerClasses = new Class[noOfField];
		handlerArgs = new String[noOfField];
		filterOns = new String[noOfField];
		filterOnFields = new String[noOfField];
		nullDefinitions = new String[noOfField];
		startIndexes = new String[noOfField];
		endIndexes = new String[noOfField];
		
		generatedKeyField = null;
		generatedKeySeqName = null;
		selectQueryWhere = null;
		updateQueryWhere = null;
		headerFieldText = new ArrayList();
		trailerFieldText = new ArrayList();
		postInsertInsertion = null;
		postInsertUpdate = null;
		postUpdateInsertion = null;
		postUpdateUpdate = null;
		
		Arrays.fill(lookupCached, true);

		NamedNodeMap rootAttr = root.getAttributes();

		if (rootAttr.getNamedItem(XML_TYPE_KEY) != null)
			loadType = rootAttr.getNamedItem(XML_TYPE_KEY).getNodeValue();

		if (loadType.equalsIgnoreCase(RecordDispenserFactory.DELIMITED)) {
			if (rootAttr.getNamedItem(XML_DELIMITER_KEY) != null) {
				delimiter = rootAttr.getNamedItem(XML_DELIMITER_KEY).getNodeValue();
				if (delimiter == null || delimiter.length() == 0)
					throw new RuntimeException("Missing delimiter information for delimited load file");
			} else
				throw new RuntimeException("Missing delimiter information for delimited load file");
		}

		if (rootAttr.getNamedItem(XML_PAD_LINE_KEY) != null)
			padLine = rootAttr.getNamedItem(XML_PAD_LINE_KEY).getNodeValue().trim().toUpperCase().startsWith("T");

		int fIdx = 0;
		Node elementNode;
		NodeList fieldList = document.getElementsByTagName(XML_FIELD_KEY);
		for (int fieldIdx = 0; fieldIdx < fieldList.getLength(); fieldIdx++) {
			Node fieldNode = fieldList.item(fieldIdx);
			if (fieldNode.getParentNode() != root)
				continue;

			elementNode = hasNode(fieldNode, XML_LOCATION_SUFFIX);
			if (elementNode != null) {
				NamedNodeMap locationAttr = elementNode.getAttributes();

				Node locStart = locationAttr.getNamedItem(XML_LOCATION_START);
				Node locFinish = locationAttr.getNamedItem(XML_LOCATION_END);

				if (locStart == null || locFinish == null) {
					throw new RuntimeException("Please state the Location of " + elementNode.getNodeName() + "!");
				}
				startIndexes[fIdx] = locStart.getNodeValue();
				endIndexes[fIdx] = locFinish.getNodeValue();
			} else
				continue;

			elementNode = hasNode(fieldNode, XML_FIELD_NAME);
			if (elementNode != null) {
				fields.add(getFirstTextNode(elementNode));
			} else
				throw new RuntimeException("Cannot find name element tag");

			NamedNodeMap fieldAttr = fieldNode.getAttributes();

			if (fieldAttr.getNamedItem(XML_IS_KEY_SUFFIX) != null)
				if (fieldAttr.getNamedItem(XML_IS_KEY_SUFFIX).getNodeValue().trim().toUpperCase().startsWith("T")) {
					keyFields.add(fields.get(fIdx));
				}

			if (fieldAttr.getNamedItem(XML_IS_OPTIONAL_SUFFIX) != null)
				lookupOptional[fIdx] = fieldAttr.getNamedItem(XML_IS_OPTIONAL_SUFFIX).getNodeValue().trim().toUpperCase().startsWith("T");
			if (fieldAttr.getNamedItem(XML_IS_EXTERNAL_SUFFIX) != null)
				isExternal[fIdx] = fieldAttr.getNamedItem(XML_IS_EXTERNAL_SUFFIX).getNodeValue().trim().toUpperCase().startsWith("T");

			elementNode = hasNode(fieldNode, XML_CACHE);
			if (elementNode != null)
				lookupCached[fIdx] = getFirstTextNode(elementNode).equals("") || getFirstTextNode(elementNode).equalsIgnoreCase("true");

			elementNode = hasNode(fieldNode, XML_DEFAULT_SUFFIX);
			if (elementNode != null)
				defaultValues.add(getFirstTextNode(elementNode));
			else
				defaultValues.add(null);

			elementNode = hasNode(fieldNode, XML_FILTER_ON_SUFFIX);
			if (elementNode != null) {
				NodeList filterOnList = ((Element) elementNode).getElementsByTagName(XML_VALUE_KEY);
				if (filterOnList.getLength() > 0) {
					filterOns[fIdx] = getFirstTextNode(filterOnList.item(0));
					for (int filterOnIdx = 1; filterOnIdx < filterOnList.getLength(); filterOnIdx++) {
						filterOns[fIdx] += ";" + getFirstTextNode(filterOnList.item(filterOnIdx));
					}
				}
				NodeList filterOnFieldList = ((Element) elementNode).getElementsByTagName(XML_FIELD_KEY);
				if (filterOnFieldList.getLength() > 0) {
					filterOnFields[fIdx] = getFirstTextNode(filterOnFieldList.item(0));
					for (int filterOnIdx = 1; filterOnIdx < filterOnFieldList.getLength(); filterOnIdx++) {
						filterOnFields[fIdx] += ";" + getFirstTextNode(filterOnFieldList.item(filterOnIdx));
					}
				}
			}

			elementNode = hasNode(fieldNode, XML_NULL_DEFINITION_SUFFIX);
			if (elementNode != null) {
				NodeList nullDefList = ((Element) elementNode).getElementsByTagName(XML_VALUE_KEY);
				if (nullDefList.getLength() > 0) {
					nullDefinitions[fIdx] = getFirstTextNode(nullDefList.item(0));
					for (int nullDefIdx = 1; nullDefIdx < nullDefList.getLength(); nullDefIdx++) {
						nullDefinitions[fIdx] += ";" + getFirstTextNode(nullDefList.item(nullDefIdx));
					}
				}
			}

			elementNode = hasNode(fieldNode, XML_LOOKUP_KEY);
			if (elementNode != null) {
				NamedNodeMap lAttr = elementNode.getAttributes();
				Node lType = lAttr.getNamedItem(XML_TYPE_KEY);
				if (lType != null)
					lookupTypes.add(getLookupDataType(lType.getNodeValue().trim()));
				else
					lookupTypes.add(getLookupDataType(null));

				Node lookupNode = hasNode(elementNode, XML_QUERY_KEY);
				String lookupQueryV = lookupNode == null ? null : getFirstTextNode(lookupNode);
				lookupQueries.add(lookupQueryV);

				List lookupQueryParam = extractFieldsFromLookupQuery(lookupQueryV);
				lookupMultipleParam[fIdx] = (lookupQueryParam != null && lookupQueryParam.size() > 1);
				if (isReferencialLookupQuery(lookupQueryParam)
						&& !isLookupQueryParameterValid(lookupQueryParams, (String) fields.get(fIdx), lookupQueryParam))
					throw new InvalidLookupQueryException(lookupQueryV, "Lookup query parameter references are circular.");
				lookupQueryParams.add(lookupQueryParam);

				lookupNode = hasNode(elementNode, XML_INSERT_KEY);
				if (lookupNode != null)
					lookupInserts.add(getFirstTextNode(lookupNode));
				else
					lookupInserts.add(null);

				lookupNode = hasNode(elementNode, XML_INSERT_KEY_KEY);
				if (lookupNode != null) {
					lookupSelectKeys.add(getFirstTextNode(lookupNode));
				} else {
					lookupSelectKeys.add(null);
				}
				lookupNode = hasNode(elementNode, XML_FULL_CACHE_KEY);
				if (lookupNode != null) {
					lookupFullCacheQueries.add(getFirstTextNode(lookupNode));
				} else {
					lookupFullCacheQueries.add(null);
				}
			} else {
				lookupTypes.add(getLookupDataType(null));
				lookupQueries.add(null);
				lookupQueryParams.add(new ArrayList());
				lookupInserts.add(null);
				lookupSelectKeys.add(null);
				lookupFullCacheQueries.add(null);
			}

			elementNode = hasNode(fieldNode, XML_HANDLER_SUFFIX);
			if (elementNode != null) {
				String handlerValue = getFirstTextNode(elementNode);

				handlerClasses[fIdx] = getHandlerClass(handlerValue);

				if (handlerClasses[fIdx].equals(PassThroughHandler.class) && lookupQueries.get(fIdx) != null)
					handlerClasses[fIdx] = LongHandler.class;

				handlerArgs[fIdx] = ((Element) elementNode).getAttribute(XML_HANDLER_ARGS_SUFFIX);
				if (handlerArgs[fIdx] == null || handlerArgs[fIdx].length() == 0) {
					Node handlerChild = hasNode(elementNode, XML_HANDLER_ARGS_SUFFIX);
					if (handlerChild != null)
						handlerArgs[fIdx] = getFirstTextNode(handlerChild);
				}
			} else {
				handlerClasses[fIdx] = getHandlerClass(null);
				if (lookupQueries.get(fIdx) != null)
					handlerClasses[fIdx] = LongHandler.class;
			}

			fIdx++;
		}

		Node loadElement = hasNode(root, XML_TABLE_KEY);
		if (loadElement != null)
			destTable = getFirstTextNode(loadElement);
		loadElement = hasNode(root, XML_QUERY_WHERE_KEY);
		if (loadElement != null) {
			Node qwNode = hasNode(loadElement, XML_SELECT_KEY);
			if (qwNode != null)
				selectQueryWhere = getFirstTextNode(qwNode);

			qwNode = hasNode(loadElement, XML_UPDATE_KEY);
			if (qwNode != null)
				updateQueryWhere = qwNode.getChildNodes().item(0).getNodeValue().trim();

		}

		loadElement = hasNode(root, XML_POST_KEY);
		if (loadElement != null) {
			Node postNode = hasNode(loadElement, XML_INSERT_UPDATE_KEY);
			if (postNode != null)
				postInsertUpdate = getFirstTextNode(postNode);

			postNode = hasNode(loadElement, XML_INSERT_INSERTION_KEY);
			if (postNode != null)
				postInsertInsertion = getFirstTextNode(postNode);

			postNode = hasNode(loadElement, XML_UPDATE_UPDATE_KEY);
			if (postNode != null)
				postUpdateUpdate = getFirstTextNode(postNode);

			postNode = hasNode(loadElement, XML_UPDATE_INSERTION_KEY);
			if (postNode != null)
				postUpdateInsertion = getFirstTextNode(postNode);
		}

		loadElement = hasNode(root, XML_GENERATED_KEY_KEY);
		if (loadElement != null)
			generatedKeyField = getFirstTextNode(loadElement);

		loadElement = hasNode(root, XML_GENERATED_KEY_SEQ_NAME_KEY);
		if (loadElement != null)
			generatedKeySeqName = getFirstTextNode(loadElement);

		loadElement = hasNode(root, XML_HEADER_KEY);
		if (loadElement != null) {
			NodeList childList = ((Element) loadElement).getElementsByTagName(XML_VALUE_KEY);
			for (int fChildListIdx = 0; fChildListIdx < childList.getLength(); fChildListIdx++) {
				Node nodeValue = childList.item(fChildListIdx);
				headerFieldText.add(getFirstTextNode(nodeValue));
			}
		}

		loadElement = hasNode(root, XML_TRAILER_KEY);
		if (loadElement != null) {
			NodeList childList = ((Element) loadElement).getElementsByTagName(XML_VALUE_KEY);
			for (int fChildListIdx = 0; fChildListIdx < childList.getLength(); fChildListIdx++) {
				Node nodeValue = childList.item(fChildListIdx);
				trailerFieldText.add(getFirstTextNode(nodeValue));
			}
		}

		loadElement = hasNode(root, XML_REFRESH_DATA_KEY);
		if (loadElement != null) {
			Attr refrKey = (Attr) loadElement.getAttributes().getNamedItem(XML_KEY);
			if (refrKey != null)
				refreshData = refrKey.getNodeValue().trim().toUpperCase().startsWith("T");

			Attr refrTimeout = (Attr) loadElement.getAttributes().getNamedItem(XML_REFRESH_DATA_QUERY_TIMEOUT);
			if (refrTimeout != null) {
				try {
					refreshDataQueryTimeout = Long.parseLong(refrTimeout.getNodeValue().trim());
				} catch (NumberFormatException nfe) {
					throw new InvalidParserConfiguration(XML_REFRESH_DATA_QUERY_TIMEOUT, refrTimeout.getNodeValue().trim(), nfe);
				}
				if (refreshDataQueryTimeout < 0)
					throw new InvalidParserConfiguration(XML_REFRESH_DATA_QUERY_TIMEOUT, "" + refreshDataQueryTimeout);
			}

			refreshDataQuery = getFirstTextNode(loadElement);

			if (refreshData && refreshDataQuery != null && refreshDataQuery.length() != 0)
				if (refreshDataQuery.toLowerCase().startsWith("update"))
					refreshDataQueryType = REFRESH_DATA_UPDATE;
				else if (refreshDataQuery.toLowerCase().startsWith("delete"))
					refreshDataQueryType = REFRESH_DATA_DELETE;
				else
					throw new InvalidRefreshDataQueryException(refreshDataQuery);

		}

		if (getLoadType().equalsIgnoreCase(RecordDispenserFactory.XML)) {
			loadElement = hasNode(root, XML_XML_START_TAG);
			if (loadElement != null) {
				xmlStartTag = getFirstTextNode(loadElement);
				if (xmlStartTag == null)
					throw new Exception("Missing record start tag for xml load file");
			}

			loadElement = hasNode(root, XML_XQUERY);
			if (loadElement != null)
				xQuery = getFirstTextNode(loadElement);

		}

		for (int i = 0; i < fIdx; i++) {
			if (getLoadType().equalsIgnoreCase(RecordDispenserFactory.FIXED_WIDTH) || getLoadType().equalsIgnoreCase(RecordDispenserFactory.DELIMITED))
				recordSize = (recordSize < new Integer(endIndexes[i]).intValue() ? new Integer(endIndexes[i]).intValue() : recordSize);
		}
	}

	private Node hasNode(Node list, String keyword) {
		// NodeList result = ((Element)list).getElementsByTagName(keyword);
		NodeList result = list.getChildNodes();
		for (int i = 0; i < result.getLength(); i++)
			if (result.item(i).getNodeName().equals(keyword)) {
				return result.item(i);

			}
		return null;
	}

	public String[] getLoadKeyFields() {
		return (String[]) keyFields.toArray(new String[0]);
	}

	public String getUpdateMode() {
		if (document == null) {
			return super.getUpdateMode();
		}
		Element root = document.getDocumentElement();
		Attr modeNode = root.getAttributeNode(XML_MODE_KEY);
		if (modeNode == null) {
			return MIXED_MODE;
		} else {
			String value = modeNode.getNodeValue();
			if (value.equals(MIXED_MODE)) {
				return MIXED_MODE;
			} else if (value.equals(UPDATE_MODE)) {
				return UPDATE_MODE;
			} else if (value.equals(INSERT_MODE)) {
				// If we are insert mode then we should never be fullcache, it makes no
				// sense
				if (isFullCacheEnabled()) {
					throw new IllegalArgumentException("Illegal combination of mode as '" + value + "' and full cache enabled for load");
				}
				return INSERT_MODE;
			} else {
				throw new IllegalArgumentException("Illegal value for '" + XML_MODE_KEY + "' attribute '" + value + "' expected one of "
						+ Strings.getListString(new String[] { MIXED_MODE, UPDATE_MODE, INSERT_MODE }, ", ") + " if specifed");
			}
		}
	}
}
