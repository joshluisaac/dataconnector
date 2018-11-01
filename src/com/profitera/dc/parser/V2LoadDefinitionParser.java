package com.profitera.dc.parser;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.profitera.dc.RecordDispenserFactory;
import com.profitera.dc.impl.DataLoaderLogClient;
import com.profitera.dc.parser.exception.InvalidConfigurationException;
import com.profitera.dc.parser.exception.InvalidParserConfiguration;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.GlobalFilter;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.dc.parser.impl.Location;
import com.profitera.dc.parser.impl.LookupDefinition;
import com.profitera.log.DefaultLogProvider;
import com.profitera.log.ILogProvider;
import com.profitera.util.Strings;
import com.profitera.util.io.FileUtil;
import com.profitera.util.xml.DOMDocumentUtil;
import com.profitera.util.xml.DocumentLoader;

public class V2LoadDefinitionParser implements XMLConstantKey, PropertiesConstantKey {
  private static ILogProvider log;
  public static LoadDefinition parse(File path) {
    Document doc = DocumentLoader.loadDocument(path);
    return parse(doc, FileUtil.tryCanonical(path));
  }
  /**
   * The returned LoadDefintion is not yet validated. Validate it with
   * com.profitera.dc.parser.LoadDefinitionValidator
   * 
   * @param doc
   *          XML document contains parsing configuration
   * @return LoadDefinition
   * @see com.profitera.dc.parser.LoadDefinitionValidator
   */
  public static LoadDefinition parse(Document doc, String path) {
    LoadDefinition definition = new LoadDefinition();
    Element root = doc.getDocumentElement();
    Element documentation = DOMDocumentUtil.getFirstChildElementWithName(XML_DOC, root);
    if (documentation != null) {
      definition.setDocumentation(DOMDocumentUtil.getNodeChildText(documentation));
    }
    // load type
    if (root.hasAttribute(XML_TYPE_KEY)) {
      definition.setLoadType(root.getAttribute(XML_TYPE_KEY));
    }
    // load mode
    if (root.hasAttribute(XML_MODE_KEY)) {
      definition.setUpdateMode(root.getAttribute(XML_MODE_KEY));
    }
    // pad line
    if (root.hasAttribute(XML_PAD_LINE_KEY)) {
      definition.setPadLine(Boolean.parseBoolean(root.getAttribute(XML_PAD_LINE_KEY)));
    }
    if (root.hasAttribute(XML_SPREADSHEET)) {
      definition.setSelectedSheet(root.getAttribute(XML_SPREADSHEET));
    }
    // full cache loading
    if (root.hasAttribute(XML_FULL_CACHE_KEY)) {
      definition.setFullCache(Boolean.parseBoolean(root.getAttribute(XML_FULL_CACHE_KEY)));
    }
    if (root.hasAttribute(XML_VERIFY_UNIQUENESS_KEY)) {
      definition.setKeyVerificationScan(Boolean.parseBoolean(root.getAttribute(XML_VERIFY_UNIQUENESS_KEY)));
    }
    
    // delimiter used for load type 'delimited'
    String delimiterLiteral = Strings.nullifyIfBlank(root.getAttribute(XML_DELIMITER_KEY));
    String delimiterCharacterIndex = Strings.nullifyIfBlank(root.getAttribute(XML_ASCII_DELIMITER_KEY));
    if (delimiterLiteral != null || delimiterCharacterIndex != null) {
      if (delimiterLiteral != null && delimiterCharacterIndex != null) {
        getLog().emit(DataLoaderLogClient.CONFLICTING_DELIMITERS, path);
      }
      if (delimiterLiteral == null) {
        int v = Integer.parseInt(delimiterCharacterIndex);
        definition.setAsciiCodepointDelimiter(v);
      } else {
        definition.setDelimiter(delimiterLiteral);
      }
    }
    // XQuery used for load type 'xml'
    Element xQuery = DOMDocumentUtil.getFirstChildElementWithName(XML_XQUERY, root);
    if (xQuery != null) {
      definition.setXQuery(DOMDocumentUtil.getNodeChildText(xQuery));
    }
    // record start indicator for load type 'xml'
    Element xmlStartTag = DOMDocumentUtil.getFirstChildElementWithName(XML_XML_START_TAG, root);
    if (xmlStartTag != null) {
      definition.setXmlStartTag(DOMDocumentUtil.getNodeChildText(xmlStartTag));
    }
    // destination table
    Element table = DOMDocumentUtil.getFirstChildElementWithName(XML_TABLE_KEY, root);
    if (table != null) {
      definition.setDestTable(DOMDocumentUtil.getNodeChildText(table));
    }
    // auto incremental column
    Element genKey = DOMDocumentUtil.getFirstChildElementWithName(XML_GENERATED_KEY_KEY, root);
    if (genKey != null) {
      String keyColumn = DOMDocumentUtil.getNodeChildText(genKey);
      if (!keyColumn.trim().equals("")) {
        definition.setGenerateKey(true);
        definition.setGenerateKeyColumn(keyColumn);
        // sequence name
        Element genSeq = DOMDocumentUtil.getFirstChildElementWithName(XML_GENERATED_KEY_SEQ_NAME_KEY, root);
        if (genSeq != null) {
          definition.setGenerateKeySeq(DOMDocumentUtil.getNodeChildText(genSeq));
        }
      }
    }
    // header
    Element header = DOMDocumentUtil.getFirstChildElementWithName(XML_HEADER_KEY, root);
    if (header != null) {
      Element[] value = DOMDocumentUtil.getChildElementsWithName(XML_VALUE_KEY, header);
      List<String> l = new ArrayList<String>();
      for (int i = 0; i < value.length; i++) {
        String headerValue = DOMDocumentUtil.getNodeChildText(value[i]);
        if (Strings.nullifyIfBlank(headerValue) != null) {
          l.add(headerValue.trim());
        }
      }
      definition.setHeader(l.toArray(new String[0]));
    }
    // trailer
    Element trailer = DOMDocumentUtil.getFirstChildElementWithName(XML_TRAILER_KEY, root);
    if (trailer != null) {
      Element[] value = DOMDocumentUtil.getChildElementsWithName(XML_VALUE_KEY, trailer);
      List<String> l = new ArrayList<String>();
      for (int i = 0; i < value.length; i++) {
        String trailerValue = DOMDocumentUtil.getNodeChildText(value[i]);
        if (Strings.nullifyIfBlank(trailerValue) != null) {
          l.add(trailerValue.trim());
        }
      }
      definition.setTrailer(l.toArray(new String[0]));
    }
    // where clause
    Element whereClause = DOMDocumentUtil.getFirstChildElementWithName(XML_QUERY_WHERE_KEY, root);
    if (whereClause != null) {
      Element select = DOMDocumentUtil.getFirstChildElementWithName(XML_SELECT_KEY, whereClause);
      if (select != null) {
        definition.setSelectWhereClause(DOMDocumentUtil.getNodeChildText(select));
      }
      Element update = DOMDocumentUtil.getFirstChildElementWithName(XML_UPDATE_KEY, whereClause);
      if (update != null) {
        definition.setUpdateWhereClause(DOMDocumentUtil.getNodeChildText(update));
      }
    }
    // pre loading
    Element refreshData = DOMDocumentUtil.getFirstChildElementWithName(XML_REFRESH_DATA_KEY, root);
    if (refreshData != null) {
      if (refreshData.hasAttribute(XML_KEY)) {
        definition.setRefreshData(Boolean.parseBoolean(refreshData.getAttribute(XML_KEY)));
      }
      if (refreshData.hasAttribute(XML_REFRESH_DATA_QUERY_TIMEOUT)) {
        definition.setRefreshTimeout(Long.parseLong(refreshData.getAttribute(XML_REFRESH_DATA_QUERY_TIMEOUT)));
      }
      definition.setRefreshDataQuery(DOMDocumentUtil.getNodeChildText(refreshData));
    }
    // post loading
    Element post = DOMDocumentUtil.getFirstChildElementWithName(XML_POST_KEY, root);
    if (post != null) {
      {
        Element parent = DOMDocumentUtil.getFirstChildElementWithName(XML_INSERT_INSERTION_KEY, post);
        if (parent != null) {
          String[] statements = getPostStatements(parent);
          definition.setPostInsertInsertQueries(statements);
        }
      }
      {
        Element parent = DOMDocumentUtil.getFirstChildElementWithName(XML_INSERT_UPDATE_KEY, post);
        if (parent != null) {
          String[] statements = getPostStatements(parent);
          definition.setPostInsertUpdateQueries(statements);
        }
      }
      {
        Element parent = DOMDocumentUtil.getFirstChildElementWithName(XML_UPDATE_INSERTION_KEY, post);
        if (parent != null) {
          String[] statements = getPostStatements(parent);
          definition.setPostUpdateInsertQueries(statements);
        }
      }
      {
        Element parent = DOMDocumentUtil.getFirstChildElementWithName(XML_UPDATE_UPDATE_KEY, post);
        if (parent != null) {
          String[] statements = getPostStatements(parent);
          definition.setPostUpdateUpdateQueries(statements);
        }
      }
    }

    // loop over field configuration
    Element[] fields = DOMDocumentUtil.getChildElementsWithName(XML_FIELD_KEY, root);
    List<FieldDefinition> allFieldDefinition = parseLoadFields(fields);
    definition.setFields(allFieldDefinition);
    GlobalFilter[] filters = getGlobalFilters(root);
    definition.setGlobalFilters(filters);
    return definition;
  }

  private static String[] getPostStatements(Element postDirectChild) {
    Element[] statements = DOMDocumentUtil.getChildElementsWithName("statement", postDirectChild);
    if (statements.length == 0) {
      String text = DOMDocumentUtil.getNodeChildText(postDirectChild);
      if (Strings.nullifyIfBlank(text) == null) {
        return new String[0];
      } else {
        return new String[]{text};
      }
    } else {
      String[] statementText = new String[statements.length];
      for (int i = 0; i < statementText.length; i++) {
        statementText[i] = DOMDocumentUtil.getNodeChildText(statements[i]);
      }
      return statementText;
    }
  }

  private static List<FieldDefinition> parseLoadFields(Element[] fields) {
    List<FieldDefinition> allFieldDefinition = new ArrayList<FieldDefinition>();
    for (int i = 0; i < fields.length; i++) {
      Element field = fields[i];
      // fieldName
      String fieldName = null;
      Element nameElement = DOMDocumentUtil.getFirstChildElementWithName(XML_FIELD_NAME, field);
      if (nameElement != null) {
        fieldName = DOMDocumentUtil.getNodeChildText(nameElement);
      }
      // location
      Location loc = null;
      Element locationElement = DOMDocumentUtil.getFirstChildElementWithName(XML_LOCATION_SUFFIX, field);
      if (locationElement != null) {
        loc = new Location(locationElement.getAttribute(XML_LOCATION_START),
            locationElement.getAttribute(XML_LOCATION_END));
      }
      boolean isKey = getBooleanAttribute(XML_IS_KEY_SUFFIX, field, false);
      // initialize field definition
      FieldDefinition fieldDefinition = new FieldDefinition(fieldName, loc, isKey);
      fieldDefinition.setOptional(getBooleanAttribute(XML_IS_OPTIONAL_SUFFIX, field, true));
      // default value
      Element defaultValueElement = DOMDocumentUtil.getFirstChildElementWithName(XML_DEFAULT_SUFFIX, field);
      if (defaultValueElement != null) {
        fieldDefinition.setDefaultValue(DOMDocumentUtil.getNodeChildText(defaultValueElement));
      }
      // filter
      Element filterElement = DOMDocumentUtil.getFirstChildElementWithName(XML_FILTER_ON_SUFFIX, field);
      if (filterElement != null) {
        // on value
        Element[] filterValue = DOMDocumentUtil.getChildElementsWithName(XML_VALUE_KEY, filterElement);
        List<String> valueList = new ArrayList<String>();
        for (int k = 0; k < filterValue.length; k++) {
          String valueText = DOMDocumentUtil.getNodeChildText(filterValue[k]).trim();
          if (Strings.nullifyIfBlank(valueText) != null) {
            valueList.add(valueText);
          }
        }
        fieldDefinition.setFilterOnValue(valueList.toArray(new String[0]));
        // on field
        Element[] filterField = DOMDocumentUtil.getChildElementsWithName(XML_FIELD_KEY, filterElement);
        List<String> fieldList = new ArrayList<String>();
        for (int k = 0; k < filterField.length; k++) {
          String fieldText = DOMDocumentUtil.getNodeChildText(filterField[k]);
          if (Strings.nullifyIfBlank(fieldText) != null) {
            fieldList.add(fieldText);
          }
        }
        fieldDefinition.setFilterOnField(fieldList.toArray(new String[0]));
      }
      // null definition
      Element nullDefinitionElement = DOMDocumentUtil.getFirstChildElementWithName(XML_NULL_DEFINITION_SUFFIX, field);
      if (nullDefinitionElement != null) {
        Element[] valueElement = DOMDocumentUtil.getChildElementsWithName(XML_VALUE_KEY, nullDefinitionElement);
        List<String> values = new ArrayList<String>();
        for (int k = 0; k < valueElement.length; k++) {
          values.add(DOMDocumentUtil.getNodeChildText(valueElement[k]));
        }
        String[] nullDefinition = values.toArray(new String[0]);
        fieldDefinition.setNullDefinition(nullDefinition);
      }
      // field text handler
      Element handlerElement = DOMDocumentUtil.getFirstChildElementWithName(XML_HANDLER_SUFFIX, field);
      if (handlerElement != null) {
        String handlerClass = DOMDocumentUtil.getNodeChildText(handlerElement);
        if (Strings.nullifyIfBlank(handlerClass) == null) {
          handlerClass = handlerElement.getAttribute(XML_HANDLER_NAME);
        }
        String handlerArgs = handlerElement.getAttribute(XML_HANDLER_ARGS_SUFFIX);
        if (Strings.nullifyIfBlank(handlerArgs) == null) {
          Element config = DOMDocumentUtil.getFirstChildElementWithName(XML_HANDLER_ARGS_SUFFIX, handlerElement);
          if (config != null) {
            handlerArgs = DOMDocumentUtil.getNodeChildText(config);
          }
        }
        if (handlerArgs.equals("")) {
          handlerArgs = null;
        }
        fieldDefinition.setHandler(handlerClass, handlerArgs);
      }
      // is external
      if (field.hasAttribute(XML_IS_EXTERNAL_SUFFIX)) {
        boolean isExternal = Boolean.parseBoolean(field.getAttribute(XML_IS_EXTERNAL_SUFFIX));
        fieldDefinition.setExternal(isExternal);
      }

      // lookup configuration
      Element lookupElement = DOMDocumentUtil.getFirstChildElementWithName(XML_LOOKUP_KEY, field);
      if (lookupElement != null) {
        fieldDefinition.setLookupDefinition(parseLookup(lookupElement, fieldDefinition.getFieldName(), field, false));
      }
      Element remark = DOMDocumentUtil.getFirstChildElementWithName(REMARKS, field);
      if (remark != null) {
        fieldDefinition.setRemarks(DOMDocumentUtil.getNodeChildText(remark));
      }
      // add fieldDefintiion
      allFieldDefinition.add(fieldDefinition);
    }
    return allFieldDefinition;
  }

  private static LookupDefinition parseLookup(Element lookupElement, String lookupForName, Element parentElement, boolean isDefaultOptional) {
    // lookup query
    Element queryElement = DOMDocumentUtil.getFirstChildElementWithName(XML_QUERY_KEY, lookupElement);
    if (queryElement == null) {
      return null;
    }
    {
      String lookupQuery = DOMDocumentUtil.getNodeChildText(queryElement);
      LookupDefinition lookup = new LookupDefinition(lookupQuery);
      // lookup full cache
      Element fullcacheElement = DOMDocumentUtil.getFirstChildElementWithName(XML_FULL_CACHE_KEY, lookupElement);
      if (fullcacheElement != null) {
        lookup.setLookupFullCacheQuery(DOMDocumentUtil.getNodeChildText(fullcacheElement));
      }
      // lookup insert & insert gen key
      Element insertElement = DOMDocumentUtil.getFirstChildElementWithName(XML_INSERT_KEY, lookupElement);
      if (insertElement != null) {
        String lookupInsertQuery = DOMDocumentUtil.getNodeChildText(insertElement);
        String lookupInsertGenKey = null;
        Element insertKeyElement = DOMDocumentUtil.getFirstChildElementWithName(XML_INSERT_KEY_KEY, lookupElement);
        if (insertKeyElement != null) {
          lookupInsertGenKey = DOMDocumentUtil.getNodeChildText(insertKeyElement);
        }
        lookup.setLookupInsertQuery(lookupInsertQuery, lookupInsertGenKey);
      }
      // cache lookup value
      if (parentElement != null && parentElement.hasAttribute(XML_CACHE)) {
        boolean lookupCache = Boolean.parseBoolean(parentElement.getAttribute(XML_CACHE));
        lookup.setLookupCache(lookupCache);
      }
      if (lookupElement.hasAttribute(XML_CACHE)) {
        boolean lookupCache = Boolean.parseBoolean(lookupElement.getAttribute(XML_CACHE));
        lookup.setLookupCache(lookupCache);
      }
      if (lookup.isLookupCache()) {
        Attr attributeNode = lookupElement.getAttributeNode(XML_CACHE_CAPACITY);
        if (attributeNode != null) {
          lookup.setMaximumCacheSize(Integer.parseInt(attributeNode.getNodeValue()));
        }
      }
      // The behaviour is decidedly unusual for some backward compatibility
      // reasons.
      // Added a lookup element specific optional attrib. that will default
      // to false,
      // but the code needs to fall back to the field-level attrib if not
      // present.
      // The default is "true" for the normal field-level attrib but for the
      // lookup
      // behaviour the default is instead false.
      if (lookupElement.hasAttribute(XML_IS_OPTIONAL_SUFFIX)) {
        lookup.setLookupOptional(getBooleanAttribute(XML_IS_OPTIONAL_SUFFIX, lookupElement, isDefaultOptional));
      } else if (parentElement != null){
        getLog().emit(DataLoaderLogClient.LOOKUP_OPTIONAL_ATTR, lookupForName);
        lookup.setLookupOptional(getBooleanAttribute(XML_IS_OPTIONAL_SUFFIX, parentElement, isDefaultOptional));
      } else {
        lookup.setLookupOptional(isDefaultOptional);
      }
      return lookup;
    }
  }

  private static GlobalFilter[] getGlobalFilters(Element root) {
    Element[] filtersParent = DOMDocumentUtil.getChildElementsWithName(FILTERS, root);
    if (filtersParent.length == 0) {
      return new GlobalFilter[]{};
    }
    if (filtersParent.length > 1) {
      throw new IllegalArgumentException("Only one set of global filters permitted, multiple " + FILTERS + " tags found");
    }
    Element[] filterElements = DOMDocumentUtil.getChildElementsWithName(FILTER, filtersParent[0]);
    GlobalFilter[] filters = new GlobalFilter[filterElements.length];
    for (int i = 0; i < filters.length; i++) {
      String filterName = "filter " + i;
      Element lookupElement = DOMDocumentUtil.getFirstChildElementWithName(XML_LOOKUP_KEY, filterElements[i]);
      LookupDefinition lookup = parseLookup(lookupElement, filterName, null, true);
      if (lookup == null) {
        throw new IllegalArgumentException("Filter " + filterName + " found with no lookup definition");
      }
      filters[i] = new GlobalFilter(filterName, lookup);
    }
    return filters;
  }

  private static boolean getBooleanAttribute(String attributeName, Element field, boolean defaultValue) {
    if (field.hasAttribute(attributeName)) {
      return Boolean.parseBoolean(field.getAttribute(attributeName));
    }
    return defaultValue;
  }

  /**
   * The returned LoadDefintion is not yet validated. Validate it with
   * com.profitera.dc.parser.LoadDefinitionValidator
   * 
   * @param defBuffer
   *          Properties contains parsing configuration
   * @return LoadDefinition
   * @throws InvalidConfigurationException
   * @deprecated
   * 
   *             This method is for backward compatibility. It does not support
   *             new feature.
   * @see com.profitera.dc.parser.LoadDefinitionValidator
   */
  public static LoadDefinition parse(Properties defBuffer) throws InvalidConfigurationException {
    LoadDefinition definition = new LoadDefinition();
    definition.setLoadType(getAndTrim(LOAD_TYPE_KEY, defBuffer));
    definition.setDelimiter(getAndTrim(DELIMITER, defBuffer));
    definition.setXmlStartTag(getAndTrim(XML_START_TAG, defBuffer));
    definition.setXQuery(getAndTrim(XQUERY, defBuffer));
    String padLine = getAndTrim(PAD_LINE_KEY, defBuffer);
    if (padLine != null) {
      definition.setPadLine(Boolean.parseBoolean(padLine));
    }
    definition.setDestTable(getAndTrim(TABLE_KEY, defBuffer));
    String generateKey = getAndTrim(GENERATED_KEY_KEY, defBuffer);
    if (generateKey != null) {
      definition.setGenerateKey(true);
      definition.setGenerateKeyColumn(generateKey);
      definition.setGenerateKeySeq(getAndTrim(GENERATED_KEY_SEQ_NAME_KEY, defBuffer));
    }
    String header = getAndTrim(HEADER_NAME_KEY, defBuffer);
    if (header != null) {
      definition.setHeader(header.split(";"));
    }
    String trailer = getAndTrim(TRAILER_NAME_KEY, defBuffer);
    if (trailer != null) {
      definition.setTrailer(trailer.split(";"));
    }
    String refreshData = getAndTrim(REFRESH_DATA_KEY, defBuffer);
    if (refreshData != null) {
      definition.setRefreshData(Boolean.parseBoolean(refreshData));
      definition.setRefreshDataQuery(getAndTrim(REFRESH_DATA_QUERY, defBuffer));
      String timeout = getAndTrim(REFRESH_DATA_QUERY_TIMEOUT, defBuffer);
      if (timeout != null) {
        try {
          definition.setRefreshTimeout(Long.parseLong(timeout));
        } catch (NumberFormatException e) {
          throw new InvalidConfigurationException(new InvalidParserConfiguration(REFRESH_DATA_QUERY_TIMEOUT, timeout));
        }
      }
    }
    definition.setSelectWhereClause(getAndTrim(SELECT_QUERY_WHERE_KEY, defBuffer));
    definition.setUpdateWhereClause(getAndTrim(UPDATE_QUERY_WHERE_KEY, defBuffer));
    definition.setPostInsertInsertQueries(asQueryArray(getAndTrim(POST_INSERT_INSERTION_KEY, defBuffer)));
    definition.setPostInsertUpdateQueries(asQueryArray(getAndTrim(POST_INSERT_UPDATE_KEY, defBuffer)));
    definition.setPostUpdateInsertQueries(asQueryArray(getAndTrim(POST_UPDATE_INSERTION_KEY, defBuffer)));
    definition.setPostUpdateUpdateQueries(asQueryArray(getAndTrim(POST_UPDATE_UPDATE_KEY, defBuffer)));
    // field
    List<FieldDefinition> allFieldDefinition = new ArrayList<FieldDefinition>();
    for (Iterator<Object> i = defBuffer.keySet().iterator(); i.hasNext();) {
      String key = (String) i.next();
      if (key.endsWith(LOCATION_SUFFIX)) {
        String fieldName = key.substring(0, key.length() - LOCATION_SUFFIX.length());
        String locationStart = "0";
        String locationEnd = "0";
        String location = getAndTrim(fieldName + LOCATION_SUFFIX, defBuffer);
        if (location != null) {
          if (definition.getLoadType().equals(RecordDispenserFactory.FIXED_WIDTH)) {
            String[] cut = location.split("[-]|\\s+");
            if (cut.length < 2) {
              throw new InvalidConfigurationException(fieldName + LOCATION_SUFFIX + " value " + location
                  + " invalid, must be in the form <start>-<end>.");
            }
            locationStart = cut[0];
            locationEnd = cut[1];
          } else {
            locationStart = location;
            locationEnd = location;
          }
        }
        String defaultValue = getAndTrim(fieldName + DEFAULT_SUFFIX, defBuffer);
        String[] nullDefinition = new String[0];
        String ndText = getAndTrim(fieldName + NULL_DEFINITION_SUFFIX, defBuffer);
        if (ndText != null) {
          nullDefinition = ndText.split(";");
        }
        String[] filterOnValue = new String[0];
        String filterString = getAndTrim(fieldName + FILTER_ON_SUFFIX, defBuffer);
        if (filterString != null) {
          filterOnValue = filterString.split(";");
        }
        String handlerClass = getAndTrim(fieldName + HANDLER_SUFFIX, defBuffer);
        String handlerArgs = getAndTrim(fieldName + HANDLER_ARGS_SUFFIX, defBuffer);
        boolean isKey = false;
        String isKeyString = getAndTrim(fieldName + IS_KEY_SUFFIX, defBuffer);
        if (isKeyString != null) {
          isKey = Boolean.parseBoolean(isKeyString);
        }
        boolean isExternal = false;
        String isExternalString = getAndTrim(fieldName + IS_EXTERNAL_SUFFIX, defBuffer);
        if (isExternalString != null) {
          isExternal = Boolean.parseBoolean(isExternalString);
        }
        Location loc = new Location(locationStart, locationEnd);
        FieldDefinition field = new FieldDefinition(fieldName, loc, isKey);
        field.setDefaultValue(defaultValue);
        field.setHandler(handlerClass, handlerArgs);
        field.setFilterOnValue(filterOnValue);
        field.setNullDefinition(nullDefinition);
        field.setExternal(isExternal);
        // lookup configuration
        if (getAndTrim(fieldName + LOOKUP_QUERY_SUFFIX, defBuffer) != null) {
          String lookupQuery = getAndTrim(fieldName + LOOKUP_QUERY_SUFFIX, defBuffer);
          String lookupInsertQuery = getAndTrim(fieldName + LOOKUP_INSERT_SUFFIX, defBuffer);
          String lookupInsertGenKey = getAndTrim(fieldName + LOOKUP_KEY_GEN_SUFFIX, defBuffer);
          boolean isCache = true;
          String isCacheString = getAndTrim(fieldName + CACHE, defBuffer);
          if (isCacheString != null) {
            isCache = Boolean.parseBoolean(isCacheString);
          }
          boolean isOptional = false;
          String isOptionalString = getAndTrim(fieldName + IS_OPTIONAL_SUFFIX, defBuffer);
          if (isOptionalString != null) {
            isOptional = Boolean.parseBoolean(isOptionalString);
          }
          LookupDefinition lookupDefinition = new LookupDefinition(lookupQuery);
          lookupDefinition.setLookupInsertQuery(lookupInsertQuery, lookupInsertGenKey);
          lookupDefinition.setLookupCache(isCache);
          lookupDefinition.setLookupOptional(isOptional);
          field.setLookupDefinition(lookupDefinition);
        }

        allFieldDefinition.add(field);
      }
    }
    definition.setFields(allFieldDefinition);
    return definition;
  }
  public static String[] asQueryArray(String singleQuery) {
    String q = Strings.nullifyIfBlank(singleQuery);
    if (q == null) {
      return new String[0];
    } else {
      return new String[]{q};
    }
  }


  private static String getAndTrim(String key, Properties defBuffer) {
    String value = defBuffer.getProperty(key);
    if (value != null) {
      value = value.trim();
    }
    return value;
  }

  private static ILogProvider getLog() {
    if (log == null) {
      DefaultLogProvider p = new DefaultLogProvider();
      p.register(new DataLoaderLogClient());
      log = p;
    }
    return log;
  }

}
