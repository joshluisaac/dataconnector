package com.profitera.dc.parser.impl;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.profitera.dc.RecordDispenserFactory;
import com.profitera.dc.parser.XMLConstantKey;
import com.profitera.util.Strings;
import com.profitera.util.xml.DOMDocumentUtil;
import com.profitera.util.xml.DocumentLoader;
import com.sun.corba.se.impl.ior.ByteBuffer;

/**
 * Use com.profitera.dc.parser.LoadDefinitionValidator to validate it before any
 * processing with it's definition.
 * 
 * @see com.profitera.dc.parser.LoadDefinitionValidator
 * @author ricky
 * 
 */
public class LoadDefinition {
  public static final String MIXED_MODE = "mixed";
  public static final String UPDATE_MODE = "update";
  public static final String INSERT_MODE = "insert";

  public static final String[] RESERVED_DELIMITERS = {"|"};
  public static final String SPACE = "SPACE";
  public static final String TAB = "TAB";

  private String loadType;
  private String updateMode;

  private boolean fullCache = false;
  private boolean padLine = true;
  private String delimiter;
  private String xQuery;
  private String xmlStartTag;

  private String destTable;
  private boolean generateKey = false;;
  private String generateKeyColumn;
  private String generateKeySeq;

  private String[] header = new String[0];
  private String[] trailer = new String[0];

  private String selectWhereClause;
  private String updateWhereClause;

  private boolean refreshData = false;
  private String refreshDataQuery;
  private long refreshTimeout = -1;

  private String[] postInsertInsertQueries;
  private String[] postInsertUpdateQueries;
  private String[] postUpdateInsertQueries;
  private String[] postUpdateUpdateQueries;

  private List<FieldDefinition> fields = new ArrayList<FieldDefinition>();
  private boolean isKeyVerificationScan = false;
  private String documentation;
  private String selectedSheet;
  private GlobalFilter[] globalFilters = new GlobalFilter[0];
  private int lastFieldStart = 0;
  private Integer asciiCodePointDelimiter;

  public String getLoadType() {
    if (loadType == null) {
      return RecordDispenserFactory.FIXED_WIDTH;
    }
    return loadType;
  }
  public boolean isDelimited() {
    return getLoadType().equals(RecordDispenserFactory.DELIMITED);
  }

  public void setLoadType(String loadType) {
    if (Strings.nullifyIfBlank(loadType) == null) {
      loadType = RecordDispenserFactory.FIXED_WIDTH;
    }
    loadType = loadType.trim();
    this.loadType = loadType;
  }

  public String getUpdateMode() {
    if (updateMode == null) {
      return MIXED_MODE;
    }
    return updateMode;
  }

  public void setUpdateMode(String updateMode) {
    if (Strings.nullifyIfBlank(updateMode) == null) {
      updateMode = MIXED_MODE;
    }
    this.updateMode = updateMode.trim();
  }

  public boolean isFullCache() {
    return fullCache;
  }

  public void setFullCache(boolean fullCache) {
    this.fullCache = fullCache;
  }

  public boolean isPadLine() {
    return padLine;
  }

  public void setPadLine(boolean padLine) {
    this.padLine = padLine;
  }

  public String getDelimiter() {
    if (asciiCodePointDelimiter != null) {
      char c = (char) asciiCodePointDelimiter.shortValue();
      return new String(new char[] {c});
    }
    if (delimiter == null || delimiter.length() == 0) {
      return null;
    }
    if (delimiter.equalsIgnoreCase(SPACE)) {
      return " ";
    }
    if (delimiter.equalsIgnoreCase(TAB)) {
      return "\t";
    }
    for (int i = 0; i < RESERVED_DELIMITERS.length; i++) {
      if (delimiter.equals(RESERVED_DELIMITERS[i])) {
        delimiter = "\\" + delimiter;
      }
      break;
    }
    return delimiter;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
    if (delimiter != null) {
      asciiCodePointDelimiter = null;
    }
  }
  public void setAsciiCodepointDelimiter(Integer v) {
    this.asciiCodePointDelimiter = v;
    if (asciiCodePointDelimiter != null) {
      delimiter = null;
    }
  }
  public Integer getAsciiCodePointDelimiter() {
    return asciiCodePointDelimiter;
  }
  public String getXQuery() {
    return xQuery;
  }

  public void setXQuery(String query) {
    xQuery = Strings.nullifyIfBlank(query);
  }

  public String getXmlStartTag() {
    return xmlStartTag;
  }

  public void setXmlStartTag(String xmlStartTag) {
    this.xmlStartTag = Strings.nullifyIfBlank(xmlStartTag);
  }

  public String getDestTable() {
    return destTable;
  }

  public void setDestTable(String destTable) {
    if (destTable != null)
      destTable = destTable.trim();
    this.destTable = Strings.nullifyIfBlank(destTable);
  }

  public boolean isGenerateKey() {
    return generateKey;
  }

  public void setGenerateKey(boolean generateKey) {
    this.generateKey = generateKey;
  }

  public String getGeneratedKeyColumn() {
    return generateKeyColumn == null ? "ID" : generateKeyColumn;
  }

  public void setGenerateKeyColumn(String generateKeyColumn) {
    if (generateKeyColumn != null)
      generateKeyColumn = generateKeyColumn.trim();
    this.generateKeyColumn = Strings.nullifyIfBlank(generateKeyColumn);
  }

  public String getGenerateKeySeq() {
    if (generateKeySeq == null) {
      return getDestTable() + "_" + getGeneratedKeyColumn();
    }
    return generateKeySeq;
  }

  public void setGenerateKeySeq(String generateKeySeq) {
    this.generateKeySeq = Strings.nullifyIfBlank(generateKeySeq);
  }

  public String[] getHeader() {
    return Strings.asEmptyStringArrayIfNull(header);
  }

  public void setHeader(String[] header) {
    this.header = Strings.asEmptyStringArrayIfNull(header);
  }

  public String[] getTrailer() {
    return Strings.asEmptyStringArrayIfNull(trailer);
  }

  public void setTrailer(String[] trailer) {
    this.trailer = Strings.asEmptyStringArrayIfNull(trailer);
  }

  public boolean isHeader(Object lineValue) {
    if (getLoadType().equals(RecordDispenserFactory.XML)) {
      return false;
    } else if (getLoadType().equals(RecordDispenserFactory.MSXLS)) {
      @SuppressWarnings("unchecked")
      String line = ((Map<String, String>) lineValue).get("A");
      return startsWithOneOf(line, getHeader());
    } else {
      String line = (String) lineValue;
      return startsWithOneOf(line, getHeader());
    }
  }

  public boolean isTrailer(Object lineValue) {
    if (getLoadType().equals(RecordDispenserFactory.XML)) {
      return false;
    } else if (getLoadType().equals(RecordDispenserFactory.MSXLS)) {
      @SuppressWarnings("unchecked")
      String line = ((Map<String, String>) lineValue).get("A");
      return startsWithOneOf(line, getTrailer());
    } else {
      String line = (String) lineValue;
      return startsWithOneOf(line, getTrailer());
    }
  }

  private boolean startsWithOneOf(String line, String[] headersOrTrailers) {
    if (line == null) {
      return false;
    } else {
      for (int i = 0; i < headersOrTrailers.length; i++) {
        if (line.startsWith(headersOrTrailers[i])) {
          return true;
        }
      }
    }
    return false;
  }

  public String getSelectWhereClause() {
    return selectWhereClause;
  }

  public void setSelectWhereClause(String selectWhereClause) {
    this.selectWhereClause = Strings.nullifyIfBlank(selectWhereClause);
  }

  public String getUpdateWhereClause() {
    return updateWhereClause;
  }

  public void setUpdateWhereClause(String updateWhereClause) {
    this.updateWhereClause = Strings.nullifyIfBlank(updateWhereClause);
  }

  public boolean isRefreshData() {
    return refreshData;
  }

  public void setRefreshData(boolean refreshData) {
    this.refreshData = refreshData;
  }

  public String getRefreshDataQuery() {
    return refreshDataQuery;
  }

  public void setRefreshDataQuery(String refreshDataQuery) {
    this.refreshDataQuery = Strings.nullifyIfBlank(refreshDataQuery);
  }

  public long getRefreshTimeout() {
    return refreshTimeout;
  }

  public void setRefreshTimeout(long refreshTimeout) {
    if (refreshTimeout < 0)
      refreshTimeout = -1;
    this.refreshTimeout = refreshTimeout;
  }

  public String[] getPostInsertInsertQueries() {
    return postInsertInsertQueries;
  }

  public void setPostInsertInsertQueries(String[] s) {
    this.postInsertInsertQueries = s;
  }

  public String[] getPostInsertUpdateQueries() {
    return postInsertUpdateQueries;
  }

  public void setPostInsertUpdateQueries(String[] statements) {
    this.postInsertUpdateQueries = statements;
  }

  public String[] getPostUpdateInsertQueries() {
    return postUpdateInsertQueries;
  }

  public void setPostUpdateInsertQueries(String[] s) {
    this.postUpdateInsertQueries = s;
  }

  public String[] getPostUpdateUpdateQueries() {
    return postUpdateUpdateQueries;
  }

  public void setPostUpdateUpdateQueries(String[] s) {
    this.postUpdateUpdateQueries = s;
  }

  public String[] getAllFieldName() {
    String[] name = new String[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      name[i] = fields.get(i).getFieldName();
    }
    return name;
  }

  public String[] getAllKeyName() {
    List<String> keys = new ArrayList<String>();
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).isKey()) {
        keys.add(fields.get(i).getFieldName());
      }
    }
    return keys.toArray(new String[0]);
  }

  public FieldDefinition[] getAllKeys() {
    List<FieldDefinition> keys = new ArrayList<FieldDefinition>();
    for (int i = 0; i < fields.size(); i++) {
      if (fields.get(i).isKey()) {
        keys.add(fields.get(i));
      }
    }
    return keys.toArray(new FieldDefinition[0]);
  }

  public List<FieldDefinition> getAllFields() {
    return fields;
  }
  public void putFieldBeforeInList(FieldDefinition def, String beforeMe) {
    FieldDefinition beforeMeField = getField(beforeMe);
    FieldDefinition moveMe = getField(def.getFieldName());
    fields.remove(moveMe);
    int indexOf = fields.indexOf(beforeMeField);
    fields.add(indexOf, moveMe);
  }

  public void moveFieldUp(String fieldName) {
    for (int i = 0; i < fields.size(); i++) {
      FieldDefinition f = fields.get(i);
      if (f.getFieldName().equals(fieldName) && i > 0) {
        FieldDefinition above = fields.get(i - 1);
        fields.set(i - 1, f);
        fields.set(i, above);
      }
    }
  }
  
  public FieldDefinition getField(String fieldName) {
    for (int i = 0; i < fields.size(); i++) {
      FieldDefinition f = fields.get(i);
      if (f.getFieldName().equals(fieldName))
        return f;
    }
    return null;
  }

  public void setFields(List<FieldDefinition> fields) {
    this.fields = fields;
    lastFieldStart = findLastFieldStart(fields);
  }

  public void updateField(String currentName, FieldDefinition newField) {
    ArrayList<FieldDefinition> fieldCopy = new ArrayList<FieldDefinition>(fields);
    for (int i = 0; i < fieldCopy.size(); i++) {
      if (fieldCopy.get(i).getFieldName().equals(currentName)) {
        fieldCopy.set(i, newField);
        setFields(fieldCopy);
        return;
      }
    }
    fieldCopy.add(newField);
    setFields(fieldCopy);
  }

  public void updateField(FieldDefinition newField) {
    updateField(newField.getFieldName(), newField);
  }

  public void removeField(String fieldName) {
    if (fieldName != null) {
      ArrayList<FieldDefinition> fieldCopy = new ArrayList<FieldDefinition>(fields);
      for (int i = 0; i < fieldCopy.size(); i++) {
        FieldDefinition field = fieldCopy.get(i);
        if (field.getFieldName().equals(fieldName)) {
          fieldCopy.remove(i);
          setFields(fieldCopy);
          return;
        }
      }
    }
  }

  /*
   * Purposely load everything into XML document for rich information, except
   * generate key for backward compatibility.
   */
  public Document asXML() {
    Document doc = DocumentLoader.parseDocument("<load/>");
    Element root = doc.getDocumentElement();
    root.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_DOC, getDocumentation()));
    root.setAttribute(XMLConstantKey.XML_FULL_CACHE_KEY, String.valueOf(isFullCache()));
    root.setAttribute(XMLConstantKey.XML_VERIFY_UNIQUENESS_KEY, String.valueOf(isKeyVerificationScan()));
    root.setAttribute(XMLConstantKey.XML_MODE_KEY, getUpdateMode());
    root.setAttribute(XMLConstantKey.XML_TYPE_KEY, getLoadType());
    //TODO: Fix non-printable character saving here!
    if (getAsciiCodePointDelimiter() != null) {
      root.setAttribute(XMLConstantKey.XML_ASCII_DELIMITER_KEY, "" + getAsciiCodePointDelimiter());
    } else {
      root.setAttribute(XMLConstantKey.XML_DELIMITER_KEY, delimiter);
    }
    root.setAttribute(XMLConstantKey.XML_PAD_LINE_KEY, String.valueOf(isPadLine()));

    root.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_TABLE_KEY, getDestTable()));
    root.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_XML_START_TAG, getXmlStartTag()));
    root.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_XQUERY, getXQuery()));

    if (isGenerateKey()) {
      root.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_GENERATED_KEY_KEY,
          getGeneratedKeyColumn()));
      root.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_GENERATED_KEY_SEQ_NAME_KEY,
          getGenerateKeySeq()));
    }

    // queryWhere
    Element queryWhereElement = DOMDocumentUtil.appendNewChild(root, XMLConstantKey.XML_QUERY_WHERE_KEY);
    queryWhereElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_SELECT_KEY,
        getSelectWhereClause()));
    queryWhereElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_UPDATE_KEY,
        getUpdateWhereClause()));
    root.appendChild(queryWhereElement);

    // post
    Element postElement = DOMDocumentUtil.appendNewChild(root, XMLConstantKey.XML_POST_KEY);
    
    appendGroupedStatements(XMLConstantKey.XML_INSERT_INSERTION_KEY, getPostInsertInsertQueries(), postElement, doc);
    appendGroupedStatements(XMLConstantKey.XML_INSERT_UPDATE_KEY, getPostInsertUpdateQueries(), postElement, doc);
    appendGroupedStatements(XMLConstantKey.XML_UPDATE_INSERTION_KEY, getPostUpdateInsertQueries(), postElement, doc);
    appendGroupedStatements(XMLConstantKey.XML_UPDATE_UPDATE_KEY, getPostUpdateUpdateQueries(), postElement, doc);
    root.appendChild(postElement);

    // header
    Element headerElement = DOMDocumentUtil.appendNewChild(root, XMLConstantKey.XML_HEADER_KEY);
    for (String value : header)
      headerElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_VALUE_KEY, value));
    root.appendChild(headerElement);

    // trailer
    Element trailerElement = DOMDocumentUtil.appendNewChild(root, XMLConstantKey.XML_TRAILER_KEY);
    for (String value : trailer)
      trailerElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_VALUE_KEY, value));
    root.appendChild(trailerElement);

    Element refreshDataElement = DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_REFRESH_DATA_KEY,
        getRefreshDataQuery());
    refreshDataElement.setAttribute(XMLConstantKey.XML_KEY, String.valueOf(isRefreshData()));
    refreshDataElement.setAttribute(XMLConstantKey.XML_REFRESH_DATA_QUERY_TIMEOUT, String.valueOf(getRefreshTimeout()));
    root.appendChild(refreshDataElement);

    // fields
    for (FieldDefinition field : fields) {
      Element fieldElement = DOMDocumentUtil.appendNewChild(root, XMLConstantKey.XML_FIELD_KEY);
      fieldElement.setAttribute(XMLConstantKey.XML_IS_KEY_SUFFIX, String.valueOf(field.isKey()));
      fieldElement.setAttribute(XMLConstantKey.XML_IS_OPTIONAL_SUFFIX, String.valueOf(field.isOptional()));
      fieldElement.setAttribute(XMLConstantKey.XML_IS_EXTERNAL_SUFFIX, String.valueOf(field.isExternal()));
      fieldElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_FIELD_NAME,
          field.getFieldName()));
      fieldElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_DEFAULT_SUFFIX,
          field.getDefaultValue()));
      Element fieldLocElement = DOMDocumentUtil.appendNewChild(fieldElement, XMLConstantKey.XML_LOCATION_SUFFIX);
      fieldLocElement.setAttribute(XMLConstantKey.XML_LOCATION_START, field.getLocation().getStart());
      fieldLocElement.setAttribute(XMLConstantKey.XML_LOCATION_END, field.getLocation().getEnd());
      Element handlerElement = DOMDocumentUtil.appendNewChild(fieldElement, XMLConstantKey.XML_HANDLER_SUFFIX);
      handlerElement.setAttribute(XMLConstantKey.XML_HANDLER_NAME, field.getHandler());
      handlerElement.setAttribute(XMLConstantKey.XML_HANDLER_ARGS_SUFFIX, field.getHandlerArgs());
      Element nullDefElement = DOMDocumentUtil.appendNewChild(fieldElement, XMLConstantKey.XML_NULL_DEFINITION_SUFFIX);
      for (String value : field.getNullDefinition()) {
        nullDefElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_VALUE_KEY, value));
      }
      Element filterElement = DOMDocumentUtil.appendNewChild(fieldElement, XMLConstantKey.XML_FILTER_ON_SUFFIX);

      for (String value : field.getFilterOnValue()) {
        filterElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_VALUE_KEY, value));
      }
      for (String value : field.getFilterOnField()) {
        filterElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_FIELD_KEY, value));
      }
      // lookup
      if (field.isLookupField()) {
        Element lookupElement = DOMDocumentUtil.appendNewChild(fieldElement, XMLConstantKey.XML_LOOKUP_KEY);
        LookupDefinition lookup = field.getLookupDefinition();
        lookupElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_QUERY_KEY,
            lookup.getLookupQuery()));
        lookupElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_FULL_CACHE_KEY,
            lookup.getLookupFullCacheQuery()));
        lookupElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_INSERT_KEY,
            lookup.getLookupInsertQuery()));
        lookupElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_INSERT_KEY_KEY,
            lookup.getLookupInsertGenKey()));
        lookupElement.setAttribute(XMLConstantKey.XML_IS_OPTIONAL_SUFFIX, String.valueOf(lookup.isLookupOptional()));
        fieldElement.setAttribute(XMLConstantKey.XML_CACHE, String.valueOf(lookup.isLookupCache()));
        if (lookup.getMaximumCacheSize() != null) {
          lookupElement.setAttribute(XMLConstantKey.XML_CACHE_CAPACITY, String.valueOf(lookup.getMaximumCacheSize()));
        }
      }
      if (field.getRemarks().length() != 0) {
        fieldElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.REMARKS,
            field.getRemarks()));
      }
    }
    GlobalFilter[] filters = getGlobalFilters();
    Element filtersElement = (Element) root.appendChild(doc.createElement(XMLConstantKey.FILTERS));
    for (int i = 0; i < filters.length; i++) {
      Element filterElement = (Element) filtersElement.appendChild(doc.createElement(XMLConstantKey.FILTER));
      // This should use general lookup rendering code, but it does not.
      Element lookupElement = (Element) filterElement.appendChild(doc.createElement(XMLConstantKey.XML_LOOKUP_KEY));
      lookupElement.appendChild(DOMDocumentUtil.createElementWithText(doc, XMLConstantKey.XML_QUERY_KEY,
          filters[i].getLookup().getLookupQuery()));

    }
    
    return doc;
  }

  private void appendGroupedStatements(String groupTag, String[] queries,
      Element grandParentElement, Document doc) {
    if (queries != null && queries.length > 0) {
      Element insertInsert = (Element) grandParentElement.appendChild(doc.createElement(groupTag));
      for (int i = 0; i < queries.length; i++) {
        insertInsert.appendChild(DOMDocumentUtil.createElementWithText(doc, "statement", queries[i]));
      }
    }
  }

  public boolean isKeyVerificationScan() {
    return isKeyVerificationScan;
  }

  public void setKeyVerificationScan(boolean b) {
    isKeyVerificationScan = b;
  }

  public boolean isGeneratedKeyFieldAlsoDefined() {
    return isGenerateKey() && getField(getGeneratedKeyColumn()) != null;
  }

  public void setDocumentation(String text) {
    this.documentation = text;
  }

  public String getDocumentation() {
    return documentation;
  }

  public void setSelectedSheet(String nameOrIndex) {
    this.selectedSheet = nameOrIndex;
  }

  public String getSelectedSheet() {
    return selectedSheet;
  }

  public void setGlobalFilters(GlobalFilter[] filters) {
    this.globalFilters  = filters;
  }
  public GlobalFilter[] getGlobalFilters() {
    return Arrays.copyOf(globalFilters, globalFilters.length);
  }

  public int getLastFieldStart() {
    return lastFieldStart;
  }
  private int findLastFieldStart(List<FieldDefinition> allFields) {
    if (allFields == null || allFields.isEmpty()) {
      return 0;
    }
    int last = Integer.parseInt(allFields.get(0).getLocation().getStart());
    for (FieldDefinition fieldDefinition : allFields) {
      int start = Integer.parseInt(fieldDefinition.getLocation().getStart());
      if (start > last) {
        last = start;
      }
    }
    return last;
  }
}
