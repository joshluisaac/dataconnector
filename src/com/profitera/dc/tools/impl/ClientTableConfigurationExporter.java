package com.profitera.dc.tools.impl;

import java.io.IOException;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.profitera.dc.impl.FieldTypeInfoRegistry;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.gui.conf.V1TableConfigParser;
import com.profitera.util.reflect.ReflectionException;
import com.profitera.util.xml.DOMDocumentUtil;
import com.profitera.util.xml.DocumentLoader;
import com.profitera.util.xml.DocumentRenderer;

public class ClientTableConfigurationExporter {
  private Document getStarterTemplate() throws IOException {
    Document newDocument = DocumentLoader.getNewDocument();
    Element root = newDocument.createElement("ptruispec");
    root.setAttribute("version", "1.0");
    newDocument.appendChild(root);
    Element table = newDocument.createElement("table");
    root.appendChild(table);
    Element c = newDocument.createElement("column");
    c.setAttribute("name", "NO");
    c.setAttribute("visible", "true");
    table.appendChild(c);
    String tag = "label";
    String text = "No.";
    Element l = DOMDocumentUtil.createElementWithText(newDocument, tag, text);
    c.appendChild(l);
    c.appendChild(DOMDocumentUtil.createElementWithText(newDocument, "width", "28"));
    c.appendChild(DOMDocumentUtil.createElementWithText(newDocument, "data", "NO"));
    return newDocument;
  }
  public String export(LoadDefinition definition) throws IOException, ReflectionException {
    Document document = getStarterTemplate();
    Element tElt = DOMDocumentUtil.getFirstChildElementWithName("table", document.getDocumentElement());
    V1TableConfigParser p = new V1TableConfigParser(tElt);
    FieldTypeInfoRegistry reg = new FieldTypeInfoRegistry(definition);
    List<FieldDefinition> allFields = definition.getAllFields();
    for (FieldDefinition d : allFields) {
      p.addColumn(d.getFieldName(), !d.isExternal());
      p.setColumnData(d.getFieldName(), d.getFieldName());
      p.setColumnWidth(d.getFieldName(), 100);
      p.setColumnLabel(d.getFieldName(), d.getFieldName().replaceAll("_", " "));
      p.setColumnDataType(d.getFieldName(), reg.getHandler(d).getValueType());
    }
    return DocumentRenderer.transform(document, "");
  }
}
