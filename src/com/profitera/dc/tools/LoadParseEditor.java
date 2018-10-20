package com.profitera.dc.tools;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.BorderFactory;
import javax.swing.DefaultListCellRenderer;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToolBar;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.text.JTextComponent;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;

import com.profitera.dc.ErrorSummary;
import com.profitera.dc.InvalidLookupQueryException;
import com.profitera.dc.LoadingErrorList;
import com.profitera.dc.RecordDispenserFactory;
import com.profitera.dc.handler.IFieldTextHandler;
import com.profitera.dc.handler.StringHandler;
import com.profitera.dc.impl.FixedWidthFileOutput;
import com.profitera.dc.parser.LoadDefinitionValidator;
import com.profitera.dc.parser.LoadingQueryWriter;
import com.profitera.dc.parser.V2LoadDefinitionParser;
import com.profitera.dc.parser.XMLConstantKey;
import com.profitera.dc.parser.exception.InvalidConfigurationException;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.GlobalFilter;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.dc.parser.impl.Location;
import com.profitera.dc.parser.impl.LookupDefinition;
import com.profitera.dc.parserconverter.PropertiesToXMLParser;
import com.profitera.dc.tools.TextFilePreviewer.Span;
import com.profitera.dc.tools.impl.ClientTableConfigurationExporter;
import com.profitera.dc.tools.impl.ComponentGroup;
import com.profitera.dc.tools.impl.DBConfigurator;
import com.profitera.dc.tools.impl.ISpanOptionProvider;
import com.profitera.dc.tools.impl.JTextAreaPrintStream;
import com.profitera.dc.tools.impl.ListMapTableModel;
import com.profitera.dc.tools.impl.SampleDataLoader;
import com.profitera.dc.tools.impl.TableGenerator;
import com.profitera.layout.TablePos;
import com.profitera.math.MathUtil;
import com.profitera.prefs.PreferencesManager;
import com.profitera.swing.Images;
import com.profitera.swing.PreferenceUtil;
import com.profitera.swing.file.FileOpenAction;
import com.profitera.swing.file.FileOpenAction.IFileOpener;
import com.profitera.swing.menu.AbstractPopupMenuMouseListener;
import com.profitera.swing.scroll.JScrollPaneUtil;
import com.profitera.swing.selector.FileSelector;
import com.profitera.swing.split.JSplitPaneUtil;
import com.profitera.swing.text.JTextUtil;
import com.profitera.util.CollectionUtil;
import com.profitera.util.Strings;
import com.profitera.util.Utilities;
import com.profitera.util.io.FileUtil;
import com.profitera.util.reflect.ClassComparator;
import com.profitera.util.reflect.ClasspathSearcher;
import com.profitera.util.reflect.Reflect;
import com.profitera.util.reflect.ReflectionException;
import com.profitera.util.xml.DocumentLoader;
import com.profitera.util.xml.DocumentRenderer;

import info.clearthought.layout.TableLayout;

public class LoadParseEditor {

  private static final int LOOKUP_TEXT_COLUMNS = 42;
  private static final String AUTOINCREMENT = "autoincrement";
  private LoadDefinition definition;
  private File currentFile;
  private ComponentGroup columnGroup = new ComponentGroup();
  private ComponentGroup loadingGroup = new ComponentGroup();

  private JFrame frame;

  private JComboBox<String> loadTypeComboBox;
  private JComboBox<String> loadModeComboBox;
  private JComboBox fieldHandlerComboBox;
  private JCheckBox fullcacheCheckBox;
  private JCheckBox padlineCheckBox;
  private JCheckBox generateKeyCheckBox;
  private JCheckBox refreshDataCheckBox;
  private JCheckBox isKeyCheckBox;
  private JCheckBox isExternalCheckBox;
  private JCheckBox lookupCacheCheckBox;
  private JCheckBox lookupOptionalCheckBox;
  private JCheckBox filterNullCheckBox;
  private JCheckBox filterNotNullCheckBox;
  private JTextField columnNameTextField;
  private JTextField cacheCapacityTextField;
  private JTextField locationStartTextField;
  private JTextField locationEndTextField;
  private JTextField tableNameTextField;
  private JTextField generateKeyTextField;
  private JTextField generateKeySequenceTextField;
  private JTextField defaultValueTextField;
  private JTextField handlerArgsTextField;
  private JTextArea refreshQueryTextField;
  private JTextArea queryWhereSelectTextField;
  private JTextArea queryWhereUpdateTextField;
  private JTextArea lookupQueryTextField;
  private JTextArea lookupQueryInsertTextField;
  private JTextArea lookupQueryInsertKeyTextField;
  private JTextArea lookupQueryFullCacheTextField;
  private JTextArea globalFilterLookupTextField;
  private JList<FieldDefinition> columnList;
  private StringListEditor nullDefinitionList;
  private StringListEditor headerList;
  private StringListEditor trailerList;
  private StringListEditor filterValueList;
  private StringListEditor filterFieldList;
  private DefaultListModel<FieldDefinition> columnListModel;

  private FileOpenAction openAction;
  private JSplitPane editor;
  private JPanel preview;
  private FileOpenAction sampleFileOpener;
  private TextFilePreviewer textFielPreviewer;

  private JToolBar tool;
  private DBConfigurator dbConfigurator;
  private JLabel editingFileLabel;
  private JCheckBox verifyFileKeyValuesUniqueCheckBox;
  private JCheckBox isOptionalCheckBox;
  private JTextArea intermediateTableDefinition;
  private JTextArea loaderCommand;
  private JTextArea generatedStoredProcedure;
  private JTextArea documentationText;
  private JTextArea fieldRemarksTextField;
  private JTextArea targetTableDefinitionTextField;
  private JTextField locationLengthTextField;
  private PostActionStatementListEditor postActionEditor;
  private JComboBox<String> sampleFileEncodingSelector;
  private JComboBox<String> delimiterTypeComboBox;
  private JTextField delimiterTextField;

  public LoadParseEditor() {
    PreferencesManager.getInstance().setPreferencesClass(this.getClass());
    getNewDefinitionAction().actionPerformed(null);
    SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        getFrame().setVisible(true);
      }
    });

  }

  public JFrame getFrame() {
    if (frame == null) {
      frame = new JFrame() {
        private static final long serialVersionUID = 1L;
        @Override
        public void setTitle(String title) {
          if (title == null)
            title = "";
          super.setTitle("Profitera PowerApps - Data Mapping Workbench  [file: " + title + "]");
        }
      };

      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      frame.getContentPane().add(getDisplayComponent());
      frame.getContentPane().add(getToolbar(), BorderLayout.NORTH);
      frame.pack();
      frame.setLocationRelativeTo(null);
      PreferenceUtil.trackWindow(frame, getClass().getName(), frame.getWidth(), frame.getHeight());
    }
    return frame;
  }

  private JLabel getValidationMessageLabel() {
    if (editingFileLabel == null) {
      editingFileLabel = new JLabel();
    }
    return editingFileLabel;
  }

  private JComponent getToolbar() {
    if (tool == null) {
      tool = new JToolBar();
      tool.add(getNewDefinitionAction());
      tool.add(getDefinitionSaveAction());
      tool.add(getDefinitionOpenAction());
      tool.add(getDefinitionExportAction());
      tool.addSeparator();
    }
    return tool;
  }

  private ComponentGroup getColumnGroup() {
    return columnGroup;
  }

  private ComponentGroup getLoadingGroup() {
    return loadingGroup;
  }

  private JComponent getDisplayComponent() {
    TableLayout tl = new TableLayout(new double[] {5, 150, TableLayout.PREFERRED, TableLayout.FILL,
        TableLayout.PREFERRED, 5}, new double[] {5, TableLayout.PREFERRED, 5, TableLayout.FILL, 5,
        TableLayout.PREFERRED, 5});
    JPanel mainPane = new JPanel(tl);
    mainPane.add(new JLabel("Table Name  "), "1,1,1,1");
    mainPane.add(getTableNameTextField(), "2,1,2,1");
    mainPane.add(getTabbedPanel(), "1,3,4,3");
    mainPane.add(getValidationMessageLabel(), "1,5,4,5");
    return mainPane;
  }

  private JComponent getTabbedPanel() {
    JTabbedPane p = new JTabbedPane();
    p.addTab("Columns", getColumnTab());
    p.addTab("Loading", getLoadingTab());
    p.addTab("Filter", getFiltersPane());
    p.addTab("Custom Where/Refresh Data", getPreActionPane());
    p.addTab("Post action", getPostActionPane());
    p.addTab("Native Loader Statements", getNativeLoaderPane());
    p.addTab("Sample Target Table Statements", getTargetTablePane());
    getLoadingGroup().addChangeListener(new ChangeListener() {
      public void stateChanged(ChangeEvent e) {
        updateLoadConfig(e);
      }
    });
    p.addChangeListener(new ChangeListener() {
      @Override
      public void stateChanged(ChangeEvent e) {
        updateNativeLoaderPane();
        updateTargetTablePane();
      }
    });
    return p;
  }
  private void updateTargetTablePane() {
    TableGenerator tableGenerator = new TableGenerator(definition);
    try {
      String main = tableGenerator.getTargetTable();
      String ext = tableGenerator.getTableForExternalFields();
      String extInsert = tableGenerator.getInsertForExternalTable();
      String extUpdate = tableGenerator.getUpdateForExternalTable();
      getTargetTableDefinitionTextField().setText(main + ";\n\n" + ext + ";\n"+ extInsert + ";\n"+ extUpdate + ";\n");
    } catch (ReflectionException e) {
      getTargetTableDefinitionTextField().setText("Unable to render table definition");
    }
  }
  private void updateLoadConfig(ChangeEvent e) {
    definition.setDocumentation(getDocumentationText().getText());
    definition.setDestTable(getTableNameTextField().getText());
    definition.setPadLine(getPadlineCheckBox().isSelected());
    definition.setHeader(getHeaderList().getData());
    definition.setTrailer(getTrailerList().getData());
    definition.setUpdateMode((String) getLoadModeComboBox().getSelectedItem());
    definition.setLoadType((String) getLoadTypeComboBox().getSelectedItem());
    if (getDelimiterTypeComboBox().getSelectedIndex() == 0) {
      definition.setDelimiter(getDelimiterTextField().getText());
    } else {
      try {
      int v = Integer.parseInt(getDelimiterTextField().getText());
      definition.setAsciiCodepointDelimiter(v);
      } catch (NumberFormatException nfe) {}
    }
    definition.setFullCache(getFullcacheCheckBox().isSelected());
    definition.setKeyVerificationScan(getVerifyFileKeyValuesUniqueCheckBox().isSelected());
    definition.setGenerateKey(getGenerateKeyCheckBox().isSelected());
    definition.setGenerateKeyColumn(getGenerateKeyTextField().getText());
    definition.setGenerateKeySeq(getGenerateKeySequenceTextField().getText());
    definition.setRefreshData(getRefreshDataCheckBox().isSelected());
    definition.setRefreshDataQuery(getRefreshQueryTextField().getText());
    definition.setSelectWhereClause(getQueryWhereSelectTextField().getText());
    definition.setUpdateWhereClause(getQueryWhereUpdateTextField().getText());
    Map<String, List<String>> data = getPostActionStatementsEditor().getData();
    definition.setPostInsertInsertQueries(data.get(XMLConstantKey.XML_INSERT_INSERTION_KEY).toArray(new String[0]));
    definition.setPostInsertUpdateQueries(data.get(XMLConstantKey.XML_INSERT_UPDATE_KEY).toArray(new String[0]));
    definition.setPostUpdateInsertQueries(data.get(XMLConstantKey.XML_UPDATE_INSERTION_KEY).toArray(new String[0]));
    definition.setPostUpdateUpdateQueries(data.get(XMLConstantKey.XML_UPDATE_UPDATE_KEY).toArray(new String[0]));
    //
    String filterQueryValue = getGlobalFilterLookupTextField().getText();
    if (filterQueryValue.trim().equals("")) {
      definition.setGlobalFilters(new GlobalFilter[]{});
    } else {
      definition.setGlobalFilters(new GlobalFilter[]{new GlobalFilter("filter 0", new LookupDefinition(filterQueryValue))});
    }
    //
    validateDefinition();
    SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        refresh();
      }
    });
  }

  private JComponent getPreActionPane() {
    JPanel preAction = new JPanel(new BorderLayout());
    JPanel whereClause = new JPanel(new TableLayout(new double[] {5, TableLayout.FILL, 5}, new double[] {5,
        TableLayout.PREFERRED, 80, 5, TableLayout.PREFERRED, 80, 10}));
    whereClause.setBorder(BorderFactory.createTitledBorder("Custom Where Clause"));
    whereClause.add(new JLabel("Select"), "1,1");
    whereClause.add(new JScrollPane(getQueryWhereSelectTextField()), "1,2");
    whereClause.add(new JLabel("Update"), "1,4");
    whereClause.add(new JScrollPane(getQueryWhereUpdateTextField()), "1,5");
    preAction.add(whereClause, BorderLayout.NORTH);
    preAction.add(getRefreshDataPane(), BorderLayout.CENTER);
    return preAction;
  }

  private JComponent getRefreshDataPane() {
    TableLayout tl = new TableLayout(new double[] {5, TableLayout.FILL, 5}, new double[] {5, TableLayout.PREFERRED, 5,
        TableLayout.PREFERRED, TableLayout.FILL, 5});
    JPanel p = new JPanel(tl);
    p.setBorder(BorderFactory.createTitledBorder("Refresh Data Configuration"));
    p.add(getRefreshDataCheckBox(), "1,1");
    p.add(new JLabel("Custom refresh query "), "1,3");
    p.add(new JScrollPane(getRefreshQueryTextField()), "1,4");
    return p;
  }

  private JComponent getFiltersPane() {
    JPanel p = new JPanel(new BorderLayout());
    p.add(new JLabel("Filter Query"), BorderLayout.BEFORE_FIRST_LINE);
    p.add(getGlobalFilterLookupTextField(), BorderLayout.CENTER);
    return p;
  }

  private JComponent getPostActionPane() {
    JPanel p = new JPanel(new BorderLayout());
    p.add(getPostActionStatementsEditor());
    return p;
  }

  private PostActionStatementListEditor getPostActionStatementsEditor() {
    if (postActionEditor == null) {
      postActionEditor = new PostActionStatementListEditor("Post Insert/Update Statements");
    }
    return postActionEditor;
  }

  private JComponent getNativeLoaderPane() {
    GridLayout gl = new GridLayout(3, 1);
    gl.setHgap(10);
    gl.setVgap(10);
    JPanel p = new JPanel(gl);
    JPanel ii = new JPanel(new BorderLayout());
    ii.setBorder(BorderFactory.createTitledBorder("Intermediate Table Definition"));
    ii.add(new JScrollPane(getIntermediateTableDefinitionTextField()), BorderLayout.CENTER);
    p.add(ii);
    JPanel iu = new JPanel(new BorderLayout());
    iu.setBorder(BorderFactory.createTitledBorder("Loader Command"));
    iu.add(new JScrollPane(getIntermediateLoaderCommandTextField()), BorderLayout.CENTER);
    p.add(iu);
    JPanel ui = new JPanel(new BorderLayout());
    ui.setBorder(BorderFactory.createTitledBorder("Generated Stored Procedure"));
    ui.add(new JScrollPane(getGeneratedStoredProcedureTextField()), BorderLayout.CENTER);
    p.add(ui);
    return p;
  }
  private JComponent getTargetTablePane() {
    JPanel p = new JPanel(new BorderLayout());
    p.add(new JScrollPane(getTargetTableDefinitionTextField()), BorderLayout.CENTER);
    return p;
  }


  private JTextArea getIntermediateTableDefinitionTextField() {
    if (intermediateTableDefinition == null) {
      intermediateTableDefinition = new JTextArea();
      intermediateTableDefinition.setEditable(false);
    }
    return intermediateTableDefinition;
  }
  private JTextArea getIntermediateLoaderCommandTextField() {
    if (loaderCommand == null) {
      loaderCommand = new JTextArea();
      loaderCommand.setEditable(false);
    }
    return loaderCommand;
  }
  private JTextArea getGeneratedStoredProcedureTextField() {
    if (generatedStoredProcedure == null) {
      generatedStoredProcedure = new JTextArea();
      generatedStoredProcedure.setEditable(false);
    }
    return generatedStoredProcedure;
  }
  private JTextArea getTargetTableDefinitionTextField() {
    if (targetTableDefinitionTextField == null) {
      targetTableDefinitionTextField = new JTextArea();
      targetTableDefinitionTextField.setEditable(false);
    }
    return targetTableDefinitionTextField;
  }

  private JComponent getLoadingTab() {
    TableLayout tl = new TableLayout(new double[] {5, TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED,
        TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.FILL, 15},
        new double[] {5, TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED,
        TableLayout.PREFERRED, 20, TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED,
        TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.FILL, 5});
    tl.setVGap(4);
    tl.setHGap(4);
    JPanel p = new JPanel(tl);
    p.add(new JLabel("Loading type"), TablePos.pos(1, 1));
    p.add(getLoadTypeComboBox(), TablePos.pos(1, 2));
    p.add(getDelimiterTypeComboBox(), TablePos.pos(1, 3));
    p.add(new JLabel(": "), TablePos.pos(1, 4));
    p.add(getDelimiterTextField(), TablePos.pos(1, 5));
    p.add(getPadlineCheckBox(), "2,2,2,2");
    p.add(getHeaderList(), "2,3,2,3");
    p.add(getTrailerList(), "3,3,3,3");

    p.add(new JLabel("Loading mode"), "1,6");
    p.add(getLoadModeComboBox(), "2,6");
    p.add(getFullcacheCheckBox(), "2,7,2,7");
    p.add(getVerifyFileKeyValuesUniqueCheckBox(), "2,8,2,8");

    JPanel keyPane = new JPanel(new TableLayout(new double[] {5, 150, TableLayout.PREFERRED, 5}, new double[] {5,
        TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED, 5}));
    keyPane.add(getGenerateKeyCheckBox(), "1,1,2,1");
    keyPane.add(new JLabel(" Column "), "1,2");
    keyPane.add(getGenerateKeyTextField(), "2,2");
    keyPane.add(new JLabel(" Sequence "), "1,3");
    keyPane.add(getGenerateKeySequenceTextField(), "2,3");
    keyPane.setBorder(BorderFactory.createTitledBorder(keyPane.getBorder()));

    p.add(keyPane, "2,9,3,9");
    return new JScrollPane(p);
  }

  private JComponent getColumnTab() {
    JSplitPane p = JSplitPaneUtil.buildLeftRightSplit(getAddRemoveColumnEditor(), getEditor(), null);
    PreferenceUtil.trackDividerLocation(p, getClass().getName() + p, 200);
    return p;
  }

  private JSplitPane getEditor() {
    if (editor == null) {
      JTabbedPane fieldEditorTabs = new JTabbedPane();
      fieldEditorTabs.addTab("Basic configuration", getBasicFieldConfigPanel());
      fieldEditorTabs.addTab("Lookup configuration", getLookupConfigPanel());
      fieldEditorTabs.addTab("Filter configuration", getFilterConfigPanel());
      fieldEditorTabs.addTab("Null Definition", getNullDefinitionList());
      TableLayout tl = new TableLayout(new double[]{TableLayout.PREFERRED, TableLayout.FILL}, new double[]{
          TableLayout.PREFERRED, TableLayout.FILL
      });
      tl.setVGap(4);
      JPanel fieldEditor = new JPanel(tl);
      fieldEditor.add(fieldEditorTabs, TablePos.colPos(0, 0, 1));
      fieldEditor.add(new JLabel("Field Remarks"), TablePos.pos(0, 1));
      fieldEditor.add(JScrollPaneUtil.wrap(getFieldRemarksTextField()), TablePos.pos(1, 1));
      JTabbedPane docsAndSampleTabs = new JTabbedPane();
      docsAndSampleTabs.addTab("Load Documentation/Notes", JScrollPaneUtil.wrap(getDocumentationText()));
      docsAndSampleTabs.addTab("Sample Source File", getSourceFilePreview());
      editor = JSplitPaneUtil.buildTopBottomSplit(docsAndSampleTabs, fieldEditor);
      PreferenceUtil.trackDividerLocation(editor, getClass().getName() + editor, 120);
      getColumnGroup().setEnabled(false);
      getColumnGroup().addChangeListener(new ChangeListener() {
        public void stateChanged(ChangeEvent e) {
          updateColumn(e);
        }
      });
    }
    return editor;
  }

  private JTextArea getDocumentationText() {
    if (documentationText == null) {
      documentationText = new JTextArea();
      loadingGroup.add(documentationText, "");
    }
    return documentationText;
  }

  protected void updateColumn(ChangeEvent e) {
    String newName = getColumnNameTextField().getText();
    if (newName.equals("")) {
      getValidationMessageLabel().setText("Field name cannot be blanked.");
      return;
    }
    final String currentName = getSelectedField().getFieldName();
    boolean nameChanged = !(newName.equals("") || currentName.equals(newName));
    if (nameChanged && definition.getField(newName) != null) {
      getValidationMessageLabel().setText("Duplicated field name " + newName);
      return;
    }

    System.out.println("Updating " + currentName + " with " + e);
    // Basic stuff
    boolean isKey = getIsKeyCheckBox().isSelected();
    if (isKey) {
      getIsOptionalCheckBox().setEnabled(false);
      getIsOptionalCheckBox().setSelected(false);
    }
    boolean isOptional = getIsOptionalCheckBox().isSelected();
    String start = getLocationStartTextField().getText();
    String end = getLocationEndTextField().getText();
    if (definition.getLoadType().equals(RecordDispenserFactory.FIXED_WIDTH)) {
      start = Strings.leftTrim(start, "0");
      end = Strings.leftTrim(end, "0");
      if (start.equals(""))
        start = "0";
      if (end.equals(""))
        end = "0";
    }
    Location l = new Location(start, end);
    FieldDefinition def = new FieldDefinition(newName, l, isKey);
    def.setRemarks(getFieldRemarksTextField().getText());
    def.setOptional(isOptional);
    def.setDefaultValue(getDefaultValueTextField().getText());
    def.setExternal(getIsExternalCheckBox().isSelected());

    // Lookup
    if (getLookupQueryField().getText().trim().length() != 0) {
      LookupDefinition look = new LookupDefinition(getLookupQueryField().getText());
      look.setLookupCache(getLookupCacheCheckBox().isSelected());
      look.setLookupOptional(getLookupOptionalCheckBox().isSelected());
      String insert = getLookupQueryInsertField().getText();
      look.setLookupInsertQuery(insert, getLookupQueryInsertKeyField().getText());
      look.setLookupFullCacheQuery(getLookupQueryFullCacheTextField().getText());
      if (look.getLookupFullCacheQuery() == null) {
        try {
          look.setMaximumCacheSize(Integer.valueOf(getLookupCacheCapacityTextField().getText()));
        } catch (NumberFormatException ex) {}
      }
      def.setLookupDefinition(look);
    }
    // Filter
    boolean filterNull = getFilterNullCheckBox().isSelected();
    boolean filterNotNull = getFilterNotNullCheckBox().isSelected();
    String[] filterValues = getFilterValueList().getData();
    if (filterNull) {
      filterValues = (String[]) CollectionUtil.extendArray(filterValues, "NULL");
    }
    if (filterNotNull) {
      filterValues = (String[]) CollectionUtil.extendArray(filterValues, "NOT_NULL");
    }
    def.setFilterOnValue(filterValues);
    String[] filterFields = getFilterFieldList().getData();
    def.setFilterOnField(filterFields);
    // Null definitions
    String[] nullDefs = getNullDefinitionList().getData();
    def.setNullDefinition(nullDefs);
    //
    String handerConfigs = getHandlerArgsTextField().getText();
    def.setHandler(((Class<?>) getFieldHandlerComboBox().getSelectedItem()).getName(), handerConfigs);

    if (nameChanged) {
      getColumnListModel().set(getColumnList().getSelectedIndex(), def);
      definition.updateField(currentName, def);
    } else {
      definition.updateField(def);
    }
    System.out.println("Column updated: " + currentName);
    validateDefinition();
    SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        refreshColumn();
      }
    });
  }

  private boolean validateDefinition() {
    try {
      LoadDefinitionValidator.validate(definition);
      getValidationMessageLabel().setText(null);
      return true;
    } catch (InvalidConfigurationException ex) {
      getValidationMessageLabel().setText(ex.getMessage());
    } catch (InvalidLookupQueryException ex) {
      getValidationMessageLabel().setText(ex.getMessage());
    }
    return false;
  }

  private JPanel getSourceFilePreview() {
    if (preview == null) {
      preview = new JPanel(new BorderLayout());
      preview.add(getTextFilePreviewer().getComponent(), BorderLayout.CENTER);
      JPanel trail = new JPanel(new FlowLayout(FlowLayout.TRAILING));
      trail.add(getSampleFileEncodingSelector());
      trail.add(new JButton(getSampleFileOpener()));
      trail.add(new JButton(getTestRunAction()));
      preview.add(trail, BorderLayout.PAGE_END);
    }
    return preview;
  }

  private TextFilePreviewer getTextFilePreviewer() {
    if (textFielPreviewer == null) {
      textFielPreviewer = new TextFilePreviewer();
    }
    return textFielPreviewer;
  }
  
  private JComboBox<String> getSampleFileEncodingSelector() {
    if (sampleFileEncodingSelector == null) {
      sampleFileEncodingSelector = new JComboBox<String>(new String[] {"UTF8", "UTF16", "ISO8859-1", "LATIN1"});
      sampleFileEncodingSelector.setEditable(true);
      sampleFileEncodingSelector.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(ActionEvent e) {
          try {
          getTextFilePreviewer().setFile(getTextFilePreviewer().getFile(), (String)getSampleFileEncodingSelector().getSelectedItem());
          } catch(RuntimeException ex) {
            System.out.println(ex.getMessage());
          }
        }
      });
    }
    return sampleFileEncodingSelector;
  }

  private FileOpenAction getSampleFileOpener() {
    if (sampleFileOpener == null) {
      sampleFileOpener = new FileOpenAction("Open Sample Input", null, LoadParseEditor.class.getName() + "-no-file", 20,
          new IFileOpener() {
            public void openUserFile(FileOpenAction a) {
              JFileChooser c = new JFileChooser();
              File f = getSampleFileOpener().showChooser(c, getSourceFilePreview());
              if (f == null || !f.isFile())
                return;
              openFile(f, a);
            }

            public void openFile(File f, FileOpenAction a) {
              getTextFilePreviewer().setFile(f, (String)getSampleFileEncodingSelector().getSelectedItem());
              a.addToRecentFiles(f);
              getTextFilePreviewer().setSpanOptions(new ISpanOptionProvider() {
                public JPopupMenu getSpanOptions(final Span s) {
                  JPopupMenu menu = new JPopupMenu();
                  List<FieldDefinition> allFields = definition.getAllFields();
                  if (s == null) {
                    menu.add(new JMenuItem());
                    return menu;
                  }
                  for (FieldDefinition d : allFields) {
                    int start = new Integer(d.getLocation().getStart());
                    int end = new Integer(d.getLocation().getEnd());
                    boolean surrounds = start <= s.getStart() && end >= s.getEnd();
                    boolean partlyWithin = MathUtil.between(start, s.getStart(), s.getEnd())
                        || MathUtil.between(end, s.getStart(), s.getEnd());
                    if (surrounds || partlyWithin) {
                      final FieldDefinition def = d;
                      JMenuItem mi = new JMenuItem("Select " + d.getFieldName() + " (" + d.getLocation().getStart() + "-"
                          + d.getLocation().getEnd() + ")");
                      mi.addActionListener(new ActionListener() {
                        public void actionPerformed(ActionEvent e) {
                          getColumnList().setSelectedValue(def, true);
                        }
                      });
                      menu.add(mi);
                    }
                  }
                  JMenuItem mi = new JMenuItem("Add field at " + s.getStart() + "-" + s.getEnd());
                  mi.addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                      Location loc = new Location(s.getStart() + "", s.getEnd() + "");
                      FieldDefinition def = new FieldDefinition("F", loc, false);
                      for (int i = 1; definition.getField(def.getFieldName()) != null; i++) {
                        def = new FieldDefinition("F_" + i, loc, false);
                      }
                      definition.updateField(def);
                      refresh();
                      getColumnList().setSelectedValue(def, true);
                    }
                  });
                  menu.add(mi);
                  return menu;
                }
              });
            }
          });
    }

    return sampleFileOpener;
  }

  private JComponent getBasicFieldConfigPanel() {
    TableLayout tl = new TableLayout(new double[] {5, 150, TableLayout.PREFERRED, TableLayout.PREFERRED,
        TableLayout.PREFERRED, 5, 5},
        new double[] {5, TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED,
            TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED,
            TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED, 5});
    tl.setVGap(5);
    tl.setHGap(2);
    JPanel p = new JPanel(tl);
    int row = 1;
    p.add(new JLabel("Name"), TablePos.pos(row, 1));
    p.add(getColumnNameTextField(), TablePos.pos(row, 2));
    row++;
    p.add(getIsKeyCheckBox(), TablePos.pos(row, 2));
    row++;
    p.add(getIsOptionalCheckBox(), TablePos.pos(row, 2));
    row++;
    p.add(getIsExternalCheckBox(), TablePos.pos(row, 2));
    // location
    TableLayout grid = TablePos.grid(3, 2);
    grid.setHGap(4);
    grid.setVGap(4);
    JPanel locationPane = new JPanel(grid);
    locationPane.setBorder(BorderFactory.createTitledBorder("Location"));
    locationPane.add(new JLabel(" start"), TablePos.pos(0, 0));
    locationPane.add(getLocationStartTextField(), TablePos.pos(0, 1));
    locationPane.add(new JLabel(" end"), TablePos.pos(1, 0));
    locationPane.add(getLocationEndTextField(), TablePos.pos(1, 1));
    locationPane.add(new JLabel(" length"), TablePos.pos(2, 0));
    locationPane.add(getLocationLengthTextField(), TablePos.pos(2, 1));
    p.add(locationPane, TablePos.pos(1, 3, row, 3));
    row++;
    p.add(new JLabel("Default value"), TablePos.pos(row, 1));
    p.add(getDefaultValueTextField(), TablePos.pos(row, 2, row, 3));
    // handler
    row++;
    p.add(getHandlerPanel(), TablePos.pos(row, 1, row + 1, 3));
    //
    //p.add(new JLabel("Field Remarks"), TablePos.pos(1, 5));
    //p.add(JScrollPaneUtil.wrap(getFieldRemarksTextField()), TablePos.pos(2, 5, 7, 5));
    return new JScrollPane(p);
  }

  private JTextComponent getFieldRemarksTextField() {
    if (fieldRemarksTextField == null) {
      fieldRemarksTextField = new JTextArea(10, 40);
      fieldRemarksTextField.setWrapStyleWord(true);
      fieldRemarksTextField.setLineWrap(true);
      getColumnGroup().add(fieldRemarksTextField, null);
    }
    return fieldRemarksTextField;
  }

  private JPanel getHandlerPanel() {
    TableLayout hLayout = new TableLayout();
    hLayout.setColumn(new double[] {TableLayout.PREFERRED, TableLayout.FILL, TableLayout.PREFERRED});
    hLayout.setRow(new double[] {TableLayout.PREFERRED, TableLayout.PREFERRED});
    hLayout.setVGap(5);
    hLayout.setHGap(2);
    JPanel handlerPane = new JPanel(hLayout);
    handlerPane.setBorder(BorderFactory.createTitledBorder("Field Handler"));
    handlerPane.add(new JLabel(" Class "), "0,0");
    handlerPane.add(getFieldHandlerComboBox(), "1,0,2,0");
    handlerPane.add(new JLabel(" Argument "), "0,1");
    handlerPane.add(getHandlerArgsTextField(), "1,1");
    handlerPane.add(getHandlerDocumentButton(), "2,1");
    return handlerPane;
  }

  private JComponent getFilterConfigPanel() {
    TableLayout tl = new TableLayout(
        new double[] {5, TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.FILL, 5}, new double[] {5,
            TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED,
            TableLayout.FILL, 5});
    tl.setVGap(1);
    tl.setHGap(1);
    JPanel p = new JPanel(tl);
    p.add(getFilterNullCheckBox(), "1,1");
    p.add(getFilterNotNullCheckBox(), "1,2");
    p.add(getFilterValueList(), "1,3,1,3");
    p.add(getFilterFieldList(), "2,3,2,3");
    return new JScrollPane(p);
  }

  private JComponent getLookupConfigPanel() {
    double[] columns = new double[] {TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED, 100, TableLayout.FILL, TableLayout.PREFERRED, 5};
    TableLayout tl = new TableLayout(columns, new double[] {5,
            TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED,
            TableLayout.PREFERRED, TableLayout.FILL, 5});
    tl.setVGap(4);
    tl.setHGap(4);
    JPanel p = new JPanel(tl);
    p.add(getLookupCacheCheckBox(), TablePos.pos(1, 0));
    JButton lookupTemplateButton = new JButton("Lookup Template");
    lookupTemplateButton.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        final FieldDefinition fd = getSelectedField();
        if (fd == null) {
          return;
        }
        String tableName = JOptionPane.showInputDialog("Enter lookup table name");
        if (tableName != null) {
          getLookupQueryField().setText("select ID from " + tableName + " where CODE = #" + fd.getFieldName() + "#");
          getLookupQueryFullCacheTextField().setText("select ID, CODE from " + tableName + "");
          getLookupQueryInsertField().setText("insert into " + tableName + " (ID, CODE, DESCRIPTION) values (#ID#, #"
            + fd.getFieldName() + "#, 'Created by Loading')");
          String sequenceName = tableName + "_ID";
          LoadingQueryWriter q = new LoadingQueryWriter(definition);
          getLookupQueryInsertKeyField().setText(q.getSequenceSelect(sequenceName));
          refreshColumn();
        }
      }
    });
    p.add(new JLabel("Maximum Cache Size "), TablePos.pos(1, 2));
    p.add(getLookupCacheCapacityTextField(), TablePos.pos(1, 3));
    p.add(lookupTemplateButton, TablePos.pos(1, 5));
    p.add(getLookupOptionalCheckBox(), TablePos.fullRow(2, tl));
    JScrollPane lookupQueryPane = JScrollPaneUtil.wrap(getLookupQueryField());
    lookupQueryPane.setBorder(BorderFactory.createTitledBorder("Lookup Query"));
    p.add(lookupQueryPane, TablePos.fullRow(3, tl));
    JScrollPane lookupQueryFullcachePane = JScrollPaneUtil.wrap(getLookupQueryFullCacheTextField());
    lookupQueryFullcachePane.setBorder(BorderFactory.createTitledBorder("Lookup Full Cache Query"));
    p.add(lookupQueryFullcachePane, TablePos.fullRow(4, tl));
    JPanel lookupQueryInsertPane = new JPanel(new BorderLayout());
    lookupQueryInsertPane.setBorder(BorderFactory.createTitledBorder("Lookup Insert Query"));
    lookupQueryInsertPane.add(getLookupQueryInsertField(), BorderLayout.CENTER);
    JPanel lookupQueryInsertKeyPane = new JPanel(new FlowLayout(FlowLayout.LEADING));
    lookupQueryInsertKeyPane.add(new JLabel("insert key : "));
    lookupQueryInsertKeyPane.add(getLookupQueryInsertKeyField());
    lookupQueryInsertPane.add(lookupQueryInsertKeyPane, BorderLayout.SOUTH);
    p.add(lookupQueryInsertPane, TablePos.fullRow(5, tl));
    return new JScrollPane(p);
  }

  private JTextField getLookupCacheCapacityTextField() {
    if (cacheCapacityTextField == null) {
      cacheCapacityTextField = new JTextField(8);
      cacheCapacityTextField.setHorizontalAlignment(JTextField.TRAILING);
      getColumnGroup().add(cacheCapacityTextField, "10000");
    }
    return cacheCapacityTextField;
  }

  private JComboBox getLoadModeComboBox() {
    if (loadModeComboBox == null) {
      loadModeComboBox = new JComboBox<>(new String[] {
          LoadDefinition.MIXED_MODE, LoadDefinition.INSERT_MODE, LoadDefinition.UPDATE_MODE});
      loadModeComboBox.addActionListener(getLoadModeComboBoxListener());
      loadingGroup.add(loadModeComboBox, LoadDefinition.MIXED_MODE);
      loadModeComboBox.setName(XMLConstantKey.XML_MODE_KEY);
      loadModeComboBox.setToolTipText(getToolTips(loadModeComboBox.getName()));
    }
    return loadModeComboBox;
  }

  private JComboBox<String> getLoadTypeComboBox() {
    if (loadTypeComboBox == null) {
      loadTypeComboBox = new JComboBox<>(new String[] {RecordDispenserFactory.FIXED_WIDTH,
          RecordDispenserFactory.DELIMITED, RecordDispenserFactory.XML});
      loadingGroup.add(loadTypeComboBox, RecordDispenserFactory.FIXED_WIDTH);
      loadTypeComboBox.setName(XMLConstantKey.XML_TYPE_KEY);
      loadTypeComboBox.setToolTipText(getToolTips(loadTypeComboBox.getName()));
      loadingGroup.add(loadTypeComboBox, RecordDispenserFactory.FIXED_WIDTH);
    }
    return loadTypeComboBox;
  }
  
  private JComboBox<String> getDelimiterTypeComboBox() {
    if (delimiterTypeComboBox == null) {
      delimiterTypeComboBox = new JComboBox<>(new String[] {"Delimiter", "Delimiter Codepoint"});
      loadingGroup.add(delimiterTypeComboBox, delimiterTypeComboBox.getItemAt(0));
    }
    return delimiterTypeComboBox;
  }
  private JTextField getDelimiterTextField() {
    if (delimiterTextField == null) {
      delimiterTextField = new JTextField(8);
      loadingGroup.add(delimiterTextField, "|");
    }
    return delimiterTextField;
  }


  private JTextField getTableNameTextField() {
    if (tableNameTextField == null) {
      tableNameTextField = new JTextField(30);
      loadingGroup.add(tableNameTextField, "PTR");
      tableNameTextField.setName(XMLConstantKey.XML_TABLE_KEY);
      tableNameTextField.setToolTipText(getToolTips(tableNameTextField.getName()));
    }
    return tableNameTextField;
  }

  private JCheckBox getFullcacheCheckBox() {
    if (fullcacheCheckBox == null) {
      fullcacheCheckBox = new JCheckBox("Full cache loading");
      loadingGroup.add(fullcacheCheckBox, false);
      fullcacheCheckBox.setName(XMLConstantKey.XML_FULL_CACHE_KEY);
      fullcacheCheckBox.setToolTipText(getToolTips(fullcacheCheckBox.getName()));
    }
    return fullcacheCheckBox;
  }

  private JCheckBox getVerifyFileKeyValuesUniqueCheckBox() {
    if (verifyFileKeyValuesUniqueCheckBox == null) {
      verifyFileKeyValuesUniqueCheckBox = new JCheckBox("Verify File Key Values Unique");
      loadingGroup.add(verifyFileKeyValuesUniqueCheckBox, false);
      verifyFileKeyValuesUniqueCheckBox.setName(XMLConstantKey.XML_VERIFY_UNIQUENESS_KEY);
      verifyFileKeyValuesUniqueCheckBox.setToolTipText(getToolTips(verifyFileKeyValuesUniqueCheckBox.getName()));
    }
    return verifyFileKeyValuesUniqueCheckBox;
  }

  private JCheckBox getPadlineCheckBox() {
    if (padlineCheckBox == null) {
      padlineCheckBox = new JCheckBox("Padline");
      loadingGroup.add(padlineCheckBox, true);
      padlineCheckBox.setName(XMLConstantKey.XML_PAD_LINE_KEY);
      padlineCheckBox.setToolTipText(getToolTips(padlineCheckBox.getName()));
    }
    return padlineCheckBox;
  }

  private JComboBox getFieldHandlerComboBox() {
    if (fieldHandlerComboBox == null) {
      ClasspathSearcher searcher = new ClasspathSearcher(false, null);
      Class<?>[] handlers = searcher.findSubclasses(IFieldTextHandler.class, null);
      List<Class<?>> handlerList = new ArrayList<Class<?>>();
      for (int i = 0; i < handlers.length; i++) {
        Class<?> handler = handlers[i];
        if (handler.isInterface()) {
          continue;
        }
        handlerList.add(handler);
      }
      Collections.sort(handlerList, new ClassComparator());
      fieldHandlerComboBox = new JComboBox(handlerList.toArray(new Class[0]));
      fieldHandlerComboBox.setRenderer(new DefaultListCellRenderer(){
        private static final long serialVersionUID = 1L;

        @Override
        public Component getListCellRendererComponent(JList list, Object value, int index, boolean isSelected,
            boolean cellHasFocus) {
          Class<?> c = (Class<?>) value;
          return super.getListCellRendererComponent(list, c.getSimpleName(), index, isSelected, cellHasFocus);
        }
        
      });
      fieldHandlerComboBox.addItemListener(new ItemListener() {
        public void itemStateChanged(ItemEvent e) {
          Class<?> name = (Class<?>) e.getItem();
          try {
            IFieldTextHandler handler = (IFieldTextHandler) Reflect
                .invokeConstructor(name, new Class[0], new Object[0]);
            String text = "BEHAVIOUR :\n" + handler.getBehaviourDocumentation().trim() + "\n\nCONFIGURATION :\n"
                + handler.getConfigurationDocumentation().trim();
            getHandlerDocumentDialog().setTitle(name.getName());
            getHandlerDocumentationTextArea().setText(text);
          } catch (ReflectionException ex) {
            ex.printStackTrace();
            getHandlerDocumentationTextArea().setText("Error occurred while retrieving documentation");
          }
        }
      });
      getColumnGroup().add(fieldHandlerComboBox, StringHandler.class.getName());
      String name = XMLConstantKey.XML_HANDLER_SUFFIX + "." + XMLConstantKey.XML_HANDLER_NAME;
      fieldHandlerComboBox.setName(name);
      fieldHandlerComboBox.setToolTipText(getToolTips(name));
    }
    return fieldHandlerComboBox;
  }

  private StringListEditor getFilterFieldList() {
    if (filterFieldList == null) {
      filterFieldList = new StringListEditor("Filter on field");
      getColumnGroup().add(filterFieldList, null);
      String name = XMLConstantKey.XML_FILTER_ON_SUFFIX + "." + XMLConstantKey.XML_FIELD_KEY;
      filterFieldList.setName(name);
      filterFieldList.setToolTipText(getToolTips(name));
    }
    return filterFieldList;
  }

  private StringListEditor getFilterValueList() {
    if (filterValueList == null) {
      filterValueList = new StringListEditor("Filter on value (Matching values are loaded)");
      getColumnGroup().add(filterValueList, null);
      String name = XMLConstantKey.XML_FILTER_ON_SUFFIX + "." + XMLConstantKey.XML_VALUE_KEY;
      filterValueList.setName(name);
      filterValueList.setToolTipText(getToolTips(name));
    }
    return filterValueList;
  }

  private JList<FieldDefinition> getColumnList() {
    if (columnList == null) {
      columnList = new JList<>(getColumnListModel());
      columnList.addMouseListener(new AbstractPopupMenuMouseListener() {
        
        @Override
        protected JPopupMenu getMenu() {
          JPopupMenu p = new JPopupMenu();
          final FieldDefinition selectedValue = getColumnList().getSelectedValue();
          if (selectedValue == null) {
            return null;
          }
          if (selectedValue.getLocation().getEnd().equals("0")) {
            return null;
          }
          if (definition.getLoadType().equals(RecordDispenserFactory.DELIMITED)) {
            Action a = new AbstractAction("Insert Column Before") {
              private static final long serialVersionUID = 1L;
              @Override
              public void actionPerformed(ActionEvent e) {
                Location startlocationForOffset = selectedValue.getLocation();
                int columnNo = Integer.parseInt(startlocationForOffset.getStart());
                FieldDefinition def = createNewGenericField(startlocationForOffset);
                for (FieldDefinition d : definition.getAllFields()) {
                  if (def.getFieldName().equals(d.getFieldName())) {
                    continue;
                  }
                  if (d.getLocation().getStart().equals("0")) {
                    continue;
                  }
                  int currentColumnNo;
                  try {
                    currentColumnNo = Integer.parseInt(d.getLocation().getStart());
                  } catch (NumberFormatException ex) {
                    currentColumnNo = 0;
                  }
                  if (currentColumnNo >= columnNo) {
                    String newColumn = String.valueOf(currentColumnNo + 1);
                    d.setLocation(new Location(newColumn, newColumn));
                    definition.updateField(d);
                  }
                }
                definition.putFieldBeforeInList(def, selectedValue.getFieldName());
                refresh();
              }
            };
            p.add(new JMenuItem(a));
          }
          return p;
        }
      });
      columnList.addListSelectionListener(new ListSelectionListener() {
        public void valueChanged(ListSelectionEvent e) {
          // revert to last valid field configuration
          refreshColumn();
        }
      });
     columnList.setCellRenderer(new DefaultListCellRenderer(){
      private static final long serialVersionUID = 1L;

      @Override
      public Component getListCellRendererComponent(JList list, Object value, int index, boolean isSelected,
          boolean cellHasFocus) {
        JLabel l = (JLabel) super.getListCellRendererComponent(list, value, index, isSelected, cellHasFocus);
        if (value instanceof FieldDefinition) {
          FieldDefinition d = (FieldDefinition) value;
          if (d.isExternal()) {
            l.setForeground(Color.GRAY);
          }
          Font f = l.getFont();
          if (d.isKey()) {
            l.setFont(f.deriveFont(Font.BOLD));
          } else if (d.isOptional()){
            l.setFont(f.deriveFont(Font.ITALIC));
          } else {
            l.setFont(f.deriveFont(Font.PLAIN));
          }
        }
        return l;
      }
      
     });
    }
    return columnList;
  }

  private StringListEditor getHeaderList() {
    if (headerList == null) {
      headerList = new StringListEditor("Header");
      loadingGroup.add(headerList, null);
      headerList.setName(XMLConstantKey.XML_HEADER_KEY);
      headerList.setToolTipText(getToolTips(headerList.getName()));
    }
    return headerList;
  }

  private StringListEditor getTrailerList() {
    if (trailerList == null) {
      trailerList = new StringListEditor("Trailer");
      loadingGroup.add(trailerList, null);
      trailerList.setName(XMLConstantKey.XML_TRAILER_KEY);
      trailerList.setToolTipText(getToolTips(trailerList.getName()));
    }
    return trailerList;
  }

  private JTextField getHandlerArgsTextField() {
    if (handlerArgsTextField == null) {
      handlerArgsTextField = new JTextField(20);
      getColumnGroup().add(handlerArgsTextField, null);
      String name = XMLConstantKey.XML_HANDLER_SUFFIX + "." + XMLConstantKey.XML_HANDLER_ARGS_SUFFIX;
      handlerArgsTextField.setName(name);
      handlerArgsTextField.setToolTipText(getToolTips(name));
    }
    return handlerArgsTextField;
  }

  private StringListEditor getNullDefinitionList() {
    if (nullDefinitionList == null) {
      nullDefinitionList = new StringListEditor("Null definition", TableLayout.PREFERRED);
      getColumnGroup().add(nullDefinitionList, null);
      nullDefinitionList.setName(XMLConstantKey.XML_NULL_DEFINITION_SUFFIX);
      nullDefinitionList.setToolTipText(getToolTips(nullDefinitionList.getName()));
    }
    return nullDefinitionList;
  }

  private JCheckBox getGenerateKeyCheckBox() {
    if (generateKeyCheckBox == null) {
      generateKeyCheckBox = new JCheckBox("Auto increment primary key");
      generateKeyCheckBox.addChangeListener(getGenerateKeyCheckBoxAction());
      loadingGroup.add(generateKeyCheckBox, false);
      generateKeyCheckBox.setName(AUTOINCREMENT);
      generateKeyCheckBox.setToolTipText(getToolTips(AUTOINCREMENT));
    }
    return generateKeyCheckBox;
  }

  private JTextField getGenerateKeyTextField() {
    if (generateKeyTextField == null) {
      generateKeyTextField = new JTextField(30);
      loadingGroup.add(generateKeySequenceTextField, "ID");
      String name = AUTOINCREMENT + "." + XMLConstantKey.XML_GENERATED_KEY_KEY;
      generateKeyTextField.setName(name);
      generateKeyTextField.setToolTipText(getToolTips(name));
    }
    return generateKeyTextField;
  }

  private JTextField getGenerateKeySequenceTextField() {
    if (generateKeySequenceTextField == null) {
      generateKeySequenceTextField = new JTextField(30);
      loadingGroup.add(generateKeySequenceTextField, null);
      String name = AUTOINCREMENT + "." + XMLConstantKey.XML_GENERATED_KEY_SEQ_NAME_KEY;
      generateKeySequenceTextField.setName(name);
      generateKeySequenceTextField.setToolTipText(getToolTips(name));
    }
    return generateKeySequenceTextField;
  }

  private JCheckBox getRefreshDataCheckBox() {
    if (refreshDataCheckBox == null) {
      refreshDataCheckBox = new JCheckBox("Refresh data");
      loadingGroup.add(refreshDataCheckBox, false);
      refreshDataCheckBox.setName(XMLConstantKey.XML_REFRESH_DATA_KEY);
      refreshDataCheckBox.setToolTipText(getToolTips(refreshDataCheckBox.getName()));
      refreshDataCheckBox.addChangeListener(new ChangeListener() {
        public void stateChanged(ChangeEvent e) {
          getRefreshQueryTextField().setEnabled(getRefreshDataCheckBox().isSelected());
        }
      });
    }
    return refreshDataCheckBox;
  }

  private JTextArea getRefreshQueryTextField() {
    if (refreshQueryTextField == null) {
      refreshQueryTextField = new JTextArea();
      loadingGroup.add(refreshQueryTextField, null);
      String name = XMLConstantKey.XML_REFRESH_DATA_KEY + "." + XMLConstantKey.XML_QUERY_KEY;
      refreshQueryTextField.setName(name);
      refreshQueryTextField.setToolTipText(getToolTips(name));
    }
    return refreshQueryTextField;
  }
  
  private JTextArea getGlobalFilterLookupTextField() {
    if (globalFilterLookupTextField == null) {
      globalFilterLookupTextField = new JTextArea();
      loadingGroup.add(globalFilterLookupTextField, null);
      String name = XMLConstantKey.FILTERS;
      globalFilterLookupTextField.setName(name);
      globalFilterLookupTextField.setToolTipText(getToolTips(name));
    }
    return globalFilterLookupTextField;
  }
  private JTextArea getQueryWhereSelectTextField() {
    if (queryWhereSelectTextField == null) {
      queryWhereSelectTextField = new JTextArea();
      loadingGroup.add(queryWhereSelectTextField, null);
      String name = XMLConstantKey.XML_QUERY_WHERE_KEY + "." + XMLConstantKey.XML_SELECT_KEY;
      queryWhereSelectTextField.setName(name);
      queryWhereSelectTextField.setToolTipText(getToolTips(name));
    }
    return queryWhereSelectTextField;
  }

  private JTextArea getQueryWhereUpdateTextField() {
    if (queryWhereUpdateTextField == null) {
      queryWhereUpdateTextField = new JTextArea();
      loadingGroup.add(queryWhereUpdateTextField, null);
      String name = XMLConstantKey.XML_QUERY_WHERE_KEY + "." + XMLConstantKey.XML_UPDATE_KEY;
      queryWhereUpdateTextField.setName(name);
      queryWhereUpdateTextField.setToolTipText(getToolTips(name));
    }
    return queryWhereUpdateTextField;
  }

  private JTextField getColumnNameTextField() {
    if (columnNameTextField == null) {
      columnNameTextField = new JTextField(20);
      getColumnGroup().add(columnNameTextField, null);
      String name = XMLConstantKey.XML_FIELD_KEY + "." + XMLConstantKey.XML_FIELD_NAME;
      columnNameTextField.setName(name);
      columnNameTextField.setToolTipText(getToolTips(name));
    }
    return columnNameTextField;
  }

  private JTextField getLocationStartTextField() {
    if (locationStartTextField == null) {
      locationStartTextField = new JTextField(5);
      locationStartTextField.setHorizontalAlignment(JTextField.TRAILING);
      getColumnGroup().add(locationStartTextField, "0");
      String name = XMLConstantKey.XML_LOCATION_SUFFIX + "." + XMLConstantKey.XML_LOCATION_START;
      locationStartTextField.setName(name);
      locationStartTextField.setToolTipText(getToolTips(name));
    }
    return locationStartTextField;
  }

  private JTextField getLocationEndTextField() {
    if (locationEndTextField == null) {
      locationEndTextField = new JTextField(5);
      locationEndTextField.setHorizontalAlignment(JTextField.TRAILING);
      getColumnGroup().add(locationEndTextField, "0");
      String name = XMLConstantKey.XML_LOCATION_SUFFIX + "." + XMLConstantKey.XML_LOCATION_END;
      locationEndTextField.setName(name);
      locationEndTextField.setToolTipText(getToolTips(name));
    }
    return locationEndTextField;
  }
  private JTextField getLocationLengthTextField() {
    if (locationLengthTextField == null) {
      locationLengthTextField = new JTextField(5);
      locationLengthTextField.setHorizontalAlignment(JTextField.TRAILING);
      locationLengthTextField.setEditable(false);
      locationLengthTextField.setEnabled(false);
    }
    return locationLengthTextField;
  }

  private JCheckBox getIsKeyCheckBox() {
    if (isKeyCheckBox == null) {
      isKeyCheckBox = new JCheckBox("isKey");
      getColumnGroup().add(isKeyCheckBox, false);
      isKeyCheckBox.setName(XMLConstantKey.XML_IS_KEY_SUFFIX);
      isKeyCheckBox.setToolTipText(getToolTips(XMLConstantKey.XML_IS_KEY_SUFFIX));
    }
    return isKeyCheckBox;
  }

  private JCheckBox getIsOptionalCheckBox() {
    if (isOptionalCheckBox == null) {
      isOptionalCheckBox = new JCheckBox("isOptional");
      getColumnGroup().add(isOptionalCheckBox, false);
      isOptionalCheckBox.setName(XMLConstantKey.XML_IS_OPTIONAL_SUFFIX);
      isOptionalCheckBox.setToolTipText(getToolTips(XMLConstantKey.XML_IS_OPTIONAL_SUFFIX));
    }
    return isOptionalCheckBox;
  }

  private JCheckBox getIsExternalCheckBox() {
    if (isExternalCheckBox == null) {
      isExternalCheckBox = new JCheckBox("isExternal");
      getColumnGroup().add(isExternalCheckBox, false);
      isExternalCheckBox.setName(XMLConstantKey.XML_IS_EXTERNAL_SUFFIX);
      isExternalCheckBox.setToolTipText(getToolTips(XMLConstantKey.XML_IS_EXTERNAL_SUFFIX));
    }
    return isExternalCheckBox;
  }

  private JTextField getDefaultValueTextField() {
    if (defaultValueTextField == null) {
      defaultValueTextField = new JTextField(40);
      getColumnGroup().add(defaultValueTextField, null);
      defaultValueTextField.setName(XMLConstantKey.XML_DEFAULT_SUFFIX);
      defaultValueTextField.setToolTipText(getToolTips(XMLConstantKey.XML_DEFAULT_SUFFIX));
    }
    return defaultValueTextField;
  }

  private JTextArea getLookupQueryField() {
    if (lookupQueryTextField == null) {
      lookupQueryTextField = new JTextArea(4, LOOKUP_TEXT_COLUMNS);
      getColumnGroup().add(lookupQueryTextField, null);
      String name = XMLConstantKey.XML_LOOKUP_KEY + "." + XMLConstantKey.XML_QUERY_KEY;
      lookupQueryTextField.setName(name);
      lookupQueryTextField.setToolTipText(getToolTips(name));
    }
    return lookupQueryTextField;
  }

  private JTextArea getLookupQueryInsertField() {
    if (lookupQueryInsertTextField == null) {
      lookupQueryInsertTextField = new JTextArea(4, LOOKUP_TEXT_COLUMNS);
      getColumnGroup().add(lookupQueryInsertTextField, null);
      String name = XMLConstantKey.XML_LOOKUP_KEY + "." + XMLConstantKey.XML_INSERT_KEY;
      lookupQueryInsertTextField.setName(name);
      lookupQueryInsertTextField.setToolTipText(getToolTips(name));
    }
    return lookupQueryInsertTextField;
  }

  private JTextArea getLookupQueryInsertKeyField() {
    if (lookupQueryInsertKeyTextField == null) {
      lookupQueryInsertKeyTextField = new JTextArea(1, LOOKUP_TEXT_COLUMNS);
      getColumnGroup().add(lookupQueryInsertKeyTextField, null);
      String name = XMLConstantKey.XML_LOOKUP_KEY + "." + XMLConstantKey.XML_INSERT_KEY_KEY;
      lookupQueryInsertKeyTextField.setName(name);
      lookupQueryInsertKeyTextField.setToolTipText(getToolTips(name));
    }
    return lookupQueryInsertKeyTextField;
  }

  private JCheckBox getLookupCacheCheckBox() {
    if (lookupCacheCheckBox == null) {
      lookupCacheCheckBox = new JCheckBox("Cache lookup value");
      getColumnGroup().add(lookupCacheCheckBox, true);
      String name = XMLConstantKey.XML_LOOKUP_KEY + "." + XMLConstantKey.XML_CACHE;
      lookupCacheCheckBox.setName(name);
      lookupCacheCheckBox.setToolTipText(getToolTips(name));
    }
    return lookupCacheCheckBox;
  }

  private JTextArea getLookupQueryFullCacheTextField() {
    if (lookupQueryFullCacheTextField == null) {
      lookupQueryFullCacheTextField = new JTextArea(4, LOOKUP_TEXT_COLUMNS);
      getColumnGroup().add(lookupQueryFullCacheTextField, null);
      String name = XMLConstantKey.XML_LOOKUP_KEY + "." + XMLConstantKey.XML_FULL_CACHE_KEY;
      lookupQueryFullCacheTextField.setName(name);
      lookupQueryFullCacheTextField.setToolTipText(getToolTips(name));
    }
    return lookupQueryFullCacheTextField;
  }

  private JCheckBox getLookupOptionalCheckBox() {
    if (lookupOptionalCheckBox == null) {
      lookupOptionalCheckBox = new JCheckBox("Accept null lookup value");
      getColumnGroup().add(lookupOptionalCheckBox, false);
      String name = XMLConstantKey.XML_LOOKUP_KEY + "." + XMLConstantKey.XML_IS_OPTIONAL_SUFFIX;
      lookupOptionalCheckBox.setName(name);
      lookupOptionalCheckBox.setToolTipText(getToolTips(name));
    }
    return lookupOptionalCheckBox;
  }

  private JCheckBox getFilterNullCheckBox() {
    if (filterNullCheckBox == null) {
      filterNullCheckBox = new JCheckBox("Filter null value");
      getColumnGroup().add(filterNullCheckBox, false);
      String name = XMLConstantKey.XML_LOOKUP_KEY + ".filternull";
      filterNullCheckBox.setName(name);
      filterNullCheckBox.setToolTipText(getToolTips(name));
    }
    return filterNullCheckBox;
  }

  private JCheckBox getFilterNotNullCheckBox() {
    if (filterNotNullCheckBox == null) {
      filterNotNullCheckBox = new JCheckBox("Filter not null value");
      getColumnGroup().add(filterNotNullCheckBox, false);
      String name = XMLConstantKey.XML_LOOKUP_KEY + ".filternotnull";
      filterNotNullCheckBox.setName(name);
      filterNotNullCheckBox.setToolTipText(getToolTips(name));
    }
    return filterNotNullCheckBox;
  }

  private JComponent getAddRemoveColumnEditor() {
    double[] columns = new double[] {5, TableLayout.FILL, 5};
    JPanel p = new JPanel(new TableLayout(
        columns, new double[] {5, TableLayout.FILL,
        TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED, TableLayout.PREFERRED, 5}));
    p.add(new JScrollPane(getColumnList()), "1,1");
    JButton addColumnButton = new JButton(getAddColumnAction());
    p.add(addColumnButton, "1,2");
    addColumnButton.setToolTipText(getToolTips(XMLConstantKey.XML_FIELD_KEY + ".add"));
    JButton removeColumnButton = new JButton(getRemoveColumnAction());
    p.add(removeColumnButton, "1,3");
    removeColumnButton.setToolTipText(getToolTips(XMLConstantKey.XML_FIELD_KEY + ".remove"));
    JButton upColumnButton = new JButton(getUpColumnAction());
    p.add(upColumnButton, "1,4");
    upColumnButton.setToolTipText(getToolTips(XMLConstantKey.XML_FIELD_KEY + ".up"));
    return p;
  }
  private Action getUpColumnAction() {
    return new AbstractAction("Up", Images.loadClasspathIcon("images/up.gif")) {
      private static final long serialVersionUID = 1L;
      public void actionPerformed(ActionEvent e) {
        if (getSelectedField() == null) {
          return;
        }
        FieldDefinition selected = getSelectedField();
        definition.moveFieldUp(selected.getFieldName());
        refresh();
        getColumnList().setSelectedValue(selected, true);
      }
    };
  }

  private Action getAddColumnAction() {
    return new AbstractAction("Add", Images.loadClasspathIcon("images/add16.gif")) {
      private static final long serialVersionUID = 1L;

      public void actionPerformed(ActionEvent e) {
        FieldDefinition def = createNewGenericField(null);
        refresh();
        getColumnList().setSelectedValue(def, true);
      }
    };
  }

  private Action getRemoveColumnAction() {
    return new AbstractAction("Remove", Images.loadClasspathIcon("images/delete16.gif")) {
      private static final long serialVersionUID = 1L;

      public void actionPerformed(ActionEvent e) {
        String name = getSelectedField().getFieldName();
        definition.removeField(name);
        refresh();
      }
    };
  }
  private FieldDefinition getSelectedField() {
    return (FieldDefinition) getColumnList().getSelectedValue();
  }


  private boolean loading = false;
  private AbstractAction exportAction;

  private DBConfigurator getDBConfigurator() {
    if (dbConfigurator == null) {
      dbConfigurator = new DBConfigurator(null);
    }
    return dbConfigurator;
  }

  private Action getTestRunAction() {
    return new AbstractAction("Test Run..") {
      /**
			 * 
			 */
      private static final long serialVersionUID = 1L;

      public void actionPerformed(ActionEvent e) {
        if (loading == true)
          return;
        if (getTextFilePreviewer().getText() == null) {
          JOptionPane.showMessageDialog(getToolbar(), "Sample data not loaded.");
          return;
        }
        try {
          int option = getDBConfigurator().configure();
          if (option != JOptionPane.OK_OPTION)
            return;
        } catch (IOException e2) {
          throw new RuntimeException(e2);
        }
        Properties connectionProps = dbConfigurator.getConnectionProperties();
        loading = true;
        String loadName = definition.getDestTable();
        SampleDataLoader loader = null;
        try {
          loader = new SampleDataLoader(loadName);
          getFrame().setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));
          loader.configureLoad(definition);
          File file = File.createTempFile(loadName, ".txt");
          FileWriter writer = new FileWriter(file);
          writer.write(getTextFilePreviewer().getText());
          writer.close();

          JTextArea log = new JTextArea();
          Logger logger = Logger.getRootLogger();
          @SuppressWarnings("unchecked")
          Enumeration<Appender> enu = logger.getAllAppenders();
          while (enu.hasMoreElements()) {
            Appender a = enu.nextElement();
            if (a instanceof FileAppender) {
              FileAppender fa = (FileAppender) a;
              fa.setWriter(new OutputStreamWriter(new JTextAreaPrintStream(log)));
            }
          }

          LoadingErrorList error = loader.loadSample(file.getAbsolutePath(), connectionProps);
          popupLoadResult(error, log, loader);
        } catch (Exception e1) {
          JPanel errorPane = new JPanel(new BorderLayout());
          JLabel errorMsg = new JLabel("<html><p>" + e1.getMessage() + "</p></html>",
              UIManager.getIcon("OptionPane.errorIcon"), JLabel.LEADING);
          errorMsg.setBorder(BorderFactory.createEmptyBorder(15, 15, 25, 15));
          errorPane.add(errorMsg, BorderLayout.NORTH);
          JTabbedPane eTabPane = new JTabbedPane();
          JTextArea eLog = new JTextArea();
          e1.printStackTrace(new JTextAreaPrintStream(eLog));
          eTabPane.addTab("Details", new JScrollPane(eLog));
          if (loader != null && loader.getMapFileDocument() != null) {
            String sqlMap = DocumentRenderer.transform(loader.getMapFileDocument(), "");
            JTextArea sqlTextArea = new JTextArea(sqlMap);
            eTabPane.addTab("SQL map", new JScrollPane(sqlTextArea));
          }
          errorPane.add(eTabPane, BorderLayout.CENTER);
          displayDialog("Loading Failed", errorPane);
          /*
           * JOptionPane.showMessageDialog(getToolbar(), pane, "ERROR",
           * JOptionPane.ERROR_MESSAGE); loading = false;
           * getFrame().setCursor(Cursor
           * .getPredefinedCursor(Cursor.DEFAULT_CURSOR));
           */
        }
      }
    };
  }

  private void popupLoadResult(LoadingErrorList error, JTextArea log, SampleDataLoader loader) throws IOException,
      ClassNotFoundException {
    JTabbedPane tab = new JTabbedPane();
    JTextArea summary = new JTextArea();
    summary.setEditable(false);
    ErrorSummary total = error.getErrorSummary();
    long lines = total.getAllLines();
    long all = total.getParsingError() + total.getFiltered() + total.getDataIntegrityError() + total.getUnknownError()
        + total.getDbRequestTooLong() + total.getCommitFailed();
    summary.append(total.getParsingError() + " of " + lines + " failed due to parsing errors\n");
    summary.append(total.getFiltered() + " of " + lines + " rejected due to not meeting filter conditions\n");
    summary.append(total.getDataIntegrityError() + " of " + lines + " failed due to data integrity errors\n");
    summary.append(total.getUnknownError() + " of " + lines + " failed due to unknown errors\n");
    summary.append(total.getDbRequestTooLong() + " of " + lines
        + " failed due to database taking too long to process request\n");
    summary.append(total.getCommitFailed() + " of " + lines + " failed due to failed database commits\n");
    summary.append(all + " of " + lines + " total failures and rejects");
    tab.addTab("Summary", summary);

    // Parsed data
    JTabbedPane loadedDataPane = new JTabbedPane();
    JTable allTable = new JTable(new ListMapTableModel(loader.getAllData()));
    allTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
    JScrollPane aTablePane = new JScrollPane(allTable);
    loadedDataPane.addTab("All Parsed Data", aTablePane);
    JTable insertTable = new JTable(new ListMapTableModel(loader.getInsertedData()));
    insertTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
    JScrollPane iTablePane = new JScrollPane(insertTable);
    loadedDataPane.addTab("Inserted", iTablePane);
    JTable updateTable = new JTable(new ListMapTableModel(loader.getUpdatedData()));
    updateTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
    JScrollPane uTablePane = new JScrollPane(updateTable);
    loadedDataPane.addTab("Updated", uTablePane);
    JTable failTable = new JTable(new ListMapTableModel(loader.getFailedData()));
    failTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
    JScrollPane fTablePane = new JScrollPane(failTable);
    loadedDataPane.addTab("Failed", fTablePane);
    tab.add("Parsed Data", loadedDataPane);

    // logs
    JTabbedPane logPane = new JTabbedPane();
    List<ErrorSummary> errorList = error.getErrorList();
    JTextArea exception = new JTextArea();
    JTextArea badline = new JTextArea();
    JTextArea filteredLine = new JTextArea();
    for (int i = 0; i < errorList.size(); i++) {
      ErrorSummary es = errorList.get(i);
      List<Exception> stacktraces = es.getExceptions();
      for (int s = 0; stacktraces != null && s < stacktraces.size(); s++) {
        Exception e = stacktraces.get(s);
        e.printStackTrace(new JTextAreaPrintStream(exception));
      }
      List<String> failed = es.getFailedLines();
      for (int f = 0; failed != null && f < failed.size(); f++) {
        badline.append(failed.get(f) + "\n");
      }
      List<String> reject = es.getFilteredLines();
      for (int r = 0; reject != null && r < reject.size(); r++) {
        badline.append(reject.get(r) + "\n");
      }
    }
    logPane.addTab("Loading Log", new JScrollPane(log));
    logPane.addTab("Stacktrace", new JScrollPane(exception));
    logPane.addTab("Failed Lines", new JScrollPane(badline));
    logPane.addTab("Filtered Lines", new JScrollPane(filteredLine));
    tab.add("Logs", logPane);

    if (loader.getMapFileDocument() != null) {
      String sqlMap = DocumentRenderer.transform(loader.getMapFileDocument(), "");
      JTextArea sqlTextArea = new JTextArea(sqlMap);
      tab.addTab("SQL map", new JScrollPane(sqlTextArea));
    }

    displayDialog(definition.getDestTable(), tab);

  }

  private void displayDialog(String title, JComponent com) {
    final JDialog result = new JDialog(getFrame(), title);
    result.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
    result.addWindowListener(new WindowListener() {
      public void windowActivated(WindowEvent e) {
      }

      public void windowClosed(WindowEvent e) {
        loading = false;
      }

      public void windowClosing(WindowEvent e) {
      }

      public void windowDeactivated(WindowEvent e) {
      }

      public void windowDeiconified(WindowEvent e) {
      }

      public void windowIconified(WindowEvent e) {
      }

      public void windowOpened(WindowEvent e) {
      }
    });
    result.getContentPane().add(com);
    result.setSize((int) (getFrame().getWidth() * 0.7), (int) (getFrame().getHeight() * 0.7));
    result.setLocationRelativeTo(null);
    SwingUtilities.invokeLater(new Runnable() {
      public void run() {
        getFrame().setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        result.setVisible(true);
      }
    });
  }

  private Action getNewDefinitionAction() {
    return new AbstractAction("New", Images.loadClasspathIcon("images/new.gif")) {
      private static final long serialVersionUID = 1L;

      public void actionPerformed(ActionEvent e) {
        currentFile = null;
        getFrame().setTitle(null);
        definition = new LoadDefinition();
        definition.setDestTable("PTR");
        definition.updateField(new FieldDefinition("F", new Location(null, null), false));
        refresh();
        getTextFilePreviewer().clear();
        getSampleFileOpener().setPreferencesName(LoadParseEditor.class.getName());
      }
    };
  }

  private Action getDefinitionSaveAction() {
    return new AbstractAction("Save", Images.loadClasspathIcon("images/save16.gif")) {
      private static final long serialVersionUID = 1L;

      public void actionPerformed(ActionEvent e) {
        // Post-actions do not update interactively.
        Map<String, List<String>> data = getPostActionStatementsEditor().getData();
        definition.setPostInsertInsertQueries(data.get(XMLConstantKey.XML_INSERT_INSERTION_KEY).toArray(new String[0]));
        definition.setPostInsertUpdateQueries(data.get(XMLConstantKey.XML_INSERT_UPDATE_KEY).toArray(new String[0]));
        definition.setPostUpdateInsertQueries(data.get(XMLConstantKey.XML_UPDATE_INSERTION_KEY).toArray(new String[0]));
        definition.setPostUpdateUpdateQueries(data.get(XMLConstantKey.XML_UPDATE_UPDATE_KEY).toArray(new String[0]));
        if (!validateDefinition()) {
          int opt = JOptionPane.showConfirmDialog(getToolbar(),
              "This is an invalid definition that could cause loading to fail: \n\n " + getValidationMessageLabel().getText()
                  + "\n\nContinue?", "WARNING", JOptionPane.OK_CANCEL_OPTION, JOptionPane.WARNING_MESSAGE);
          if (opt == JOptionPane.CANCEL_OPTION)
            return;
        }
        if (currentFile == null) {
          JFileChooser fc = new JFileChooser();
          int option = fc.showSaveDialog(getToolbar());
          if (option == JFileChooser.APPROVE_OPTION) {
            currentFile = fc.getSelectedFile();
            getFrame().setTitle(currentFile.getAbsolutePath());
          } else {
            return;
          }
        }
        try {
          Document doc = definition.asXML();
          String text = DocumentRenderer.transform(doc, "");
          FileOutputStream fos = new FileOutputStream(currentFile);
          Writer writer = new BufferedWriter(new OutputStreamWriter(new DataOutputStream(fos), "UTF-8"));
          writer.write(text);
          writer.close();
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }
    };
  }

  private Action getDefinitionOpenAction() {
    if (openAction == null) {
      openAction = new FileOpenAction(getClass().getName() + "-file", 15, new IFileOpener() {
        public void openFile(File f, FileOpenAction a) {

          LoadDefinition parse = new LoadDefinition();
          if (f.getAbsolutePath().toLowerCase().endsWith("properties")) {
            try {
              String xmlFile = new PropertiesToXMLParser(f.getAbsolutePath()).getXMLFileName();
              File file = new File(xmlFile);
              parse = V2LoadDefinitionParser.parse(DocumentLoader.loadDocument(file), file.getAbsolutePath());
            } catch (Exception e) {
              JOptionPane.showMessageDialog(getToolbar(), "Failed to load and convert \n" + f.getAbsolutePath()
                  + ". Please convert the properties to XML manually.");
            }
          } else {
            parse = V2LoadDefinitionParser.parse(DocumentLoader.loadDocument(f), f.getAbsolutePath());
          }
          definition = parse;
          validateDefinition();
          currentFile = f;
          getFrame().setTitle(currentFile.getAbsolutePath());
          a.addToRecentFiles(f);
          getSampleFileOpener().setPreferencesName(getClass().getName() + currentFile.getName());
          refresh();

        }

        public void openUserFile(FileOpenAction a) {
          FileSelector fs = new FileSelector(null, "Open", null, new String[] {"xml", "properties"});
          JFileChooser ch = fs.buildChooser();
          File file = openAction.showChooser(ch, getToolbar());
          if (file == null || !file.isFile()) {
            return;
          }
          openFile(file, a);
        }
      });
    }
    return openAction;
  }
  private Action getDefinitionExportAction() {
    if (exportAction == null) {
      exportAction = new AbstractAction("Export", new Images().sizeImage(Images.loadClasspathIcon("export.png"), 16)) {
        private static final long serialVersionUID = 1L;
        @Override
        public void actionPerformed(ActionEvent e) {
          try {
            String exportText = new ClientTableConfigurationExporter().export(definition);
            FileUtil.writeFile(new File(currentFile.getParentFile(), currentFile.getName() + ".tbl"), exportText, "UTF8");
          } catch (IOException e1) {
            e1.printStackTrace();
          } catch (ReflectionException e1) {
            e1.printStackTrace();
          }
        }
      };
    }
    return exportAction;
  }

  private ChangeListener getGenerateKeyCheckBoxAction() {
    return new ChangeListener() {
      public void stateChanged(ChangeEvent e) {
        getGenerateKeyTextField().setEnabled(getGenerateKeyCheckBox().isSelected());
        getGenerateKeySequenceTextField().setEnabled(getGenerateKeyCheckBox().isSelected());
      }
    };
  }

  private ActionListener getLoadModeComboBoxListener() {
    return new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        Object mode = getLoadModeComboBox().getSelectedItem();
        getFullcacheCheckBox().setEnabled(true);
        getGenerateKeyCheckBox().setEnabled(true);
        getGenerateKeyCheckBox().setSelected(true);
        if (mode.equals(LoadDefinition.INSERT_MODE)) {
          getFullcacheCheckBox().setSelected(false);
          getFullcacheCheckBox().setEnabled(false);
          return;
        }
        if (mode.equals(LoadDefinition.UPDATE_MODE)) {
          getGenerateKeyCheckBox().setSelected(false);
          getGenerateKeyCheckBox().setEnabled(false);
          return;
        }
      }
    };
  }

  private DefaultListModel<FieldDefinition> getColumnListModel() {
    if (columnListModel == null) {
      columnListModel = new DefaultListModel<FieldDefinition>();
    }
    return columnListModel;
  }

  private void refresh() {
    getLoadingGroup().setIgnoreEvents(true);
    JTextUtil.setText(definition.getDocumentation(), getDocumentationText());
    JTextUtil.setText(definition.getDestTable(), getTableNameTextField());
    getPadlineCheckBox().setSelected(definition.isPadLine());
    getHeaderList().setData(Arrays.asList(definition.getHeader()));
    getTrailerList().setData(Arrays.asList(definition.getTrailer()));
    getLoadModeComboBox().setSelectedItem(definition.getUpdateMode());
    getLoadTypeComboBox().setSelectedItem(definition.getLoadType());
    //
    getDelimiterTypeComboBox().setEnabled(definition.isDelimited());
    getDelimiterTextField().setEnabled(definition.isDelimited());
    String delimiter = definition.getDelimiter();
    if (definition.getAsciiCodePointDelimiter() != null) {
      getDelimiterTypeComboBox().setSelectedIndex(1);
      JTextUtil.setText(definition.getAsciiCodePointDelimiter().toString(), getDelimiterTextField());
    } else {
      getDelimiterTypeComboBox().setSelectedIndex(0);
      JTextUtil.setText(delimiter, getDelimiterTextField());
    }
    //
    getFullcacheCheckBox().setSelected(definition.isFullCache());
    getVerifyFileKeyValuesUniqueCheckBox().setSelected(definition.isKeyVerificationScan());
    getGenerateKeyCheckBox().setSelected(definition.isGenerateKey());
    JTextUtil.setText(definition.getGeneratedKeyColumn(), getGenerateKeyTextField());
    JTextUtil.setText(definition.getGenerateKeySeq(), getGenerateKeySequenceTextField());
    getRefreshDataCheckBox().setSelected(definition.isRefreshData());
    JTextUtil.setText(definition.getRefreshDataQuery(), getRefreshQueryTextField());
    JTextUtil.setText(definition.getSelectWhereClause(), getQueryWhereSelectTextField());
    JTextUtil.setText(definition.getUpdateWhereClause(), getQueryWhereUpdateTextField());
    getPostActionStatementsEditor().setData(definition.getPostInsertInsertQueries(), definition.getPostInsertUpdateQueries(),
        definition.getPostUpdateInsertQueries(), definition.getPostUpdateUpdateQueries());
    // Format supports multiple filters but editor only does one for now.
      GlobalFilter[] globalFilters = definition.getGlobalFilters();
      if (globalFilters.length == 0) {
        JTextUtil.setText("", getGlobalFilterLookupTextField());
      } else {
        LookupDefinition lookup = globalFilters[0].getLookup();
        JTextUtil.setText(lookup.getLookupQuery(), getGlobalFilterLookupTextField());
      }
    //
    getLoadingGroup().setIgnoreEvents(false);

    int selectedFieldIndex = getColumnList().getSelectedIndex();

    getColumnListModel().removeAllElements();
    List<FieldDefinition> fields = definition.getAllFields();
    for (int i = 0; i < fields.size(); i++) {
      getColumnListModel().addElement(fields.get(i));
    }
    getColumnList().setSelectedIndex(selectedFieldIndex);
    updateNativeLoaderPane();
  }

  private void updateNativeLoaderPane() {
    FixedWidthFileOutput o = new FixedWidthFileOutput("Test", definition);
    try {
      getIntermediateTableDefinitionTextField().setText(o.getIntermediateTableDefinition());
      getIntermediateTableDefinitionTextField().setCaretPosition(0);
      getIntermediateLoaderCommandTextField().setText(o.getNativeLoadCommand(false));
      getIntermediateLoaderCommandTextField().setCaretPosition(0);
      getGeneratedStoredProcedureTextField().setText(o.getStoredProcedure());
      getGeneratedStoredProcedureTextField().setCaretPosition(0);
    } catch (Throwable t) {
      getIntermediateTableDefinitionTextField().setText("");
      getIntermediateLoaderCommandTextField().setText("");
      getGeneratedStoredProcedureTextField().setText("");
    }
  }

  private void refreshColumn() {
    getColumnGroup().setIgnoreEvents(true);
    try {
      int selectedIndex = getColumnList().getSelectedIndex();
      if (selectedIndex == -1) {
        getColumnGroup().setEnabled(false);
        setColumnLookup(null);
        getTextFilePreviewer().setSpan(null, null);
        return;
      }
      getColumnGroup().setEnabled(true);
      String fieldName = ((FieldDefinition) getColumnListModel().get(selectedIndex)).getFieldName();
      final FieldDefinition fd = definition.getField(fieldName);
      getColumnListModel().set(selectedIndex, fd);
      JTextUtil.setText(fd.getFieldName(), getColumnNameTextField());
      String start = fd.getLocation().getStart();
      String end = fd.getLocation().getEnd();
      if (definition.getLoadType().equals(RecordDispenserFactory.FIXED_WIDTH) || definition.getLoadType().equals(RecordDispenserFactory.DELIMITED)) {
        start = Strings.leftTrim(start, "0");
        end = Strings.leftTrim(end, "0");
        if (start.equals("")) {
          start = "0";
        }
        if (end.equals("")) {
          end = "0";
        }
      }
      JTextUtil.setText(start, getLocationStartTextField());
      JTextUtil.setText(end, getLocationEndTextField());
      trySettingFieldLength(start, end);
      JTextUtil.setText(fd.getRemarks(), getFieldRemarksTextField());
      SwingUtilities.invokeLater(new Runnable() {
        @Override
        public void run() {
          try {
            Span columnSpan = new Span(new Integer(fd.getLocation().getStart()), new Integer(fd.getLocation().getEnd()));
            String delimiter = null;
            if (definition.getLoadType().equals(RecordDispenserFactory.DELIMITED)) {
              delimiter = definition.getDelimiter();
            }
            getTextFilePreviewer().setSpan(columnSpan, delimiter);
          } catch (NumberFormatException e) {
            // do nothing, validator will send out the warning.
          }
        }
      });
      
      getIsKeyCheckBox().setSelected(fd.isKey());
      if (fd.isKey()) {
        getIsOptionalCheckBox().setEnabled(false);
      }
      getIsOptionalCheckBox().setSelected(fd.isOptional());
      getIsExternalCheckBox().setSelected(fd.isExternal());
      JTextUtil.setText(fd.getDefaultValue(), getDefaultValueTextField());
      try {
        getFieldHandlerComboBox().setSelectedItem(Class.forName(fd.getHandler()));
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
      JTextUtil.setText(fd.getHandlerArgs(), getHandlerArgsTextField());
      List<String> filterValue = Arrays.asList(fd.getFilterOnValue());
      getFilterNullCheckBox().setSelected(filterValue.contains("NULL"));
      getFilterNotNullCheckBox().setSelected(filterValue.contains("NOT_NULL"));
      List<String> filter = new ArrayList<String>(filterValue);
      filter.remove("NULL");
      filter.remove("NOT_NULL");
      getFilterValueList().setData(filter);
      getFilterFieldList().setData(fd.getFilterOnField());
      getNullDefinitionList().setData(Arrays.asList(fd.getNullDefinition()));
      setColumnLookup(fd.getLookupDefinition());
    } finally {
      getColumnGroup().setIgnoreEvents(false);
    }
  }

  private void trySettingFieldLength(String start, String end) {
    JTextUtil.setText("", getLocationLengthTextField());
    try {
      int len = Integer.parseInt(end) - Integer.parseInt(start);
      JTextUtil.setText(String.valueOf(len + 1), getLocationLengthTextField());
    } catch (Exception e) {
      // Never fail
    }
  }

  private void setColumnLookup(LookupDefinition ld) {
    if (ld != null) {
      getLookupCacheCheckBox().setSelected(ld.isLookupCache());
      getLookupOptionalCheckBox().setSelected(ld.isLookupOptional());
      Integer maximumCacheSize = ld.getMaximumCacheSize();
      if (maximumCacheSize == null) {
        getLookupCacheCapacityTextField().setText("");
      } else {
        JTextUtil.setText(String.valueOf(maximumCacheSize), getLookupCacheCapacityTextField());
      }
      JTextUtil.setText(ld.getLookupQuery(), getLookupQueryField());
      JTextUtil.setText(ld.getLookupInsertQuery(), getLookupQueryInsertField());
      JTextUtil.setText(ld.getLookupInsertGenKey(), getLookupQueryInsertKeyField());
      JTextUtil.setText(ld.getLookupFullCacheQuery(), getLookupQueryFullCacheTextField());
    } else {
      getLookupCacheCheckBox().setSelected(true);
      getLookupCacheCapacityTextField().setText("10000");
      getLookupOptionalCheckBox().setSelected(false);
      getLookupQueryField().setText("");
      getLookupQueryInsertField().setText("");
      getLookupQueryInsertKeyField().setText("");
      getLookupQueryFullCacheTextField().setText("");
    }
  }

  private JTextArea handlerDocumentationTextArea;
  private JButton handlerDocumentButton;
  private JDialog handlerDocumentDialog;

  private JButton getHandlerDocumentButton() {
    if (handlerDocumentButton == null) {
      handlerDocumentButton = new JButton("...");
      handlerDocumentButton.addActionListener(new ActionListener() {
        public void actionPerformed(ActionEvent e) {
          getHandlerDocumentDialog().setVisible(true);
        }
      });
      columnGroup.add(handlerDocumentButton, null);
    }
    return handlerDocumentButton;
  }

  private JDialog getHandlerDocumentDialog() {
    if (handlerDocumentDialog == null) {
      handlerDocumentDialog = new JDialog(getFrame());
      handlerDocumentDialog.getContentPane().add(new JScrollPane(getHandlerDocumentationTextArea()));
      handlerDocumentDialog.pack();
      handlerDocumentDialog.setLocationRelativeTo(getHandlerDocumentButton());
    }
    return handlerDocumentDialog;
  }

  private JTextArea getHandlerDocumentationTextArea() {
    if (handlerDocumentationTextArea == null) {
      handlerDocumentationTextArea = new JTextArea(5, 50);
      handlerDocumentationTextArea.setLineWrap(true);
      handlerDocumentationTextArea.setWrapStyleWord(true);
      handlerDocumentationTextArea.setEditable(false);
    }
    return handlerDocumentationTextArea;
  }

  public static void main(String[] args) {
    new LoadParseEditor();
  }

  private String getToolTips(String key) {
    Properties p = Utilities.loadPropertyFile("tooltips.properties");
    String text = p.getProperty(key);
    return text == null ? key : text;
  }

  public FieldDefinition createNewGenericField(Location l) {
    Location loc;
    if (l == null) {
      loc = new Location(null, null);
    } else {
      loc = l;
    }
    FieldDefinition def = new FieldDefinition("F", loc, false);
    for (int i = 1; definition.getField(def.getFieldName()) != null; i++) {
      def = new FieldDefinition("F_" + i, loc, false);
    }
    definition.updateField(def);
    return def;
  }

}
