package com.profitera.dc.tools;

import info.clearthought.layout.TableLayout;

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.event.DocumentEvent;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.text.BadLocationException;
import javax.swing.text.JTextComponent;

import com.profitera.dc.parser.XMLConstantKey;
import com.profitera.layout.TablePos;
import com.profitera.swing.Images;
import com.profitera.swing.text.AllEventsDocumentListener;

public class PostActionStatementListEditor extends JPanel implements
    ITextListEditor<List<String>> {
  private static final long serialVersionUID = 1L;

  private ImageIcon addIcon = Images.loadClasspathIcon("images/add16.gif");
  private ImageIcon deleteIcon = Images
      .loadClasspathIcon("images/delete16.gif");

  private JList<List<String>> list;
  private DefaultListModel<List<String>> model;
  private JTextArea statementText;
  private JComboBox<String> typeCombo;
  private JButton addButton;
  private JButton removeButton;

  public PostActionStatementListEditor(String title) {
    super();
    double[] columns = new double[] { 5, 160,
        TableLayout.FILL, TableLayout.PREFERRED, TableLayout.PREFERRED, 5 };
    double[] rows = new double[] { 5,
        TableLayout.PREFERRED, TableLayout.FILL, TableLayout.PREFERRED, 5 };
    TableLayout layout = new TableLayout(columns, rows);
    setLayout(layout);
    if (title != null) {
      setBorder(BorderFactory.createTitledBorder(title));
    }
    add(new JScrollPane(getList()), TablePos.colPos(1, 1, 3));
    add(getTypeCombo(), TablePos.pos(1, 2, 1, 4));
    add(getStatementTextField(), TablePos.pos(2, 2, 2, 4));
    add(getAddButton(), TablePos.pos(3, 3));
    add(getRemoveButton(), TablePos.pos(3, 4));
    load(null);
  }

  public JList<List<String>> getList(){
    if(list==null){
      list =  new JList<>(getModel());
      list.addListSelectionListener(new ListSelectionListener() {
        @Override
        public void valueChanged(ListSelectionEvent e) {
          if (e.getValueIsAdjusting()) {
            List<String> selectedValue = getList().getSelectedValue();
            load(selectedValue);
          }
        }
      });
    }
    return list;
  }

  private void load(List<String> selectedValue) {
    getRemoveButton().setEnabled(selectedValue != null);
    getTypeCombo().setEnabled(selectedValue != null);
    getStatementTextField().setEnabled(selectedValue != null);
    if (selectedValue == null) {
      getStatementTextField().setText("");
    } else {
      getTypeCombo().setSelectedItem(selectedValue.get(0));
      getStatementTextField().setText(selectedValue.get(1));
    }
  }

  public DefaultListModel<List<String>> getModel(){
    if(model==null){
      model = new DefaultListModel<List<String>>();
    }
    return model;
  }
  public JComboBox<String> getTypeCombo() {
    if (typeCombo == null) {
      typeCombo = new JComboBox<String>(new DefaultComboBoxModel<String>(new String[]{
          XMLConstantKey.XML_INSERT_INSERTION_KEY, XMLConstantKey.XML_INSERT_UPDATE_KEY,
          XMLConstantKey.XML_UPDATE_INSERTION_KEY, XMLConstantKey.XML_UPDATE_UPDATE_KEY
      }));
      typeCombo.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(ActionEvent e) {
          List<String> selectedValue = getList().getSelectedValue();
          if (selectedValue != null) {
            selectedValue.set(0, typeCombo.getItemAt(typeCombo.getSelectedIndex()));
          }
          getModel().set(getList().getSelectedIndex(), selectedValue);
        }
      });
    }
    return typeCombo;
  }
  public JTextComponent getStatementTextField() {
    if (statementText == null) {
      statementText = new JTextArea();
      statementText.getDocument().addDocumentListener(new AllEventsDocumentListener() {
        @Override
        public void updated(DocumentEvent e) {
          try {
            String newValue = e.getDocument().getText(0, e.getDocument().getLength());
            List<String> selectedValue = getList().getSelectedValue();
            if (selectedValue != null) {
              selectedValue.set(1, newValue);
              getModel().set(getList().getSelectedIndex(), selectedValue);
            }
          } catch (BadLocationException e1) {}
        }
      });
    }
    return statementText;
  }

  public void setEnabled(boolean enabled) {
    getStatementTextField().setEnabled(enabled);
    getTypeCombo().setEnabled(enabled);
    getAddButton().setEnabled(enabled);
    getRemoveButton().setEnabled(enabled);
  }

  public void clear() {
    getStatementTextField().setText(null);
    getModel().clear();
  }

  public void setData(String[] insertInsert, String[] insertUpdate,
      String[] updateInsert, String[] updateUpdate) {
    clear();
    Map<String, String[]> statements = new HashMap<String, String[]>();
    statements.put(XMLConstantKey.XML_INSERT_INSERTION_KEY, insertInsert);
    statements.put(XMLConstantKey.XML_INSERT_UPDATE_KEY, insertUpdate);
    statements.put(XMLConstantKey.XML_UPDATE_INSERTION_KEY, updateInsert);
    statements.put(XMLConstantKey.XML_UPDATE_UPDATE_KEY, updateUpdate);
    List<List<String>> allStatements = new ArrayList<List<String>>();
    for (Map.Entry<String, String[]> e : statements.entrySet()) {
      String postType = e.getKey();
      String[] statementsForType = e.getValue();
      for (int i = 0; statementsForType != null && i < statementsForType.length; i++) {
        allStatements.add(getTwo(postType, statementsForType[i]));
      }
    }
    for (List<String> list : allStatements) {
      getModel().addElement(list);
    }
    
  }

  private List<String> getTwo(String a, String b) {
    List<String> l = new ArrayList<String>();
    l.add(a);
    l.add(b);
    return l;
  }

  public Map<String, List<String>> getData() {
    Map<String, List<String>> data = new HashMap<String, List<String>>();
    data.put(XMLConstantKey.XML_INSERT_INSERTION_KEY, new ArrayList<String>());
    data.put(XMLConstantKey.XML_INSERT_UPDATE_KEY, new ArrayList<String>());
    data.put(XMLConstantKey.XML_UPDATE_INSERTION_KEY, new ArrayList<String>());
    data.put(XMLConstantKey.XML_UPDATE_UPDATE_KEY, new ArrayList<String>());
    for (int i = 0; i < getModel().getSize(); i++) {
      List<String> item = getModel().get(i);
      data.get(item.get(0)).add(item.get(1));
    }
    return data;
  }

  protected ActionListener getAddActionListener() {
    return new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        List<String> newValue = getTwo(XMLConstantKey.XML_INSERT_INSERTION_KEY, "insert into ..");
        getModel().addElement(newValue);
        getList().setSelectedIndex(getModel().getSize() - 1);
        load(newValue);
      }
    };
  }

  protected ActionListener getRemoveActionListener() {
    return new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        int selected = getList().getSelectedIndex();
        if (selected != -1) {
          getModel().remove(selected);
        }
      }
    };
  }

  private JButton getAddButton() {
    if (addButton == null) {
      addButton = new JButton(addIcon);
      if (addIcon != null) {
        addButton.setPreferredSize(new Dimension(addIcon.getIconWidth(),
            addIcon.getIconHeight()));
      }
      addButton.addActionListener(getAddActionListener());
    }
    return addButton;
  }

  private JButton getRemoveButton() {
    if (removeButton == null) {
      removeButton = new JButton(deleteIcon);
      if (deleteIcon != null) {
        removeButton.setPreferredSize(new Dimension(deleteIcon.getIconWidth(),
            deleteIcon.getIconHeight()));
      }
      removeButton.addActionListener(getRemoveActionListener());
    }
    return removeButton;
  }

  @Override
  public void setToolTipText(String text) {
    getList().setToolTipText(text);
  }
}
