package com.profitera.dc.tools.impl;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.ListDataEvent;
import javax.swing.event.ListDataListener;
import javax.swing.text.JTextComponent;

import com.profitera.dc.tools.ITextListEditor;
import com.profitera.swing.list.AllEventListDataListener;
import com.profitera.swing.text.AllEventsDocumentListener;
import com.profitera.swing.text.JTextUtil;

public class ComponentGroup {
  List<JComponent> comps = new ArrayList<JComponent>();
  List<Object> defaults = new ArrayList<Object>();
  List<ChangeListener> listeners = new ArrayList<ChangeListener>();
  private boolean ignore;
  private DocumentListener textListener = new AllEventsDocumentListener() {
    @Override
    public void updated(DocumentEvent e) {
      fire(e);
    }
  };
  private ActionListener actionListener = new ActionListener() {
    public void actionPerformed(ActionEvent e) {
      fire(e);
    }
  };
  private ListDataListener listListener = new AllEventListDataListener() {
    @Override
    protected void dataEvent(ListDataEvent e) {
      fire(e);
    }
  };
  
  public void add(JComponent c, Object defaultValue) {
    if (c instanceof JTextComponent) {
      ((JTextComponent)c).getDocument().addDocumentListener(textListener);
    } else if (c instanceof JCheckBox) {
      ((JCheckBox)c).addActionListener(actionListener);
    } else if (c instanceof JComboBox) {
      ((JComboBox<?>)c).addActionListener(actionListener);
    } else if (c instanceof ITextListEditor) {
      ITextListEditor<?> l = (ITextListEditor<?>) c;
      l.getModel().addListDataListener(listListener);
    } 
    comps.add(c);
    defaults.add(defaultValue);
  }
  
  protected void fire(Object e) {
    if (ignore) {
      return;
    }
    for (ChangeListener c : listeners) {
      c.stateChanged(new ChangeEvent(e));
    }
  }

  public void setEnabled(boolean b) {
    if (b) {
      for (int i = 0; i < comps.size(); i++) {
        comps.get(i).setEnabled(true);
      }
    } else {
      for (int i = 0; i < comps.size(); i++) {
        JComponent target = comps.get(i);
        Object def = defaults.get(i);
        disable(target, def);
      }
    }
  }
  
  private void disable(JComponent target, Object def) {
    target.setEnabled(false);
    if (target instanceof JTextComponent) {
      JTextComponent t = (JTextComponent) target;
      JTextUtil.setText((String) def, t);
    } else if (target instanceof JCheckBox) {
      JCheckBox t = (JCheckBox) target;
      t.setSelected((Boolean) (def == null ? false : def));
    } else if (target instanceof JComboBox) {
      JComboBox t = (JComboBox) target;
      if (def != null) {
        t.setSelectedItem(def);
      }
    }
  }
  
  public void addChangeListener(ChangeListener c) {
    listeners.add(c);
  }
  
  public void setIgnoreEvents(boolean b) {
    ignore = b;
  }

}
