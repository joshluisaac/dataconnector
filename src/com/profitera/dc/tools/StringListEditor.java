package com.profitera.dc.tools;

import info.clearthought.layout.TableLayout;

import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Arrays;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;

import com.profitera.swing.Images;

public class StringListEditor extends JPanel implements ITextListEditor<String> {
	private static final long serialVersionUID = 1L;
	
	private ImageIcon addIcon = Images.loadClasspathIcon("images/add16.gif");
	private ImageIcon deleteIcon = Images.loadClasspathIcon("images/delete16.gif");
	
	private JList<String> list;
	private DefaultListModel<String> model;
	private JTextField field;
	private JButton addButton;
	private JButton removeButton;
	
	public StringListEditor(String title){
		this(title, 70);
	}
	
	public StringListEditor(String title, double height){
		super(new TableLayout(
				new double[]{5,TableLayout.FILL,TableLayout.PREFERRED,TableLayout.PREFERRED,5},
				new double[]{5,height,TableLayout.PREFERRED,5}
				));
		if(title!=null){
			setBorder(BorderFactory.createTitledBorder(title));
		}
		add(new JScrollPane(getList()), "1,1,3,1");
		add(getField(),"1,2");
		add(getAddButton(), "2,2");
		add(getRemoveButton(), "3,2");
	}
		
	public JList<String> getList(){
		if(list==null){
			list =  new JList<>(getModel());
		}
		return list;
	}
	
	public DefaultListModel<String> getModel(){
		if(model==null){
			model = new DefaultListModel<>();
		}
		return model;
	}
	
	public JTextField getField(){
		if(field==null){
			field = new JTextField(20);
		}
		return field;
	}
	
	public void setEnabled(boolean enabled){
		getField().setEnabled(enabled);
		getAddButton().setEnabled(enabled);
		getRemoveButton().setEnabled(enabled);
	}
	
	public void clear(){
		getField().setText(null);
		getModel().clear();
	}
	
	public void setData(String[] data){
	  setData(Arrays.asList(data));
	}
	
	public void setData(List<String> data){
		clear();
		for(int i=0;i<data.size();i++){
			getModel().addElement(data.get(i));
		}
	}
	
	public String[] getData() {
	  String[] data = new String[getModel().size()];
	  for (int i = 0; i < data.length; i++) {
      data[i] = (String) getModel().get(i);
    }
	  return data;
	}
	
	protected ActionListener getAddActionListener(){
		return new ActionListener(){
			public void actionPerformed(ActionEvent e) {
				String newValue = getField().getText();
				if(newValue==null || newValue.equals("")) return;
				getModel().addElement(newValue);
				getList().setSelectedValue(newValue, true);
				getField().setText("");
			}
		};
	}
	
	protected ActionListener getRemoveActionListener(){
		return new ActionListener(){
			public void actionPerformed(ActionEvent e) {
				int selected = getList().getSelectedIndex();
				if(selected!=-1){
					getModel().remove(selected);
				}
			}
		};
	}
	
	private JButton getAddButton(){
		if(addButton==null){
			addButton = new JButton(addIcon);
			if (addIcon != null) {
			  addButton.setPreferredSize(new Dimension(addIcon.getIconWidth(),addIcon.getIconHeight()));
			}
			addButton.addActionListener(getAddActionListener());
		}
		return addButton;
	}
	
	private JButton getRemoveButton(){
		if(removeButton==null){
			removeButton = new JButton(deleteIcon);
			if (deleteIcon != null) {
			  removeButton.setPreferredSize(new Dimension(deleteIcon.getIconWidth(),deleteIcon.getIconHeight()));
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
