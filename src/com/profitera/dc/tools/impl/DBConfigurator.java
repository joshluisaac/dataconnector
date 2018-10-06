package com.profitera.dc.tools.impl;

import info.clearthought.layout.TableLayout;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.swing.AbstractAction;
import javax.swing.BorderFactory;
import javax.swing.DefaultListModel;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;

import com.profitera.dc.parser.LoadingQueryWriter;
import com.profitera.prefs.PreferencesManager;
import com.profitera.swing.Images;
import com.profitera.swing.PreferenceUtil;
import com.profitera.util.Strings;
import com.profitera.util.Utilities;
import com.profitera.util.reflect.ClasspathSearcher;

public class DBConfigurator {
	
  private ImageIcon addIcon = Images.loadClasspathIcon("images/add16.gif");
  private ImageIcon deleteIcon = Images.loadClasspathIcon("images/delete16.gif");
  
	private JPanel pane;
	private JTextField driverField;
	private JTextField urlField;
	private JTextField usernameField;
	private JTextField passwordField;
	
	private JList driverLibsList;
	private DefaultListModel driverLibs;
	private JPanel buttonsPane;
	
	private JCheckBox isPrefixSelectKey;
	private JTextField selectKeyPrefixTextField;
	private JTextField selectKeySuffixTextField;
	
	private JComponent parent;
	
	private Properties connProperties = new Properties();;

	public DBConfigurator(JComponent parent){
		this.parent = parent;
	}
	
	public Properties getConnectionProperties(){
		return connProperties;
	}
	
	public int configure() throws IOException{	  
		int option = JOptionPane.showConfirmDialog(parent, getPanel(), "", JOptionPane.OK_CANCEL_OPTION, JOptionPane.PLAIN_MESSAGE);
		if(option==JOptionPane.OK_OPTION){
			connProperties.setProperty("DRIVER", getDriverField().getText());
			connProperties.setProperty("URL", getUrlField().getText());			
			connProperties.setProperty("USERNAME", getUsernameField().getText());
			connProperties.setProperty("DB_PASSWORD", getPasswordField().getText());	
			uploadDBDriver();
			updateDBConfig();
			updateQueryTemplate();
		}		
    
		return option;
	}
	
	private void uploadDBDriver() throws IOException{
	  List<String> allLibs = new ArrayList<String>();
	  for(int i=0;i<getDriverLibsListModel().size();i++){
	    String path = (String)getDriverLibsListModel().get(i);
	    File f = new File(path);
	    try {
        ClasspathSearcher.addFileToClasspath(f.toURI().toURL());
        allLibs.add(path);
      } catch (Exception e) {
        throw new IOException(e); 
      }
	  }
	  String classpath = Strings.getListString(allLibs, File.pathSeparator);
	  PreferencesManager.getInstance().setPreferredValue(getClass().getName(), "classpath", classpath);
	}
	
	private void updateDBConfig() throws IOException {
	  String sourceFilePath = SampleDataLoader.getDbSourceConfigPath(); 
		Properties connProperties = getDataSourceConfiguration();
		connProperties.setProperty("DRIVER", getDriverField().getText());
		connProperties.setProperty("URL", getUrlField().getText());			
		updatePropertiesFile(sourceFilePath, connProperties);
	}

	private void updatePropertiesFile(String fileName, Properties props) throws IOException{
		URL url = this.getClass().getClassLoader().getResource(fileName);
		File f = new File(url.getFile());
		FileWriter writer = new FileWriter(f);
		for(Iterator<Map.Entry<Object, Object>> i = props.entrySet().iterator(); i.hasNext();){
			Entry<Object, Object> e = i.next();
			writer.write(e.getKey()+"="+e.getValue()+"\n");
		}
		writer.flush();
		writer.close();
	}
	
	private void updateQueryTemplate() throws IOException {
		String fileName = "querytemplate.properties";
		Properties queryTemplate = Utilities.load(fileName);
		queryTemplate.setProperty(LoadingQueryWriter.TEMPLATE_IS_PREFIX_SELECTKEY_NAME, String.valueOf(getIsPrefixSelectKey().isSelected()));
		queryTemplate.setProperty(LoadingQueryWriter.TEMPLATE_KEY_KEY_START_PROP_NAME, getSelectKeyPrefixTextField().getText());
		queryTemplate.setProperty(LoadingQueryWriter.TEMPLATE_KEY_KEY_END_PROP_NAME, getSelectKeySuffixTextField().getText());
		updatePropertiesFile(fileName, queryTemplate);
	}
	
	private JPanel getPanel(){
		if(pane==null){    
			// connection properties
			TableLayout connlayout = new TableLayout(
					new double[]{5,TableLayout.PREFERRED,20,450,5},
					new double[]{5,
							TableLayout.PREFERRED,
							TableLayout.FILL,
	            TableLayout.PREFERRED,
							TableLayout.PREFERRED,
							TableLayout.PREFERRED,
							TableLayout.PREFERRED,
							TableLayout.PREFERRED,
							5});
			JPanel connPane = new JPanel(connlayout);
			connPane.setBorder(BorderFactory.createTitledBorder("Driver Properties"));
			connPane.add(new JLabel("Libraries"), "1,1");
			JScrollPane sp = new JScrollPane(getDriverLibsList());
			connPane.add(sp, "3,1,3,2");
			connPane.add(getAddRemoveButtons(), "3,3");
			connPane.add(new JLabel("Name "), "1,4");
			connPane.add(getDriverField(), "3,4");
			connPane.add(new JLabel("URL "), "1,5");
			connPane.add(getUrlField(), "3,5");
			connPane.add(new JLabel("Username "), "1,6");
			connPane.add(getUsernameField(), "3,6");
			connPane.add(new JLabel("Password "), "1,7");
			connPane.add(getPasswordField(), "3,7");
			// query template
			TableLayout querylayout = new TableLayout(
					new double[]{5,TableLayout.PREFERRED,20,TableLayout.FILL,5},
					new double[]{5,
							TableLayout.PREFERRED,
							TableLayout.PREFERRED,
							TableLayout.PREFERRED,
							5});
			JPanel queryPane = new JPanel(querylayout);
			queryPane.add(getIsPrefixSelectKey(), "1,1,3,1");
			queryPane.add(new JLabel("Select Key Prefix "), "1,2");
			queryPane.add(getSelectKeyPrefixTextField(), "3,2");
			queryPane.add(new JLabel("Select Key Suffic "), "1,3");
			queryPane.add(getSelectKeySuffixTextField(), "3,3");
			queryPane.setBorder(BorderFactory.createTitledBorder("Query Template"));
			pane = new JPanel(new BorderLayout());
			pane.add(connPane, BorderLayout.CENTER);
			pane.add(queryPane, BorderLayout.SOUTH);
			pane.setSize(600, 600);
			try {
				fillValue();
			} catch (IOException e) {
				e.printStackTrace();
				JOptionPane.showMessageDialog(parent, e.getMessage(), "Failed to load default database properties", JOptionPane.ERROR_MESSAGE);
			}
		}
		return pane;
	}

	private void fillValue() throws IOException{
	  // driver lib
	  String classpath = PreferencesManager.getInstance().getPreferredValue(getClass().getName(), "classpath", null);
	  if(classpath!=null){
	    String[] paths = classpath.split(File.pathSeparator);
	    for(int i=0;i<paths.length;i++){
	      String path = paths[i].trim();
	      if(!path.equals("")){
	        getDriverLibsListModel().addElement(path);
	      }
	    }
	  }
		// db
	  Properties connProperties = getDataSourceConfiguration();
		getDriverField().setText(connProperties.getProperty("DRIVER"));
		getUrlField().setText(connProperties.getProperty("URL"));
		getUsernameField().setText(connProperties.getProperty("USERNAME"));
		// password field left blanked
		// query
		Properties queryTemplate = Utilities.load("querytemplate.properties");
		String isPrefixSelectKey = queryTemplate.getProperty(LoadingQueryWriter.TEMPLATE_IS_PREFIX_SELECTKEY_NAME);
		getIsPrefixSelectKey().setSelected(Boolean.parseBoolean(isPrefixSelectKey));
		getSelectKeyPrefixTextField().setText(queryTemplate.getProperty(LoadingQueryWriter.TEMPLATE_KEY_KEY_START_PROP_NAME));
		getSelectKeySuffixTextField().setText(queryTemplate.getProperty(LoadingQueryWriter.TEMPLATE_KEY_KEY_END_PROP_NAME));
		
	}
	
	private Properties getDataSourceConfiguration() {
	  try{
	    return SampleDataLoader.getDataSourceConfiguration(null, "sample");
	  }catch(Exception e){
	    return new Properties();
	  }
	}
	
	private JTextField getDriverField() {
		if(driverField==null){
			driverField = new JTextField();
		}
		return driverField;
	}

	private JTextField getUrlField() {
		if(urlField==null){
			urlField = new JTextField();
		}
		return urlField;
	}

	private JTextField getUsernameField() {
		if(usernameField==null){
			usernameField = new JTextField();
		}
		return usernameField;
	}

	private JTextField getPasswordField() {
		if(passwordField==null){
			passwordField = new JTextField();
		}
		return passwordField;
	}

	private JCheckBox getIsPrefixSelectKey() {
		if(isPrefixSelectKey==null){
			isPrefixSelectKey = new JCheckBox("Prefix Select Key");
		}
		return isPrefixSelectKey;
	}

	private JTextField getSelectKeyPrefixTextField() {
		if(selectKeyPrefixTextField==null){
			selectKeyPrefixTextField = new JTextField();
		}
		return selectKeyPrefixTextField;
	}

	private JTextField getSelectKeySuffixTextField() {
		if(selectKeySuffixTextField==null){
			selectKeySuffixTextField = new JTextField();
		}
		return selectKeySuffixTextField;
	}

	private DefaultListModel getDriverLibsListModel(){
	  if(driverLibs==null){
	    driverLibs = new DefaultListModel();
	  }
	  return driverLibs;
	}

  private JList getDriverLibsList() {
    if(driverLibsList==null){
      driverLibsList = new JList(getDriverLibsListModel());
    }
    return driverLibsList;
  }
	
	private JPanel getAddRemoveButtons(){
	  if(buttonsPane==null){
	    buttonsPane = new JPanel(new FlowLayout(FlowLayout.TRAILING));
	    JButton add = new JButton(new AbstractAction(null, addIcon) {
        private static final long serialVersionUID = 1L;

        public void actionPerformed(ActionEvent e) {
          File lastDir = PreferenceUtil.getLastDirectory("directory", getClass().getName());
          JFileChooser fc = new JFileChooser(lastDir);
          fc.setMultiSelectionEnabled(true);
          int option = fc.showOpenDialog(getAddRemoveButtons());
          if(option == JFileChooser.APPROVE_OPTION){
            File[] files = fc.getSelectedFiles();
            for(int i=0;i<files.length;i++){
              getDriverLibsListModel().addElement(files[i].getAbsolutePath());
            }
            PreferenceUtil.setLastDirectory("directory", getClass().getName(), fc.getCurrentDirectory());
          }
        }
      });
	    buttonsPane.add(add);
	    JButton remove = new JButton(new AbstractAction(null, deleteIcon) {
        private static final long serialVersionUID = 1L;
        public void actionPerformed(ActionEvent e) {
          int selected = getDriverLibsList().getSelectedIndex();
          if(selected==-1) return;
          getDriverLibsListModel().remove(selected);
        }
      });
	    buttonsPane.add(remove);
	  }
	  return buttonsPane;
	}

}
