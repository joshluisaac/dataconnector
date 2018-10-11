package com.profitera.dc.tools.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.w3c.dom.Document;

import com.ibatis.sqlmap.client.SqlMapClient;
import com.ibatis.sqlmap.client.SqlMapSession;
import com.ibatis.sqlmap.client.event.RowHandler;
import com.ibatis.sqlmap.engine.execution.BatchException;
import com.profitera.datasource.IDataSourceConfiguration;
import com.profitera.datasource.impl.DefaultDataSourceConfiguration;
import com.profitera.dc.DataLoader;
import com.profitera.dc.DriverNotFoundException;
import com.profitera.dc.LoadingErrorList;
import com.profitera.dc.impl.DatabaseOutput;
import com.profitera.dc.parser.LoadingQueryWriter;
import com.profitera.util.xml.ClasspathResolver;
import com.profitera.util.xml.DocumentLoader;

public class SampleDataLoader extends DataLoader{

	private SqlMapClient client;
	private List allData = new ArrayList();
	private List insertedData = new ArrayList();
	private List updatedData = new ArrayList();
	private List failedData = new ArrayList();
	private String mapFileUrl;
	
	public SampleDataLoader(String name) throws DriverNotFoundException {
		super(1, 1, name);
	}
	
	public List<Map> getAllData(){
		return allData;
	}
	
	public List<Map> getInsertedData(){
		return insertedData;
	}
	
	public List<Map> getUpdatedData(){
		return updatedData;
	}
	
	public List<Map> getFailedData(){
		return failedData;
	}
	
	public Document getMapFileDocument(){
		if(mapFileUrl==null) return null;
		try {
			URL url = new URL(mapFileUrl);
			File f = new File(url.getPath());
			ClasspathResolver resolver = new ClasspathResolver();
	  	String dtd = "com/ibatis/sqlmap/engine/builder/xml/sql-map-2.dtd";
	  	resolver.setResolution("-//iBATIS.com//DTD SQL Map 2.0//EN", dtd);
			return DocumentLoader.loadDocument(f, resolver);
		} catch (MalformedURLException e) {
			return null;
		} 
	}
	
	public LoadingErrorList loadSample(String path, Properties connectionProps) throws FileNotFoundException, IOException, Exception{
		String tmpDir = System.getProperty("java.io.tmpdir");
		IDataSourceConfiguration s = new DefaultDataSourceConfiguration("test", connectionProps);
		LoadingErrorList error = loadFile(path, s, tmpDir, 0, null, null, false, Charset.forName("UTF8"));
		return error;
	}
	
  protected void executeUpdates(final SqlMapClient client, final String statementNames, final Map statementArgs) {
  	allData.add(statementArgs);
  	try{
  	  new DatabaseOutput("testing", definition, isBatch()).executeUpdates(client, statementNames, statementArgs, 1, null, null);
  	}catch(RuntimeException e){
  		failedData.add(statementArgs);
  		throw e;
  	}
  }
	
	protected SqlMapClient getClient(String sqlMapConfig, Properties p) {
		mapFileUrl = p.getProperty(MAP_FILE_PROPERTY);
		if(client==null) {
			client = super.getClient(sqlMapConfig, p);
		}
		return new SqlMapClient(){

			public void flushDataCache() {
				client.flushDataCache();
			}

			public void flushDataCache(String arg0) {
				client.flushDataCache(arg0);
			}

			public SqlMapSession openSession() {
				return client.openSession();
			}

			public SqlMapSession openSession(Connection arg0) {
				return client.openSession();
			}

			public int delete(String arg0) throws SQLException {
				return client.delete(arg0);
			}

			public int delete(String arg0, Object arg1) throws SQLException {
				return client.delete(arg0, arg1);
			}

			public int executeBatch() throws SQLException {
				return client.executeBatch();
			}

			public List executeBatchDetailed() throws SQLException, BatchException {
				return client.executeBatchDetailed();
			}

			public Object insert(String arg0) throws SQLException {
				return client.insert(arg0);
			}

			public Object insert(String arg0, Object arg1) throws SQLException {
				try{
					Object key = client.insert(arg0, arg1);
					if(arg0.endsWith(LoadingQueryWriter.getInsertName(""))){
						insertedData.add(arg1);
					}
					return key;
				}catch(SQLException e){
					throw e;
				}
			}

			public List queryForList(String arg0) throws SQLException {
				return client.queryForList(arg0);
			}

			public List queryForList(String arg0, Object arg1) throws SQLException {
				return client.queryForList(arg0, arg1);
			}

			public List queryForList(String arg0, int arg1, int arg2) throws SQLException {
				return client.queryForList(arg0, arg1, arg2);
			}

			public List queryForList(String arg0, Object arg1, int arg2, int arg3) throws SQLException {
				return client.queryForList(arg0, arg1, arg2, arg3);
			}

			public Map queryForMap(String arg0, Object arg1, String arg2) throws SQLException {
				return client.queryForMap(arg0, arg1, arg2);
			}

			public Map queryForMap(String arg0, Object arg1, String arg2, String arg3) throws SQLException {
				return client.queryForMap(arg0, arg1, arg2, arg3);
			}

			public Object queryForObject(String arg0) throws SQLException {
				return client.queryForObject(arg0);
			}

			public Object queryForObject(String arg0, Object arg1) throws SQLException {
				return client.queryForObject(arg0, arg1);
			}

			public Object queryForObject(String arg0, Object arg1, Object arg2) throws SQLException {
				return client.queryForObject(arg0, arg1, arg2);
			}

			public void queryWithRowHandler(String arg0, RowHandler arg1) throws SQLException {
				client.queryWithRowHandler(arg0, arg1);
			}

			public void queryWithRowHandler(String arg0, Object arg1, RowHandler arg2) throws SQLException {
				client.queryWithRowHandler(arg0, arg1, arg2);
			}

			public void startBatch() throws SQLException {
				client.startBatch();
			}

			public int update(String arg0) throws SQLException {
				return client.update(arg0);
			}

			public int update(String arg0, Object arg1) throws SQLException {
				try{
					int effected = client.update(arg0, arg1);
					if(effected > 0 && arg0.endsWith(LoadingQueryWriter.getUpdateName(""))){
						updatedData.add(arg1);
					}
					return effected;
				}catch(SQLException e){
					throw e;
				}				
			}

			public void commitTransaction() throws SQLException {
				// never
			}

			public void endTransaction() throws SQLException {
				client.endTransaction();
			}

			public Connection getCurrentConnection() throws SQLException {
				return client.getCurrentConnection();
			}

			public DataSource getDataSource() {
				return client.getDataSource();
			}

			public Connection getUserConnection() throws SQLException {
				return client.getUserConnection();
			}

			public void setUserConnection(Connection arg0) throws SQLException {
				client.setUserConnection(arg0);
			}

			public void startTransaction() throws SQLException {
				client.startTransaction();
			}

			public void startTransaction(int arg0) throws SQLException {
				client.startTransaction(arg0);
			}
			
		};
	
	}


	
	
	
}
