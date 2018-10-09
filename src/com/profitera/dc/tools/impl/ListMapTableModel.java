package com.profitera.dc.tools.impl;

import java.util.List;
import java.util.Map;

import javax.swing.table.AbstractTableModel;

public class ListMapTableModel extends AbstractTableModel{

	private final List<Map> data; 
	
	public ListMapTableModel(List<Map> data) {
		this.data = data;
	}	
	
	public int getColumnCount() {
		if(data.isEmpty()) return 0;
		return data.get(0).size();
	}

	public int getRowCount() {
		return data.size();
	}

	public Object getValueAt(int rowIndex, int columnIndex) {
		if(data.isEmpty()) return null;
		Map m = data.get(rowIndex);
		if(m.isEmpty()) return null;
		return m.values().toArray()[columnIndex];
	}

	@Override
	public String getColumnName(int column) {
		if(data.isEmpty()) return super.getColumnName(column);
		Map m = data.get(0);
		if(m.isEmpty()) return super.getColumnName(column);
		return (String)m.keySet().toArray()[column];
	}

	@Override
	public Class<?> getColumnClass(int columnIndex) {
		if(data.isEmpty()) return super.getColumnClass(columnIndex);
		Map m = data.get(0);
		if(m.isEmpty()) return super.getColumnClass(columnIndex);
		Object value = m.values().toArray()[columnIndex];
		if(value==null) return super.getColumnClass(columnIndex);
		return value.getClass();
	}
}
