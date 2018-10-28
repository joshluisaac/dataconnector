package com.profitera.dc.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.profitera.dc.InvalidLookupDefinitionException;
import com.profitera.dc.InvalidLookupQueryException;
import com.profitera.dc.InvalidRefreshDataQueryException;
import com.profitera.dc.RecordDispenserFactory;
import com.profitera.dc.parser.exception.InvalidConfigurationException;
import com.profitera.dc.parser.impl.FieldDefinition;
import com.profitera.dc.parser.impl.LoadDefinition;
import com.profitera.dc.parser.impl.LookupDefinition;
import com.profitera.util.ArrayUtil;
import com.profitera.util.Strings;
import com.profitera.util.reflect.NoSuchClassException;
import com.profitera.util.reflect.Reflect;
import com.profitera.util.reflect.ReflectionException;

public class LoadDefinitionValidator {

	public static void validate(LoadDefinition def) throws InvalidConfigurationException, InvalidLookupQueryException{
		
		validateLoadType(def);
		validateUpdateMode(def);
		validateFullCache(def);
		validateDestTable(def);
		validateRefreshQuery(def);
		
		validateFields(def);
		validateFieldLocation(def);
		
	}
	
	private static void validateLoadType(LoadDefinition def) throws InvalidConfigurationException{
		String[] loadTypes = new String[]{
				RecordDispenserFactory.FIXED_WIDTH,
				RecordDispenserFactory.DELIMITED,
				RecordDispenserFactory.XML,
				RecordDispenserFactory.MSXLS};
		if(ArrayUtil.indexOf(def.getLoadType(), loadTypes)==-1){
			throw new InvalidConfigurationException("Load type expected one of "+Arrays.asList(loadTypes));
		}
	}
	
	private static void validateUpdateMode(LoadDefinition def) throws InvalidConfigurationException{
		String[] mode = new String[]{
				LoadDefinition.MIXED_MODE, 
				LoadDefinition.INSERT_MODE, 
				LoadDefinition.UPDATE_MODE};
		if(ArrayUtil.indexOf(def.getUpdateMode(), mode)==-1){
			throw new InvalidConfigurationException("Illegal update mode "+def.getUpdateMode()+". Expect one of "+Arrays.asList(mode)+" if specified.");
		}
  	if (def.getUpdateMode().equals(LoadDefinition.INSERT_MODE) && def.isFullCache()) {
			// If we are insert mode then we should never be fullcache, it makes no sense
			throw new InvalidConfigurationException("Illegal combination of mode as '" + LoadDefinition.INSERT_MODE + "' and full cache enabled for load.");
		}
	}
	
	private static void validateFullCache(LoadDefinition def) throws InvalidConfigurationException{
		if (def.getUpdateMode().equals(LoadDefinition.INSERT_MODE) && def.isFullCache()) {
			// If we are insert mode then we should never be fullcache, it makes no sense
			throw new InvalidConfigurationException("Illegal combination of mode as '" + LoadDefinition.INSERT_MODE + "' and full cache enabled for load.");
		}
	}
	
	private static void validateDestTable(LoadDefinition def) throws InvalidConfigurationException{
		if(Strings.nullifyIfBlank(def.getDestTable())==null){
  		throw new InvalidConfigurationException("Destination table not specified");
  	}
	}
	
	private static void validateFields(LoadDefinition def) throws InvalidConfigurationException, InvalidLookupQueryException{
		List<FieldDefinition> fields = def.getAllFields();
		List<String> names = new ArrayList<String>();
		for(int i=0;i<fields.size();i++){
			FieldDefinition field = fields.get(i);
			validateFieldNameNotNull(field);
			validateFieldHandler(field);
			String fieldName = field.getFieldName();
			if(names.contains(fieldName)) throw new InvalidConfigurationException("Duplicated field '"+fieldName+"'");
			names.add(fieldName);			
			if(field.isLookupField()) {
				LookupDefinition ld = field.getLookupDefinition();
				if(Strings.nullifyIfBlank(ld.getLookupQuery())==null)
					throw new InvalidConfigurationException("Lookup query for field "+fieldName+" cannot be blanked if specified.");
				String[] lookupParams = ld.getLookupQueryParams();
				for(int k=0;k<lookupParams.length;k++){
					String lookupParam = lookupParams[k];
					if(lookupParam.equals(fieldName)) continue;
					FieldDefinition lookupParamField = getField(lookupParam, fields);
					if(lookupParamField==null){
						String error = "Field '" + lookupParam + "' is not defined in load definition but used in the lookup query for field '" + fieldName+"'";
	    			throw new InvalidConfigurationException(error, new InvalidLookupDefinitionException(error));
					}
					if(lookupParamField!=null && lookupParamField.getLookupDefinition()!=null){
						String[] paramLookupParams = lookupParamField.getLookupDefinition().getLookupQueryParams();
						if(ArrayUtil.indexOf(fieldName, paramLookupParams)>=0){
							String error = "Lookup query parameter references are circular. "+fieldName+"->"+lookupParam+"->"+fieldName;
			      	throw new InvalidConfigurationException(error, new InvalidLookupQueryException(ld.getLookupQuery(), error));
						}
						
						try{
							validateCircularReferenceAtDeeperLevel(new String[]{fieldName}, paramLookupParams, fields);
						}catch(InvalidConfigurationException e){
							throw new InvalidConfigurationException("Failed to set fields. Circular lookup reference occured. "+fieldName+"->"+lookupParam+"->"+e.getMessage());
						} // the code may not sexy but it works.			
					}
				}
			}
		}
		for(int i=0;i<fields.size();i++){
			FieldDefinition field = fields.get(i);
			String fieldName = field.getFieldName();
			String[] filterFields = field.getFilterOnField();
			if(ArrayUtil.indexOf(fieldName, filterFields)>-1){
				throw new InvalidConfigurationException("Field '"+fieldName+ "' can not filter on itself.");
			}
			for(int j=0;j<filterFields.length;j++){
				String filterField = filterFields[j];
				if(!names.contains(filterField)){
					throw new InvalidConfigurationException("Field '"+fieldName+"' can not filter on field '"+filterField+"' which does not exist.");
				}
			}
		}
	}
	
	private static FieldDefinition getField(String fieldName, List<FieldDefinition> fields) {
		for(int i=0;i<fields.size();i++){
			FieldDefinition f = fields.get(i);
			if(f.getFieldName().equals(fieldName))
				return f;
		}
		return null;
	}
	
	private static void validateCircularReferenceAtDeeperLevel(String[] ancestor, String[] lookupParams, List<FieldDefinition> allFields) throws InvalidConfigurationException, InvalidLookupQueryException{
		for(int i=0;i<lookupParams.length;i++){
			FieldDefinition paramField = getField(lookupParams[i], allFields);
			if(paramField==null) continue;
			String paramFieldName = paramField.getFieldName();
			if(paramField==null || !paramField.isLookupField()) {
			  continue;
			}
			String[] paramLookupParams = paramField.getLookupDefinition().getLookupQueryParams();
			a:for(int k=0;k<paramLookupParams.length;k++){
				String paramLookupParam = paramLookupParams[k];
				if(paramLookupParam.equals(paramFieldName)) continue a;
				for(int j=0;j<ancestor.length;j++){
					if(ancestor[j].equals(paramLookupParam)){
						throw new InvalidConfigurationException(paramFieldName+"->"+ancestor[j]);
					}
					if(ancestor[j].equals(paramFieldName)) continue a;
				}
				List<String> ancestorList = new ArrayList<String>();
				ancestorList.add(paramFieldName);
				for(int j=0;j<ancestor.length;j++){
					if(!ancestor[j].equals(paramFieldName))
						ancestorList.add(ancestor[j]);
				}
				try{
					validateCircularReferenceAtDeeperLevel(ancestorList.toArray(new String[0]), paramLookupParams, allFields);
				}catch(InvalidConfigurationException e){
					throw new InvalidConfigurationException(paramFieldName+"->"+e.getMessage());
				}
			}
		}
	}
	
	private static void validateFieldNameNotNull(FieldDefinition def) throws InvalidConfigurationException{
		String fieldName = def.getFieldName();
		if(Strings.nullifyIfBlank(fieldName)==null){
			throw new InvalidConfigurationException("Field name cannot be blanked");
		}
	}
	
	private static void validateFieldHandler(FieldDefinition def) throws InvalidConfigurationException{
		String clazz = def.getHandler();
		try{
			Reflect.invokeConstructor(clazz, new Class[0], new Object[0]);
		} catch (NoSuchClassException e){
			throw new InvalidConfigurationException("No such handler " + clazz);
		} catch (ReflectionException e){
			throw new InvalidConfigurationException(clazz+ " : "+e.getMessage());
		}
	}
	
	private static void validateRefreshQuery(LoadDefinition def) throws InvalidConfigurationException{
	  // Nothing to validate, used to check the content of the refresh query statement
	  // but that was a silly thing to do since DB syntaxes are all different
	}
	
	private static void validateFieldLocation(LoadDefinition def) throws InvalidConfigurationException {
		String type = def.getLoadType();
		if(!type.equals(RecordDispenserFactory.FIXED_WIDTH)) return;
		List<FieldDefinition> fields = def.getAllFields();
		for(int i=0;i<fields.size();i++){
			FieldDefinition d = fields.get(i);
			String start = d.getLocation().getStart();
			String end = d.getLocation().getEnd();
			try{
				int s = Integer.parseInt(start);
				int e = Integer.parseInt(end);
				if(s < 0 || e < 0) throw new InvalidConfigurationException(
						d.getFieldName() + " invalid location : Expect positive number.");
				if(s > e) throw new InvalidConfigurationException(
						d.getFieldName() + " invalid location : End position should greater than start position.");
			}catch(NumberFormatException e){
				throw new InvalidConfigurationException(d.getFieldName() + " invalid location : "+ e.getMessage());
			}
		}
	}

}
