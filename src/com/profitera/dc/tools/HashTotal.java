package com.profitera.dc.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.profitera.util.Strings;

public class HashTotal {
	public static final String SOURCE = "source";
	public static final String PROGRESS = "progress";
	public static final String LOCATION_SUFFIX = "_location";
	
	public static final String NO_OF_RECORDS = "NO_OF_RECORDS";

	private File source = null;
	
	private final List fields = new ArrayList();
	private final List locations = new ArrayList();
	
	private Map totals = new HashMap();
	private long progress = 10000;
	
	private HashTotal() {
	}

	public HashTotal(Properties props) throws Exception {
		parseDefinition(props);
	}
	
	private void parseDefinition(Properties props) throws Exception{
		System.out.println("Parsing configuration");
		source = new File(props.getProperty(SOURCE));
		if (!source.exists() || !source.canRead())
			throw new Exception("File " + props.getProperty(SOURCE) + " not exist or can't be read.");
		if (props.getProperty(PROGRESS) != null)
			progress = Long.parseLong(props.getProperty(PROGRESS));
		System.out.println("Progress indicator is set to show at every " + progress + " records");
		System.out.println("Source file is " + source.getAbsolutePath());
		 for (Iterator i = props.keySet().iterator(); i.hasNext();) {
		    String key = (String) i.next();
		    if(key.endsWith(LOCATION_SUFFIX)){
		    	String field = key.substring(0, key.indexOf(LOCATION_SUFFIX)); 
		    	fields.add(field);
		    	Integer locs[] = getLocation(props.getProperty(field + LOCATION_SUFFIX));
		    	System.out.println("Key " + field + " location from " + locs[0].toString() + " to " + locs[1].toString());
		  	locations.add(locs);
		  	totals.put(field, new Long(0));
		  }
		}
		System.out.println("Definition parsing completed");
	}
	
	public Map total() throws FileNotFoundException, IOException{
		System.out.println("Initializing file " + source.getAbsolutePath());
		BufferedReader reader = new BufferedReader(new FileReader(source));
		String line = null;
		long rows = 0;
		System.out.println("Iterating through file " + source.getAbsolutePath());
		while ((line = reader.readLine()) != null){
			rows++;
			if (progress > 0 && rows % progress == 0){
				if (rows == progress)
					System.out.print("Progress: ");
				System.out.print("#");
			}
			for (int i = 0; i < fields.size(); i++){
				String field = (String)fields.get(i);
				Integer[] loc = (Integer[])locations.get(i);
				int start = loc[0].intValue();
				int end = loc[1].intValue();
				if (line != null && line.length() >= end){
					String tmpStr = line.substring(start - 1, end).trim();
					if (tmpStr.length() > 0){
						long tmpVal = Long.parseLong(tmpStr);
						totals.put(field, new Long(tmpVal + ((Long)totals.get(field)).longValue()));
					}
				}
			}
		}
		if (progress > 0 && rows >= progress)
			System.out.println();
		System.out.println("******************* SUMMARY *******************");
		for (int i = 0; i < fields.size(); i++)
			System.out.println(Strings.pad((String)fields.get(i), 30) + ": " + totals.get(fields.get(i)));
		System.out.println(Strings.pad("NO. OF RECORDS", 30) + ": " + rows);
		System.out.println("******************* SUMMARY *******************");
		System.out.println();

		totals.put(NO_OF_RECORDS, new Long(rows));
		reader.close();
		return totals;
	}
	
	private Integer[] getLocation(String locStr){
		String[] locs = locStr.split("-");
		String startLoc = locs[0].trim();
		String endLoc = locs[1].trim();
		Integer[] location = new Integer[2];
		location[0] = new Integer(startLoc);
		location[1] = new Integer(endLoc);
		return location;
	}

	public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
		if (args.length != 1){
			System.out.println("Syntax: java " + HashTotal.class.getName() + " <configuration file>");
			System.exit(1);
		}
		System.out.println("HashTotal started with configuration file " + args[0] + " at " + new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
		Properties props = new Properties();
		props.load(new FileInputStream(args[0]));
		HashTotal ht = new HashTotal(props);
		Map summary = ht.total();
		System.out.println("HashTotal finished processing " + summary.get(HashTotal.NO_OF_RECORDS) + " records");
		System.out.println("HashTotal finished at " + new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()));
	}
}