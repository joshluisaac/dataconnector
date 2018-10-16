package com.profitera.dc.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
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

public class EMatcher {

	public static final String SOURCE1 = "source1";
	public static final String SOURCE2 = "source2";
	public static final String OUTPUT = "output";
	public static final String PAGE_SIZE = "pageSize";
	public static final String LOCATION_SUFFIX = "_location";

	private File source1 = null;
	private File source2 = null;
	
	private File output = null;
	private int pageSize = 50;
	
	private int maxLength = 0;

	private final List keys = new ArrayList();
	private final List locations = new ArrayList();
	private int noOfLines = 0;
	
	private EMatcher() {
	}

	public EMatcher(Properties props) throws Exception {
		source1 = new File(props.getProperty(SOURCE1));
		source2 = new File(props.getProperty(SOURCE2));

		System.out.println("Source file 1 is " + source1);
		System.out.println("Source file 2 is " + source2);
		
		if (!source1.exists() || !source1.canRead())
			throw new Exception("File " + props.getProperty(SOURCE1) + " not exist or can't be read.");
		if (!source2.exists() || !source2.canRead())
			throw new Exception("File " + props.getProperty(SOURCE2) + " not exist or can't be read.");
		
		output = new File(props.getProperty(OUTPUT));
		System.out.println("Output file is " + output);
		
		System.out.println("Parsing configuration");
		parseDefinition(props);
	}
	
	public void match() throws IOException{
		BufferedWriter out = new BufferedWriter(new FileWriter(output));
		System.out.println("Printing header");
		printHeader(out);
		match(out);
	}
	
	private void printHeader(BufferedWriter out) throws IOException{
		out.write("E-MATCHING REPORT" + Strings.filler(67) + "DATE: " + new SimpleDateFormat("dd-MM-yyyy").format(new Date()));
		out.newLine();
		out.write("E-MATCHING FILE " + source1.getName() + " AND " + source2.getName());
		out.newLine();
		out.write(Strings.pad(new String(), 100, '-'));
		out.newLine();
		out.newLine();
		out.flush();
		noOfLines += 6;
	}
	
	private void match(BufferedWriter out) throws IOException{
		System.out.println("Matching in progress");
		BufferedReader srcReader1 = new BufferedReader(new FileReader(source1));
		BufferedReader srcReader2 = new BufferedReader(new FileReader(source2));
		String line1 = null;
		String line2 = null;
		long lineNo1 = 0;
		long lineNo2 = 0;
		long matched = 0;
		long foundInTarget = 0;
		long notFound = 0;
		long notMatched = 0;
		Map keys1 = new HashMap();
		Map keys2 = new HashMap();
		while ((line1 = srcReader1.readLine()) != null){
			lineNo1++;
			keys1.clear();
			if (line1.length() < maxLength){
				printDetails(source1.getName(), line1, lineNo1, source2.getName(), null, lineNo2, source1.getName() + " record too short, expected at least " + maxLength + ", actual is " + line1.length(), out);
				continue;
			}
			for (int i = 0; i < keys.size(); i++){
				String tmpField = (String)keys.get(i);
				Integer[] tmpLocs = (Integer[])locations.get(i);
				keys1.put(tmpField, line1.substring(tmpLocs[0].intValue() - 1, tmpLocs[1].intValue()));
			}
			boolean found = true;
			boolean match = false;
			lineNo2 = 0;
			while ((line2 = srcReader2.readLine()) != null){
				found = true;
				match = false;
				lineNo2++;
				keys2.clear();
				if (line2.length() < maxLength)
					continue;
				for (int i = 0; i < keys.size(); i++){
					String tmpField = (String)keys.get(i);
					Integer[] tmpLocs = (Integer[])locations.get(i);
					keys2.put(tmpField, line2.substring(tmpLocs[0].intValue() - 1, tmpLocs[1].intValue()));
					if (keys1.get(tmpField) == null
							|| keys2.get(tmpField) == null
							|| !((String) keys1.get(tmpField))
									.equalsIgnoreCase((String) keys2.get(tmpField))) {
						found = false;
						break;
					}
				}
				if (found){
					foundInTarget++;
					if (line1.equalsIgnoreCase(line2)){
						match = true;
						matched++;
					}
					break;
				}
			}
			if (found && !match){
				notMatched++;
				printDetails(source1.getName(), line1, lineNo1, source2.getName(), line2, lineNo2, "Records did not match", out);
			}
			else if (!found){
				notFound++;
				printDetails(source1.getName(), line1, lineNo1, source2.getName(), null, 0, "Record not found", out);
			}

			srcReader2.close();
			srcReader2 = new BufferedReader(new FileReader(source2));
			if (lineNo1 % 10000 == 0)
				System.out.print("#");
		}
		if (lineNo1 % 10000 == 0)
			System.out.println();
		System.out.println("Printing summary");
		printSummary(source1.getName(), lineNo1, source2.getName(), lineNo2, matched, foundInTarget, notFound, notMatched, out);
		srcReader1.close();
		srcReader2.close();
	}
	
	private void printDetails(String src1Name, String line1, long lineNo1, String src2Name, String line2, long lineNo2, String message, BufferedWriter out) throws IOException{
		out.write(Strings.pad(src1Name + " LINE " + lineNo1, 30) + ": " + line1);
		out.newLine();
		out.write(Strings.pad(src2Name + " LINE " + lineNo2, 30) + ": " + line2);
		out.newLine();
		out.write(Strings.pad("MESSAGE", 30) + ": " + message.toUpperCase());
		out.newLine();
		out.newLine();
		noOfLines += 4;
		out.flush();
		if (isFullPage())
			newPage(out);
	}
	
	private void printSummary(String src1Name, long lineNo1, String src2Name, long lineNo2, long matched, long found, long notFound, long notMatched, BufferedWriter out) throws IOException{
		out.write(Strings.pad(Strings.leftPad(new String("SUMMARY"), 52, '-'), 100, '-'));
		out.newLine();
		out.write(src1Name + " HAS " + lineNo1 + " LINES.");
		out.newLine();
		out.write(src2Name + " HAS " + lineNo2 + " LINES.");
		out.newLine();
		out.write("RECORDS FOUND: " + found);
		out.newLine();
		out.write("RECORDS NOT FOUND: " + notFound);
		out.newLine();
		out.write("RECORDS MATCHED: " + matched);
		out.newLine();
		out.write("RECORDS UNMATCHED: " + notMatched);
		out.newLine();
		out.write(Strings.pad(new String(), 100, '-'));
		out.newLine();
		out.write("************************* END OF REPORT *************************");
		out.flush();
	}
	
	private boolean isFullPage(){
		return !(noOfLines < pageSize);
	}
	
	private void newPage(BufferedWriter out) throws IOException{
		while (!isFullPage()){
			out.newLine();
			noOfLines++;
		}
		out.flush();
		noOfLines = 0;
		out.write(Strings.pad(new String(), 100, '-'));
		out.newLine();
		noOfLines++;
		printHeader(out);
	}
	
	private void parseDefinition(Properties props){
		int keyCnt = 0;
	   for (Iterator i = props.keySet().iterator(); i.hasNext();) {
			String key = (String) i.next();
			if (key.endsWith(LOCATION_SUFFIX)) {
				keyCnt++;
				String field = key.substring(0, key.length() - LOCATION_SUFFIX.length());
				System.out.println("Match key" + keyCnt + "=" + field);
				keys.add(field);
				locations.add(getLocation(props.getProperty(field + LOCATION_SUFFIX)));
			}
		}
	}
	
	private Integer[] getLocation(String locStr){
		String[] locs = locStr.split("-");
		String startLoc = locs[0].trim();
		String endLoc = locs[1].trim();
		Integer[] location = new Integer[2];
		location[0] = new Integer(startLoc);
		location[1] = new Integer(endLoc);
		maxLength = maxLength < location[1].intValue() ? location[1].intValue() : maxLength;
		return location;
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
		if (args.length != 1){
			System.out.println("Syntax: java EMatcher <configuration file>");
			System.exit(1);
		}
		
		System.out.println("E-Matching started at " + new SimpleDateFormat("dd-MM-yyyy HH:MM:ss").format(new Date()));
		System.out.println("Loading configuration file " + args[0]);
		Properties prop = new Properties();
		prop.load(new FileInputStream(args[0]));
		System.out.println("Initializing EMatcher");
		EMatcher em = new EMatcher(prop);
		System.out.println("Starting match");
		em.match();
		System.out.println("E-Matching ended at " + new SimpleDateFormat("dd-MM-yyyy HH:MM:ss").format(new Date()));
	}
}