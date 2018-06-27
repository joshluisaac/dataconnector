package com.profitera.dc.filehandler;

import java.io.BufferedInputStream;
import java.util.NoSuchElementException;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.Configuration;
import net.sf.saxon.query.DynamicQueryContext;
import net.sf.saxon.query.StaticQueryContext;
import net.sf.saxon.query.XQueryExpression;

import org.xml.sax.helpers.DefaultHandler;


public class XMLRecordIterator implements IXMLRecordIterator {
	private Object currentObject = null;
	private XMLRecordIteratorThread iteratorThread = null;
	private Exception exception = null;
	private RuntimeException rtException = null;
	
	public XMLRecordIterator(final BufferedInputStream inStream, String recordTag) throws Exception{
		SAXParserFactory factory = SAXParserFactory.newInstance();
		final SAXParser parser = factory.newSAXParser();
		final DefaultHandler handler = new XMLHandoffFileHandler(recordTag, this);
		synchronized (this) {
			iteratorThread = new XMLRecordIteratorThread(){
				protected void execute() throws Exception {
					parser.parse(inStream, handler);
				}};
				iteratorThread.start();
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (exception != null)
				  throw exception;
				if (rtException != null)
				  throw rtException;
		}
	}

	public XMLRecordIterator(final BufferedInputStream inStream, String recordTag, String xquery) throws Exception{
		final DefaultHandler handler = new XMLHandoffFileHandler(recordTag, this);
		Configuration conf = new Configuration();
		StaticQueryContext sqc = new StaticQueryContext(conf);
		final XQueryExpression xqe = sqc.compileQuery(xquery);
		final DynamicQueryContext dynamicContext = new DynamicQueryContext(conf);
		dynamicContext.setContextItem(sqc.buildDocument(new StreamSource(inStream)));
    final SAXResult result = new SAXResult(handler);

		synchronized (this) {
			iteratorThread = new XMLRecordIteratorThread(){
				protected void execute() throws Exception {
					xqe.run(dynamicContext, result, null);
				}};
				iteratorThread.start();
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (exception != null)
				  throw exception;
				if (rtException != null)
				  throw rtException;
		}
	}

	public synchronized void put(Object o) {
		currentObject = o;
		doNotifyAndWait();
	}

	public void remove() {
		throw new UnsupportedOperationException("Remove not supported.");
	}

	public boolean hasNext() {
		return (currentObject != null);
	}

	public Object next() {
		if (!hasNext())
			throw new NoSuchElementException();
		Object toReturn = currentObject;
		doNotifyAndWait();
		return toReturn;
	}
	
	private synchronized void doNotifyAndWait() {
		notify();
		try {
			wait();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private synchronized void finalNotify() {
    notify();
  }

	private abstract class XMLRecordIteratorThread extends Thread {
		public void run(){
			try {
			  execute();
			} catch (RuntimeException e){
			  rtException = e;
			} catch (Exception e) {
			  exception = e;
			}
			currentObject = null;
			finalNotify();
		}

		protected abstract void execute() throws Exception;
	}
}