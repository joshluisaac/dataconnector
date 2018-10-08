package com.profitera.dc.tools.impl;

import java.io.OutputStream;
import java.io.PrintStream;

import javax.swing.JTextArea;

public class JTextAreaPrintStream extends PrintStream {

	private static class JTextAreaOutputStream extends OutputStream {
		private JTextArea textArea;

		private JTextAreaOutputStream(JTextArea textArea) {
			this.textArea = textArea;
		}

		public void write(int i) {
			textArea.append(new String(new char[] { (char) i }));
		}

		public void write(byte[] b) {
			textArea.append(new String(b));
		}

		public void write(byte[] b, int offset, int len) {
			textArea.append(new String(b, offset, len));
		}
	}

	/**
	 * Creates a new <code>JTextAreaPrintStream</code> coupled to a given
	 * <code>JTextArea</code>.
	 * 
	 * @param textArea
	 *          The <code>JTextArea</code> to couple this print stream to
	 */
	public JTextAreaPrintStream(JTextArea textArea) {
		super(new JTextAreaOutputStream(textArea));
	}
}
