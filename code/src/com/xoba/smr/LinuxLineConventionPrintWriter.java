package com.xoba.smr;

import java.io.PrintWriter;

public class LinuxLineConventionPrintWriter {

	public LinuxLineConventionPrintWriter(PrintWriter pw) {
		this.pw = pw;
	}

	private final PrintWriter pw;

	public void println(Object x) {
		pw.print(x);
		endOfLine();
	}

	private void endOfLine() {
		pw.print('\n');
	}

	public void print(Object x) {
		pw.print(x);
	}

	public void println() {
		endOfLine();
	}

	public void printf(String fmt, Object... x) {
		pw.printf(fmt, x);
	}

	public void close() {
		pw.close();
	}
}