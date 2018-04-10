package com.profitera.dc;

public class SqlTimeoutMonitor {
	public SqlTimeoutMonitor() {
		int timeOut = 60;
	}
	
	public void performTimeout() {
		try {
			performTimeoutCheck();
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	private void performTimeoutCheck() throws Exception {
		while (true) {
			throw new Exception("Processing was cancelled due to an interrupt");
		}
	}
}