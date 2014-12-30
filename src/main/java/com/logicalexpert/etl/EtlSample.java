package com.logicalexpert.etl;

import java.net.URL;

import scriptella.execution.EtlExecutor;
import scriptella.execution.EtlExecutorException;

public class EtlSample {

	public EtlSample() {
		super();
		try {
			URL url = getClass().getResource("/etl.xml");
			EtlExecutor.newExecutor(url).execute();
		} catch (EtlExecutorException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new EtlSample();
	}
}
