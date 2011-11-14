package com.xoba.smr;

public enum AmazonInstance {

	T1_MICRO("t1.micro", true, 0.02, 2, 0.513),

	M1_SMALL("m1.small", false, 0.085, 1, 1.7),

	C1_MEDIUM("c1.medium", false, 0.17, 5, 1.7),

	M1_LARGE("m1.large", true, 0.34, 4, 7.5),

	M2_XLARGE("m2.xlarge", true, 0.50, 6.5, 17.1),

	M1_XLARGE("m1.xlarge", true, 0.68, 8, 15),

	C1_XLARGE("c1.xlarge", true, 0.68, 20, 7),

	M2_2XLARGE("m2.2xlarge", true, 1.00, 13, 34.2),

	M2_4XLARGE("m2.4xlarge", true, 2.00, 26, 68.4),

	CG1_4XLARGE("cg1.4xlarge", true, 2.00, 33.5, 22, true ? "ami-687c8301" : "ami-d002fdb9");

	private final boolean is64Bit;
	private final double memoryGB, computeUnits, price;
	private final String apiName;
	private final String defaultAMI;

	AmazonInstance(String apiName, boolean is64Bit, double price, double cpu, double mem) {
		this(apiName, is64Bit, price, cpu, mem, null);
	}

	public String getDefaultAMI() {
		if (defaultAMI == null) {
			return isIs64Bit() ? "ami-21f53948" : "ami-29f43840";
		} else {
			return defaultAMI;
		}
	}

	public static AmazonInstance getForAPIName(String name) {
		for (AmazonInstance i : AmazonInstance.values()) {
			if (i.getApiName().equals(name)) {
				return i;
			}
		}
		throw new IllegalArgumentException();
	}

	AmazonInstance(String apiName, boolean is64Bit, double price, double cpu, double mem, String ami) {
		this.apiName = apiName;
		this.computeUnits = cpu;
		this.memoryGB = mem;
		this.is64Bit = is64Bit;
		this.defaultAMI = ami;
		this.price = price;
	}

	public double getPrice() {
		return price;
	}

	public String getApiName() {
		return apiName;
	}

	public boolean isIs64Bit() {
		return is64Bit;
	}

	public double getMemoryGB() {
		return memoryGB;
	}

	public double getComputeUnits() {
		return computeUnits;
	}

}