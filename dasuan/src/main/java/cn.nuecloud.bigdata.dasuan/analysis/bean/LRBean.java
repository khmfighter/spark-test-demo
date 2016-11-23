package cn.nuecloud.bigdata.dasuan.analysis.bean;
public class LRBean {
	public double getElasticNetParam() {
		return elasticNetParam;
	}

	public void setElasticNetParam(double elasticNetParam) {
		this.elasticNetParam = elasticNetParam;
	}

	public boolean isFitIntercept() {
		return fitIntercept;
	}

	public void setFitIntercept(boolean fitIntercept) {
		this.fitIntercept = fitIntercept;
	}

	public int getMaxIter() {
		return maxIter;
	}

	public void setMaxIter(int maxIter) {
		this.maxIter = maxIter;
	}

	public double getRegParam() {
		return regParam;
	}

	public void setRegParam(double regParam) {
		this.regParam = regParam;
	}

	public String getSolver() {
		return solver;
	}

	public void setSolver(String solver) {
		this.solver = solver;
	}

	public boolean isStandardization() {
		return standardization;
	}

	public void setStandardization(boolean standardization) {
		this.standardization = standardization;
	}

	public double getTol() {
		return tol;
	}

	public void setTol(double tol) {
		this.tol = tol;
	}

	public String getWeightCol() {
		return weightCol;
	}

	public void setWeightCol(String weightCol) {
		this.weightCol = weightCol;
	}

	private double elasticNetParam = 0.0;
	private boolean fitIntercept = true;
	private int maxIter = 100;
	private double regParam = 0.0;
	private String solver = "auto";
	private boolean standardization = true;
	private double tol = 1E-6;
	private String weightCol = "";
}
