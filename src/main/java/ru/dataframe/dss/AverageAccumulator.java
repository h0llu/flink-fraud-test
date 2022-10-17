package ru.dataframe.dss;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

public class AverageAccumulator implements SimpleAccumulator<Double> {
	private long count;
	private double sum;

	@Override
	public void add(Double value) {
		this.count++;
		this.sum += value;
	}

	@Override
	public Double getLocalValue() {
		if (this.count == 0) {
			return 0.0;
		}
		return this.sum / (double) count;
	}

	@Override
	public void resetLocal() {
		this.count = 0;
		this.sum = 0;
	}

	@Override
	public void merge(Accumulator<Double, Double> other) {
		if (other instanceof AverageAccumulator) {
			AverageAccumulator avg = (AverageAccumulator) other;
			this.count += avg.count;
			this.sum += avg.sum;
		} else {
			throw new IllegalArgumentException("The merged accumulator must be AverageAccumulator.");
		}
	}

	@Override
	public Accumulator<Double, Double> clone() {
		AverageAccumulator average = new AverageAccumulator();
		average.count = this.count;
		average.sum = this.sum;
		return average;
	}

	@Override
	public String toString() {
		return "AverageAccumulator " + this.getLocalValue() + " for " + this.count + " elements";
	}
}
