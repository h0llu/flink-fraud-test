package ru.dataframe.dss.dto;

import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.RequiredArgsConstructor;


@Data
@RequiredArgsConstructor
public class Transaction {
	@Expose
	private final int id;
	@Expose
	@SerializedName("client_id")
	private final String clientId;
	@Expose
	private final String mcc;
	@Expose
	@SerializedName("event_time")
	private final Long eventTime;
	@Expose
	private final double amount;
	private double clientAvg;
	private boolean rule;

	public static Transaction fromJson(String json) {
		return new GsonBuilder().excludeFieldsWithoutExposeAnnotation()
				.create()
				.fromJson(json, Transaction.class);
	}

	public String toJson() {
		return new GsonBuilder().excludeFieldsWithoutExposeAnnotation()
				.create()
				.toJson(this);
	}
}
