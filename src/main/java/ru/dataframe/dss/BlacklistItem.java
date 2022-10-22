package ru.dataframe.dss;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"id", "clientId", "isBlacklisted"})
@Data
public class BlacklistItem {
	private int id;
	private String clientId;
	private boolean isBlacklisted;
}
