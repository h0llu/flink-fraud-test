package ru.dataframe.dss;

import lombok.Data;
import lombok.RequiredArgsConstructor;


@Data
@RequiredArgsConstructor
public class Transaction {
	private final int id;
	private final String clientId;
	private final String mcc;
	private final Long eventTime;
	private final double amount;
	private double clientAvg;
	private boolean rule;
}
