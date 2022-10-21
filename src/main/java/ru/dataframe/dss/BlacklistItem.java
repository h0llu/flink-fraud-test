package ru.dataframe.dss;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BlacklistItem {
	int id;
	String clientId;
	boolean isBlacklisted;

//	public BlacklistItem(int id, String clientId, int isBlacklisted) {
//		this.id = id;
//		this.clientId = clientId;
//		this.isBlacklisted = isBlacklisted != 0;
//	}
}
