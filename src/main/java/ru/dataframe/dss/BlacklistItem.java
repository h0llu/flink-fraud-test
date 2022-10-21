package ru.dataframe.dss;

import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class BlacklistItem {
	int id;
	String clientId;
	boolean flag;

	public BlacklistItem(int id, String clientId, int flag) {
		this.id = id;
		this.clientId = clientId;
		this.flag = flag != 0;
	}

	public static void main(String[] args) {
		BlacklistItem item = new BlacklistItem(1, "abc", 3);
		System.out.println(item);
	}
}
