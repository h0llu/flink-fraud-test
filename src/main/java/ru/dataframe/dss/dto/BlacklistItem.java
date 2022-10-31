package ru.dataframe.dss.dto;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

@JsonPropertyOrder({"id", "clientId", "isBlacklisted"})
@Data
public class BlacklistItem {
	private int id;
	private String clientId;
	private boolean isBlacklisted;

	public static List<BlacklistItem> getBlacklist(String blacklistFilePath) throws IOException {
		return new CsvMapper().readerWithTypedSchemaFor(BlacklistItem.class)
				.<BlacklistItem>readValues(new File(blacklistFilePath))
				.readAll();
	}
}
