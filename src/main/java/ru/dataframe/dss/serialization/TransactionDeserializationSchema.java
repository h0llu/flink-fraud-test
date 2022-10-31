package ru.dataframe.dss.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import ru.dataframe.dss.dto.Transaction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TransactionDeserializationSchema implements DeserializationSchema<Transaction> {
	private static Long getTimeInMillis(String dateTimeToParse) {

		return Duration.between(LocalDateTime.parse("1970-01-01 00:00:00",
								DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
						),
						LocalDateTime.parse(dateTimeToParse,
								DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
						))
				.toMillis();
	}

	@Override
	public Transaction deserialize(byte[] message) throws IOException {
		String line = new String(message, StandardCharsets.UTF_8);
		String[] parts = line.split(",");
		return new Transaction(Integer.parseInt(parts[0]),
				parts[1],
				parts[2],
				getTimeInMillis(parts[3]),
				Double.parseDouble(parts[4])
		);
	}

	@Override
	public boolean isEndOfStream(Transaction transaction) {
		return false;
	}

	@Override
	public TypeInformation<Transaction> getProducedType() {
		return TypeInformation.of(Transaction.class);
	}
}
