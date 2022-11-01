package ru.dataframe.dss.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import ru.dataframe.dss.dto.BlacklistItem;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BlacklistItemDeserializationSchema implements DeserializationSchema<BlacklistItem> {
	@Override
	public BlacklistItem deserialize(byte[] message) throws IOException {
		String line = new String(message, StandardCharsets.UTF_8);
		String[] parts = line.split(",");
		return new BlacklistItem(Integer.parseInt(parts[0]),
				parts[1],
				Boolean.parseBoolean(parts[2])
		);
	}

	@Override
	public boolean isEndOfStream(BlacklistItem transaction) {
		return false;
	}

	@Override
	public TypeInformation<BlacklistItem> getProducedType() {
		return TypeInformation.of(BlacklistItem.class);
	}
}
