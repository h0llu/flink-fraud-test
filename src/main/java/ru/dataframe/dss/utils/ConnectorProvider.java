package ru.dataframe.dss.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import ru.dataframe.dss.dto.Transaction;
import ru.dataframe.dss.serialization.TransactionDeserializationSchema;

public class ConnectorProvider {
	public static KafkaSource<Transaction> getSource(String topic, String bootstrapServer) {
		return KafkaSource.<Transaction>builder()
				.setBootstrapServers(bootstrapServer)
				.setTopics(topic)
				.setValueOnlyDeserializer(new TransactionDeserializationSchema())
				.build();
	}

	public static KafkaSink<String> getSink(String topic, String bootstrapServer) {
		return KafkaSink.<String>builder()
				.setBootstrapServers(bootstrapServer)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(topic)
						.setValueSerializationSchema(new SimpleStringSchema())
						.build())
				.setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();
	}
}
