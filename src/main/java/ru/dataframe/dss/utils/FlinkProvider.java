package ru.dataframe.dss.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkProvider {
	public static <T> DataStream<T> getDataStream(StreamExecutionEnvironment env,
												  String topic,
												  String bootstrapServer,
												  DeserializationSchema<T> deserializationSchema,
												  WatermarkStrategy<T> watermarkStrategy,
												  String sourceName) {
		return env.fromSource(
				getSource(topic, bootstrapServer, deserializationSchema),
				watermarkStrategy,
				sourceName
		);
	}

	public static <T> KafkaSource<T> getSource(String topic,
											   String bootstrapServer,
											   DeserializationSchema<T> deserializationSchema) {
		return KafkaSource.<T>builder()
				.setBootstrapServers(bootstrapServer)
				.setTopics(topic)
				.setValueOnlyDeserializer(deserializationSchema)
				.build();
	}

	public static <T> KafkaSink<T> getSink(String topic,
										   String bootstrapServer,
										   SerializationSchema<T> serializationSchema) {
		return KafkaSink.<T>builder()
				.setBootstrapServers(bootstrapServer)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(topic)
						.setValueSerializationSchema(serializationSchema)
						.build())
				.setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();
	}
}
