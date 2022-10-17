package ru.dataframe.dss;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileReader;
import java.util.Properties;


public class FraudTesting {
	private static final String configPath = "src/main/resources/config.properties";

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

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.load(new FileReader(configPath));

		String sourceTopic = props.getProperty("source.topic");
		String sourceBootstrapServer = props.getProperty("source.bootstrap-server");
		String sinkTopic = props.getProperty("sink.topic");
		String sinkBootstrapServer = props.getProperty("sink.bootstrap-server");
		Time windowDuration = Time.seconds(Long.parseLong(props.getProperty("window-duration")));

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Transaction> dataStream = env.fromSource(
				getSource(sourceTopic, sourceBootstrapServer),
				WatermarkStrategy.<Transaction>forMonotonousTimestamps()
						.withTimestampAssigner((transaction, timestamp) -> transaction.getEventTime()),
				"Transaction Source"
		);
		dataStream.print();
		dataStream.keyBy(Transaction::getClientId)
				.process(new CustomProcessFunction(windowDuration, new AverageAccumulator()))
				.map(String::valueOf)
				.sinkTo(getSink(sinkTopic, sinkBootstrapServer));
		env.execute();
	}
}
