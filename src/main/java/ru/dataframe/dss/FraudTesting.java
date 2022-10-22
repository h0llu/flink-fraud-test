package ru.dataframe.dss;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;


public class FraudTesting {
	private static final String configPath = "/home/h0llu/everything/internship/dss-system/" +
			"flink-fraud-test/src/main/resources/config.properties";
	private static final String blacklistFilePath =
			"/home/h0llu/everything/internship/dss-system/" +
					"flink-fraud-test/src/main/resources/blacklist.txt";

	public static FileSource<BlacklistItem> getBlacklistSource(File file) {
		CsvMapper mapper = new CsvMapper();
		CsvSchema schema = mapper.schemaFor(BlacklistItem.class)
				.withoutQuoteChar()
				.withColumnSeparator(',');

		CsvReaderFormat<BlacklistItem> csvFormat =
				CsvReaderFormat.forSchema(mapper, schema, TypeInformation.of(BlacklistItem.class));
		return FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(file))
				.build();
	}

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
		DataStream<BlacklistItem> blacklistDataStream = env.fromSource(
				getBlacklistSource(new File(blacklistFilePath)),
				WatermarkStrategy.noWatermarks(),
				"file-source"
		);
		DataStream<Transaction> windowedDataStream = dataStream.keyBy(Transaction::getClientId)
				.process(new CustomProcessFunction(windowDuration, new AverageAccumulator()));

		windowedDataStream.join(blacklistDataStream);

		//				.map(String::valueOf)
//				.sinkTo(getSink(sinkTopic, sinkBootstrapServer));
		env.execute();
	}
}
