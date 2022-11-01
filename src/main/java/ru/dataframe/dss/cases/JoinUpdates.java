package ru.dataframe.dss.cases;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.dataframe.dss.blacklist.DynamicBlacklistFunction;
import ru.dataframe.dss.dto.BlacklistItem;
import ru.dataframe.dss.dto.Transaction;
import ru.dataframe.dss.serialization.BlacklistItemDeserializationSchema;
import ru.dataframe.dss.serialization.TransactionDeserializationSchema;
import ru.dataframe.dss.utils.ConfigLoader;
import ru.dataframe.dss.utils.FlinkProvider;

import java.util.Properties;

public class JoinUpdates {
	public static void main(String[] args) throws Exception {
		Properties config = ConfigLoader.load("join_updates_config.properties");


		String bootstrapServer = config.getProperty("bootstrap-server");
		String transactionSourceTopic = config.getProperty("transaction_source.topic");
		String blacklistSourceTopic = config.getProperty("blacklist_source.topic");
		String sinkTopic = config.getProperty("sink.topic");
		Time windowDuration = Time.seconds(Long.parseLong(config.getProperty("window-duration")));

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Transaction> transactionDataStream = FlinkProvider.getDataStream(env,
				transactionSourceTopic,
				bootstrapServer,
				new TransactionDeserializationSchema(),
				WatermarkStrategy.<Transaction>forMonotonousTimestamps()
						.withTimestampAssigner((transaction, timestamp) -> transaction.getEventTime()),
				"Transaction Source"
		);

		DataStream<BlacklistItem> blacklistUpdateStream = FlinkProvider.getDataStream(env,
				blacklistSourceTopic,
				bootstrapServer,
				new BlacklistItemDeserializationSchema(),
				WatermarkStrategy.noWatermarks(),
				"Blacklist Source"
		);

		MapStateDescriptor<String, BlacklistItem> descriptor =
				new MapStateDescriptor<>("blacklist", String.class, BlacklistItem.class);
		BroadcastStream<BlacklistItem> blacklistStream =
				blacklistUpdateStream.broadcast(descriptor);

		transactionDataStream.keyBy(Transaction::getClientId)
				.connect(blacklistStream)
				.process(new DynamicBlacklistFunction(descriptor))
				.map(String::valueOf)
				.sinkTo(FlinkProvider.getSink(sinkTopic,
						bootstrapServer,
						new SimpleStringSchema()
				));

		env.execute();
	}
}
