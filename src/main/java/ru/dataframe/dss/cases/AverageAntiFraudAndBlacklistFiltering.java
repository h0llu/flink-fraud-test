package ru.dataframe.dss.cases;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.dataframe.dss.average.AverageAccumulator;
import ru.dataframe.dss.average.AverageFraudCheckingFunction;
import ru.dataframe.dss.blacklist.FilterBlacklistedFunction;
import ru.dataframe.dss.dto.Transaction;
import ru.dataframe.dss.utils.ConnectorProvider;

import java.io.FileReader;
import java.util.Properties;


public class AverageAntiFraudAndBlacklistFiltering {
	private static final String configPath = "/home/h0llu/everything/internship/dss-system/" +
			"flink-fraud-test/src/main/resources/config.properties";
	private static final String blacklistFilePath =
			"/home/h0llu/everything/internship/dss-system/" +
					"flink-fraud-test/src/main/resources/blacklist.txt";

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
				ConnectorProvider.getSource(sourceTopic, sourceBootstrapServer),
				WatermarkStrategy.<Transaction>forMonotonousTimestamps()
						.withTimestampAssigner((transaction, timestamp) -> transaction.getEventTime()),
				"Transaction Source"
		);

		dataStream.keyBy(Transaction::getClientId)
				.process(new FilterBlacklistedFunction(blacklistFilePath))
				.keyBy(Transaction::getClientId)
				.process(new AverageFraudCheckingFunction(windowDuration, new AverageAccumulator()))
				.map(String::valueOf)
				.sinkTo(ConnectorProvider.getSink(sinkTopic, sinkBootstrapServer));

		env.execute();
	}
}
