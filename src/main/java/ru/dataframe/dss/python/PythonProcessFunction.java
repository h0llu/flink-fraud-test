package ru.dataframe.dss.python;

import jep.Interpreter;
import jep.JepException;
import jep.SharedInterpreter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;
import ru.dataframe.dss.dto.Transaction;

public class PythonProcessFunction extends KeyedProcessFunction<String, Transaction, Transaction> {
	private Interpreter interpreter;

	private static Transaction sendRestRequest(String jsonedTransaction) {
		RestTemplate restTemplate = new RestTemplate();
		String url = "http://127.0.0.1:8000/multiply";
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<String> request = new HttpEntity<>(jsonedTransaction, headers);
		return Transaction.fromJson(restTemplate.postForObject(url, request, String.class));
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		interpreter = new SharedInterpreter();
	}

	private Transaction runPython(Transaction transaction) {
		try {
			interpreter.set("amount", transaction.getAmount());
			interpreter.runScript(
					"/home/h0llu/everything/internship/dss-system/flink-tests/src/main/resources" +
							"/script.py");
			transaction.setClientAvg((Double) interpreter.getValue("amount"));
			return transaction;
		} catch (JepException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void processElement(Transaction transaction,
							   KeyedProcessFunction<String, Transaction, Transaction>.Context context,
							   Collector<Transaction> collector) throws Exception {
//		collector.collect(sendRestRequest(transaction.toJson()));
		collector.collect(runPython(transaction));
	}
}
