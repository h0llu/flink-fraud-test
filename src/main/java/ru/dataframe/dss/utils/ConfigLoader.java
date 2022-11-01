package ru.dataframe.dss.utils;

import java.util.Properties;

public class ConfigLoader {
	public static Properties load(String configName) {
		Properties config = new Properties();
		try {
			config.load(ConfigLoader.class.getClassLoader()
					.getResourceAsStream(configName));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return config;
	}
}
