package org.example;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.log4j.Logger;

import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.log4j.PropertyConfigurator;

import java.io.InputStream;
import java.util.*;


public class MyConnector extends SinkConnector {

    private static final Logger logger = Logger.getLogger(MyConnector.class);
    private CSVSinkConnectorConfig connectorConfig;
    private Map configProps;
    InputStream log4jProps = getClass().getClassLoader().getResourceAsStream("log4j.properties");

    @Override
    public void start(Map<String, String> props) {
        PropertyConfigurator.configure(log4jProps);

        this.connectorConfig = new CSVSinkConnectorConfig(props);
        this.configProps = Collections.unmodifiableMap(props);

    }

    @Override
    public Class taskClass() {
        return MySinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {

        List<Map<String, String>> taskConfigs = new ArrayList<>(1);
        HashMap hash = new HashMap<>();
        hash.put("output.file", configProps.get("output.file"));
        taskConfigs.add(hash);

        return taskConfigs;
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return CSVSinkConnectorConfig.configDef();
    }

    @Override
    public String version() {
        return "1";
    }
}
