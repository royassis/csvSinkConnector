package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.apache.log4j.Logger;

import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MyConnector extends SinkConnector
{

    private static final Logger logger = Logger.getLogger(MyConnector.class);
    private CSVSinkConnectorConfig connectorConfig;
    private Map configProps;
    String PREFIX_WHITELIST_CONFIG = "prefix.whitelist";
    String TASK_PREFIXES = "prefix.whitelist";
    InputStream log4jProps = getClass().getClassLoader().getResourceAsStream("log4j.properties");

    @Override
    public void start(Map<String, String> props) {
        PropertyConfigurator.configure(log4jProps);

        this.connectorConfig = new CSVSinkConnectorConfig(props);
        this.configProps = Collections.unmodifiableMap(props);

        KafkaConsumer consumer = KafkaConsumerBuilder.build(Arrays.asList(connectorConfig.getString("topic")), "s", "s");


        FileWriter myWriter = null;
        try {
            myWriter = new FileWriter(connectorConfig.getString("output.file"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {
            // pool for records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
            // process records
            for (ConsumerRecord<String, String> record : records) {
                Map<String, Object> map = null;
                try {
                    map = new ObjectMapper().readValue(record.value(), Map.class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                StringBuilder sb = new StringBuilder();
                sb.append(map.get("a") + ",");
                sb.append(map.get("b") + ",");
                sb.append(map.get("c"));
                try {
                    myWriter.write(sb.toString());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
//        try {
//            myWriter.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public Class taskClass() {
        return MyConnector.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List prefixes = connectorConfig.getList(PREFIX_WHITELIST_CONFIG);
        int numGroups = Math.min(prefixes.size(), maxTasks);
        List<List> groupedPrefixes = ConnectorUtils.groupPartitions(prefixes, numGroups);
        List<Map<String, String>> taskConfigs = new ArrayList<>(groupedPrefixes.size());

        for (List taskPrefixes : groupedPrefixes) {
            Map<String, String> taskProps = new HashMap<>(configProps);
            taskProps.put(TASK_PREFIXES, String.join(",", taskPrefixes));
            taskConfigs.add(taskProps);
        }

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
