package org.example;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;


import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class MySinkTask extends SinkTask {
    private CSVSinkConnectorConfig connectorConfig;
    private Map configProps;

    @Override
    public String version() {
        return "1";
    }

    @Override
    public void start(Map<String, String> props) {
        this.connectorConfig = new CSVSinkConnectorConfig(props);
        this.configProps = Collections.unmodifiableMap(props);
    }

    @Override
    public void put(Collection<SinkRecord> records) {

        try {
            FileWriter fw = new FileWriter(connectorConfig.getString("output.file"));
            for (SinkRecord record : records) {
                fw.write(record.value().toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void stop() {

    }
}
