package org.example;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
        System.out.println("hellofromtheotherside");
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        String outfile = connectorConfig.getString("output.file");
        try(FileWriter fw = new FileWriter(outfile, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw))
        {
            for (SinkRecord record : records){
                String[] strs = record.value().toString().replace("}","").split(",");
                StringBuilder sb = new StringBuilder();
                for (String s : strs){
                    sb.append(s.split("=")[1]+",");
                }
                out.println(sb);
                System.out.println(sb);
            }
            //more code
        } catch (IOException e) {
            //exception handling left as an exercise for the reader
        }

    }

    @Override
    public void stop() {

    }
}
