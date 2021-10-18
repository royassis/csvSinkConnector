package org.example;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Field;

import java.io.*;
import java.lang.reflect.Array;
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

//        Boolean shouldAppend = connectorConfig.getBoolean("append");
        File outfile = new File(connectorConfig.getString("output.file"));

        try(FileWriter fw = new FileWriter(outfile, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter out = new PrintWriter(bw))
        {
            for (SinkRecord sinkRecord : records){
                String csvLine = kafkaRecordToCsvRecord(sinkRecord);
                out.println(csvLine);
                System.out.println(csvLine);
            }
            //more code
        } catch (IOException e) {
            //exception handling left as an exercise for the reader
        }

    }

    @Override
    public void stop() {

    }

    public String kafkaRecordToCsvRecord(SinkRecord sinkRecord){
        StringJoiner joiner = new StringJoiner(",");

        System.out.println("record value class " +sinkRecord.value().getClass().toString());
        List<Field> schemaFields = ((Struct)sinkRecord.value()).schema().fields();
        System.out.println("----start----");
        for (Field schemaField : schemaFields){
            System.out.println(sinkRecord.value());
            String fieldValue = ((Struct)sinkRecord.value()).get(schemaField.name()).toString();
            joiner.add(fieldValue);
        }
        return joiner.toString();
    }

    public HashSet getColumnsFromRecord(SinkRecord sinkRecord){

        List<Field> schemaFields = ((Struct)sinkRecord.value()).schema().fields();
        HashSet<String> set = new HashSet<>();
        System.out.println("----start----");

        for (Field schemaField : schemaFields){
            System.out.println(sinkRecord.value());
            String fieldName = schemaField.name();
            set.add(fieldName);
        }
        return set;
    }

    public HashSet getColumnsFromCsv(File csvFile) throws FileNotFoundException {

        HashSet<String> set = new HashSet<>();
        Scanner myReader = new Scanner(csvFile);
        String csvColums = myReader.nextLine();
        String[] csvCols = csvColums.split(",");
        for (String col : csvCols){
            set.add(col);
        }
        return set;
    }
}
