package org.example;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void justRun() throws IOException {
        CSVSInkConnector myconnector = new CSVSInkConnector();
        Properties prop = new Properties();
        String propFileName = "config.properties";

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        prop.load(inputStream);

        myconnector.start((Map)prop);
        System.out.println(CSVSInkConnector.class);

    }
}
