package org.example;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class CSVSinkConnectorConfig extends AbstractConfig
{
    public CSVSinkConnectorConfig(Map originals) {
        super(configDef(), originals);
    }
    protected static ConfigDef configDef() {
        return new ConfigDef()
                .define("output.file",
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH,
                        "Name of sink file")
                .define("append",
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.HIGH,
                        "Append or overwrite");
    }
}
