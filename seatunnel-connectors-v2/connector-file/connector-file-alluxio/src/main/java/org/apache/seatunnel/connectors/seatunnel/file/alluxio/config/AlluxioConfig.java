package org.apache.seatunnel.connectors.seatunnel.file.alluxio.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;

import java.util.Map;

/**
 * @author xuqi
 * @date 2023/9/13
 */
public class AlluxioConfig extends BaseSourceConfig {
    public static final Option<String> ALLUXIO_URI =
            Options.key("alluxio_uri").stringType().noDefaultValue().withDescription("alluxio uri");

    public static final Option<Map<String, String>> ALLUXIO_CONFIG_PREFIX =
            Options.key("alluxio.config")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("alluxio HA config");
}
