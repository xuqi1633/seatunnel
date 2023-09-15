package org.apache.seatunnel.connectors.seatunnel.file.alluxio.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import java.util.HashMap;

/**
 * @author xuqi
 * @date 2023/9/13
 */
public class AlluxioConf extends HadoopConf {
    private static final String HDFS_IMPL = "alluxio.hadoop.FileSystem";
    private static final String SCHEMA = "alluxio";

    public AlluxioConf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    @Override
    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    @Override
    public String getSchema() {
        return SCHEMA;
    }

    public static HadoopConf buildWithConfig(Config config) {
        AlluxioConf hadoopConf = new AlluxioConf(config.getString(AlluxioConfig.ALLUXIO_URI.key()));
        HashMap<String, String> alluxioOptions = new HashMap<>();
        alluxioOptions.put(
                "fs.AbstractFileSystem.alluxio.impl", "alluxio.hadoop.AlluxioFileSystem");
        alluxioOptions.put("fs.alluxio.impl", "alluxio.hadoop.FileSystem");
        if (CheckConfigUtil.isValidParam(config, AlluxioConfig.ALLUXIO_CONFIG_PREFIX.key())) {
            config.getObject(AlluxioConfig.ALLUXIO_CONFIG_PREFIX.key())
                    .forEach(
                            (key, value) -> {
                                final String configKey = key.toLowerCase();
                                alluxioOptions.put(configKey, value.unwrapped().toString());
                            });
        }
        hadoopConf.setExtraOptions(alluxioOptions);
        return hadoopConf;
    }
}
