package org.apache.seatunnel.connectors.seatunnel.file.alluxio.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.file.alluxio.config.AlluxioConf;
import org.apache.seatunnel.connectors.seatunnel.file.alluxio.config.AlluxioConfig;
import org.apache.seatunnel.connectors.seatunnel.file.alluxio.exception.AlluxioConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.BaseFileSink;

import com.google.auto.service.AutoService;

/**
 * @author xuqi
 * @date 2023/9/13
 */
@AutoService(SeaTunnelSink.class)
public class AlluxioFileSink extends BaseFileSink {

    @Override
    public String getPluginName() {
        return FileSystemType.ALLUXIO.getFileSystemPluginName();
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        super.prepare(pluginConfig);
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        AlluxioConfig.ALLUXIO_URI.key(),
                        AlluxioConfig.FILE_PATH.key());
        if (!result.isSuccess()) {
            throw new AlluxioConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        hadoopConf = AlluxioConf.buildWithConfig(pluginConfig);
    }
}
