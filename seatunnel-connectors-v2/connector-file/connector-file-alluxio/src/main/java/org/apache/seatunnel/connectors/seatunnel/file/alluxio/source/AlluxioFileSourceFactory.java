package org.apache.seatunnel.connectors.seatunnel.file.alluxio.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.file.alluxio.config.AlluxioConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;

import com.google.auto.service.AutoService;

import java.util.Arrays;

/**
 * @author xuqi
 * @date 2023/9/14
 */
@AutoService(Factory.class)
public class AlluxioFileSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return FileSystemType.ALLUXIO.getFileSystemPluginName();
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(AlluxioConfig.ALLUXIO_URI)
                .optional(AlluxioConfig.ALLUXIO_CONFIG_PREFIX)
                .required(BaseSourceConfig.FILE_FORMAT_TYPE)
                .conditional(
                        BaseSourceConfig.FILE_FORMAT_TYPE,
                        FileFormat.TEXT,
                        BaseSourceConfig.FIELD_DELIMITER)
                .conditional(
                        BaseSourceConfig.FILE_FORMAT_TYPE,
                        Arrays.asList(
                                FileFormat.TEXT, FileFormat.JSON, FileFormat.EXCEL, FileFormat.CSV),
                        TableSchemaOptions.SCHEMA)
                .optional(BaseSourceConfig.PARSE_PARTITION_FROM_PATH)
                .optional(BaseSourceConfig.DATE_FORMAT)
                .optional(BaseSourceConfig.DATETIME_FORMAT)
                .optional(BaseSourceConfig.TIME_FORMAT)
                .optional(BaseSourceConfig.FILE_FILTER_PATTERN)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AlluxioFileSource.class;
    }
}
