package org.apache.flink.table.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.sinks.TableSink;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;
import static org.apache.flink.table.filesystem.FileSystemOptions.PARTITION_DEFAULT_NAME;
import static org.apache.flink.table.filesystem.FileSystemOptions.PATH;

public class ConfigurableFileSystemTableFactory extends FileSystemTableFactory{

    public static final String IDENTIFIER = "filesystem-configurable";
    public static final String SINK_FILE_PATTERN_PREFIX = "sink.file-pattern.prefix";
    public static final String SINK_FILE_PATTERN_SUFFIX = "sink.file-pattern.suffix";

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CONNECTOR, IDENTIFIER);
        return context;
    }

    @Override
    public TableSink<RowData> createTableSink(TableSinkFactory.Context context) {
        Configuration conf = new Configuration();
        context.getTable().getOptions().forEach(conf::setString);

        return new ConfigurableFileSystemTableSink(
                context.getObjectIdentifier(),
                context.isBounded(),
                context.getTable().getSchema(),
                getPath(conf),
                context.getTable().getPartitionKeys(),
                conf.get(PARTITION_DEFAULT_NAME),
                context.getTable().getOptions());
    }

    private static Path getPath(Configuration conf) {
        return new Path(conf.getOptional(PATH).orElseThrow(() ->
                new ValidationException("Path should be not empty.")));
    }
}
