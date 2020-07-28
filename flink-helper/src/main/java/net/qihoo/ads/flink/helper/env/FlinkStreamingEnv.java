package net.qihoo.ads.flink.helper.env;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class FlinkStreamingEnv {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkStreamingEnv.class);

    public static StreamExecutionEnvironment init(String[] args, String jobPropertiesFileName) throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        InputStream jobConfFile = classLoader.getResourceAsStream("conf/" + jobPropertiesFileName);
        if (jobConfFile == null) {
            throw new IOException("Properties file conf/" + jobPropertiesFileName + " does not exist");
        } else {

            ParameterTool jobDefault = ParameterTool.fromPropertiesFile(classLoader.getResourceAsStream("conf/flinkjob-default.properties"));
            ParameterTool jobSpecific = ParameterTool.fromPropertiesFile(jobConfFile);
            ParameterTool argsParam = ParameterTool.fromArgs(args);

            // merge all conf, the priority order is argsParam > jobSpecific > jobDefault
            Configuration jobConf = jobDefault.mergeWith(jobSpecific).mergeWith(argsParam).getConfiguration();
            Configuration conf = GlobalConfiguration.loadConfiguration(jobConf);
            LOG.info("Global Configuration: " + conf.toString());

            StreamExecutionEnvironment env = conf.getBoolean("flinkjob.local", false) ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf) : StreamExecutionEnvironment.getExecutionEnvironment();
            env.getConfig().setGlobalJobParameters(conf);

            env.setParallelism(conf.getInteger(CoreOptions.DEFAULT_PARALLELISM, 1));
            env.setStreamTimeCharacteristic(TimeCharacteristic.valueOf(conf.getString("flinkjob.timeCharacteristic", "EventTime")));

            if (conf.getBoolean("flinkjob.checkpoint.config.enabled", true)) {
                env.enableCheckpointing(conf.getLong("flinkjob.checkpoint.config.interval", 600000L), CheckpointingMode.valueOf(conf.getString("flinkjob.checkpoint.config.mode", "EXACTLY_ONCE")));
                env.getCheckpointConfig().setMaxConcurrentCheckpoints(conf.getInteger("flinkjob.checkpoint.config.maxConcurrentCheckpoints", CheckpointConfig.DEFAULT_MAX_CONCURRENT_CHECKPOINTS));
                env.getCheckpointConfig().setMinPauseBetweenCheckpoints(conf.getLong("flinkjob.checkpoint.config.minPauseBetweenCheckpoints", CheckpointConfig.DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS));
                env.getCheckpointConfig().setCheckpointTimeout(conf.getLong("flinkjob.checkpoint.config.timeout", CheckpointConfig.DEFAULT_TIMEOUT));

                env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
                env.getCheckpointConfig().setPreferCheckpointForRecovery(conf.getBoolean("flinkjob.checkpoint.config.preferCheckpointForRecovery", false));
                env.getCheckpointConfig().setTolerableCheckpointFailureNumber(conf.getInteger("flinkjob.checkpoint.config.tolerableCheckpointFailureNumber", 3));
            }

            return env;
        }
    }
}
