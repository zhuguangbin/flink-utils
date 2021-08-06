package com.aerospike.jdbc.util;

import com.aerospike.client.Host;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.jdbc.scan.EventLoopProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class URLParser {
    private static final Logger logger = LoggerFactory.getLogger(URLParser.class);

    private static final String defaultAerospikePort = "3000";
    private static final int defaultRecordsPerSecond = 512;

    private static final Pattern AS_JDBC_URL = Pattern.compile("^jdbc:aerospike:(?://)?([^/?]+)");
    private static final Pattern AS_JDBC_SCHEMA = Pattern.compile("/([^?]+)");

    private Host[] hosts;
    private String schema;
    private Properties clientInfo;
    private ClientPolicy clientPolicy;
    private WritePolicy writePolicy;
    private ScanPolicy scanPolicy;

    public Host[] getHosts() {
        return hosts;
    }

    public String getSchema() {
        return schema;
    }

    public Properties getClientInfo() {
        return clientInfo;
    }

    public ClientPolicy getClientPolicy() {
        return clientPolicy;
    }

    public WritePolicy getWritePolicy() {
        return writePolicy;
    }

    public ScanPolicy getScanPolicy() {
        return scanPolicy;
    }

    public static URLParser parseUrl(String url, Properties props) {
        logger.info("URL properties: " + props);
        URLParser up = new URLParser();
        up.hosts = parseHosts(url);
        up.schema = parseSchema(url);
        up.clientInfo = parseClientInfo(url, props);
        up.clientPolicy = copy(up.clientInfo, new ClientPolicy());
        up.clientPolicy.eventLoops = EventLoopProvider.getEventLoops();

        up.writePolicy = copy(up.clientInfo, new WritePolicy());
        up.scanPolicy = copy(up.clientInfo, new ScanPolicy());
        if (up.scanPolicy.recordsPerSecond == 0) {
            up.scanPolicy.recordsPerSecond = defaultRecordsPerSecond;
        }
        Value.UseBoolBin = Optional.ofNullable(props.getProperty("useBoolBin"))
                .map(Boolean::parseBoolean).orElse(false);
        logger.info("Value.UseBoolBin = " + Value.UseBoolBin);
        return up;
    }

    public static <T> T copy(Properties props, T object) {
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) object.getClass();
        props.forEach((key, value) -> {
            try {
                clazz.getField((String) key).set(object, value);
            } catch (ReflectiveOperationException e1) {
                // ignore it; this property does not belong to the object
            }
        });
        return object;
    }

    private static Host[] parseHosts(String url) {
        Matcher m = AS_JDBC_URL.matcher(url);
        if (!m.find()) {
            throw new IllegalArgumentException("Cannot parse URL " + url);
        }
        return Arrays.stream(m.group(1).split(","))
                .map(p -> p.split(":"))
                .map(a -> a.length > 1 ? a : new String[]{a[0], defaultAerospikePort})
                .map(hostPort -> new Host(hostPort[0], Integer.parseInt(hostPort[1])))
                .toArray(Host[]::new);
    }

    private static String parseSchema(String url) {
        Matcher m = AS_JDBC_SCHEMA.matcher(url);
        return m.find() ? m.group(1) : null;
    }

    private static Properties parseClientInfo(String url, Properties props) {
        Properties all = new Properties();
        all.putAll(props);
        int questionPos = url.indexOf('?');
        if (questionPos > 0 && questionPos < url.length() - 1) {
            Arrays.stream(url.substring(questionPos + 1).split("&")).forEach(p -> {
                String[] kv = p.split("=");
                if (kv.length > 1) {
                    all.setProperty(kv[0], kv[1]);
                }
            });
        }
        return all;
    }
}
