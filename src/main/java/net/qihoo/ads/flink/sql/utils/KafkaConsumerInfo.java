package net.qihoo.ads.flink.sql.utils;

public class KafkaConsumerInfo {
    //必要参数
    private final String cluster;
    private final String consumerName;
    private final String apiSecret;
    //可选参数
    private String desc;

    public String getCluster() {
        return cluster;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public String getApiSecret() {
        return apiSecret;
    }

    public String getDesc() {
        return desc;
    }

    private KafkaConsumerInfo(Builder builder) {
        this.cluster = builder.cluster;
        this.consumerName = builder.consumerName;
        this.apiSecret = builder.apiSecret;
        this.desc = builder.desc;
    }


    public static class Builder{

        //必要参数
        private final String cluster;
        private final String consumerName;
        private final String apiSecret;
        //可选参数
        private String desc;

        public Builder(String cluster, String consumerName, String apiSecret) {
            this.cluster = cluster;
            this.consumerName = consumerName;
            this.apiSecret = apiSecret;
        }

        public Builder desc(String val) {
            this.desc = val;
            return this;
        }

        public KafkaConsumerInfo build() {
            return new KafkaConsumerInfo(this);
        }
    }


}
