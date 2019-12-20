package net.qihoo.ads.flink.sql.utils;

public class KafkaProducerInfo {
    //必要参数
    private final String cluster;
    private final String producerName;
    private final String apiSecret;
    //可选参数
    private String desc;

    public String getCluster() {
        return cluster;
    }

    public String getProducerName() {
        return producerName;
    }

    public String getApiSecret() {
        return apiSecret;
    }

    public String getDesc() {
        return desc;
    }

    private KafkaProducerInfo(Builder builder) {
        this.cluster = builder.cluster;
        this.producerName = builder.producerName;
        this.apiSecret = builder.apiSecret;
        this.desc = builder.desc;
    }


    public static class Builder{

        //必要参数
        private final String cluster;
        private final String producerName;
        private final String apiSecret;
        //可选参数
        private String desc;

        public Builder(String cluster, String producerName, String apiSecret) {
            this.cluster = cluster;
            this.producerName = producerName;
            this.apiSecret = apiSecret;
        }

        public Builder desc(String val) {
            this.desc = val;
            return this;
        }

        public KafkaProducerInfo build() {
            return new KafkaProducerInfo(this);
        }
    }


}
