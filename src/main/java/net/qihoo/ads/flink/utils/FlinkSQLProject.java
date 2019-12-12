package net.qihoo.ads.flink.utils;

public class FlinkSQLProject {
    //必要参数
    private final String name;
    //可选参数
    private KafkaConsumerInfo kafkaConsumerInfo;
    private KafkaProducerInfo kafkaProducerInfo;

    public String getName() {
        return name;
    }

    public KafkaConsumerInfo getKafkaConsumerInfo() {
        return kafkaConsumerInfo;
    }

    public KafkaProducerInfo getKafkaProducerInfo() {
        return kafkaProducerInfo;
    }


    private FlinkSQLProject(Builder builder) {
        this.name = builder.name;
        this.kafkaConsumerInfo = builder.kafkaConsumerInfo;
        this.kafkaProducerInfo = builder.kafkaProducerInfo;
    }


    public static class Builder{

        //必要参数
        private final String name;
        //可选参数
        private KafkaConsumerInfo kafkaConsumerInfo;
        private KafkaProducerInfo kafkaProducerInfo;

        public Builder(String name) {
            this.name = name;
        }

        public Builder kafkaConsumer(KafkaConsumerInfo val) {
            this.kafkaConsumerInfo = val;
            return this;
        }

        public Builder kafkaProducer(KafkaProducerInfo val) {
            this.kafkaProducerInfo = val;
            return this;
        }

        public FlinkSQLProject build() {
            return new FlinkSQLProject(this);
        }
    }


}
