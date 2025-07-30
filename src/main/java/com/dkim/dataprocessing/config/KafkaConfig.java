package com.dkim.dataprocessing.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaConfig {

    public static Properties getProductionProducerConfig(String bootstrapServers) {
        Properties props = new Properties();

        // 필수 설정
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 내구성 및 신뢰성 설정
        props.put("acks", "all");                    // 모든 리플리카에서 확인
        props.put("retries", Integer.MAX_VALUE);     // 무제한 재시도
        props.put("max.in.flight.requests.per.connection", 1);  // 순서 보장
        props.put("enable.idempotence", true);       // 중복 방지

        // 성능 최적화 설정
        props.put("batch.size", 32768);              // 32KB 배치
        props.put("linger.ms", 10);                  // 10ms 대기
        props.put("buffer.memory", 67108864);        // 64MB 버퍼
        props.put("compression.type", "snappy");     // 압축 활성화

        // 타임아웃 설정
        props.put("request.timeout.ms", 30000);      // 30초
        props.put("delivery.timeout.ms", 120000);    // 2분

        return props;
    }

    /**
     * 개발 환경용 Kafka Producer 설정
     */
    public static Properties getDevelopmentProducerConfig(String bootstrapServers) {
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 개발 환경용 빠른 설정
        props.put("acks", "1");                      // 리더만 확인
        props.put("retries", 3);                     // 3번 재시도
        props.put("batch.size", 16384);              // 16KB 배치
        props.put("linger.ms", 1);                   // 1ms 대기
        props.put("buffer.memory", 33554432);        // 32MB 버퍼

        return props;
    }

    /**
     * Spark Kafka Writer 옵션 맵
     */
    public static Map<String, String> getSparkKafkaWriterOptions(String bootstrapServers) {
        Map<String, String> options = new HashMap<>();

        // 기본 연결 설정
        options.put("kafka.bootstrap.servers", bootstrapServers);

        // 성능 설정
        options.put("kafka.acks", "all");
        options.put("kafka.retries", "3");
        options.put("kafka.batch.size", "32768");
        options.put("kafka.linger.ms", "10");
        options.put("kafka.compression.type", "snappy");

        // 보안 설정 (필요시)
        // options.put("kafka.security.protocol", "SASL_SSL");
        // options.put("kafka.sasl.mechanism", "PLAIN");

        return options;
    }

    /**
     * 스트리밍용 Kafka 설정
     */
    public static Map<String, String> getStreamingKafkaOptions(String bootstrapServers, String checkpointLocation) {
        Map<String, String> options = getSparkKafkaWriterOptions(bootstrapServers);

        // 스트리밍 특화 설정
        options.put("checkpointLocation", checkpointLocation);
        options.put("kafka.max.request.size", "10485760");  // 10MB
        options.put("kafka.buffer.memory", "67108864");     // 64MB

        return options;
    }

    /**
     * 환경별 Kafka 클러스터 설정
     */
    public enum Environment {
        DEVELOPMENT("localhost:9092", 1, 1),
        STAGING("staging-kafka-1:9092,staging-kafka-2:9092", 2, 2),
        PRODUCTION("prod-kafka-1:9092,prod-kafka-2:9092,prod-kafka-3:9092", 3, 2);

        private final String bootstrapServers;
        private final int replicationFactor;
        private final int minInSyncReplicas;

        Environment(String bootstrapServers, int replicationFactor, int minInSyncReplicas) {
            this.bootstrapServers = bootstrapServers;
            this.replicationFactor = replicationFactor;
            this.minInSyncReplicas = minInSyncReplicas;
        }

        public String getBootstrapServers() { return bootstrapServers; }
        public int getReplicationFactor() { return replicationFactor; }
        public int getMinInSyncReplicas() { return minInSyncReplicas; }
    }


    // === 보안 설정 ===

    /**
     * SASL/SSL 보안 설정
     */
    public static Properties getSecureProducerConfig(String bootstrapServers,
                                                     String username, String password) {
        Properties props = getProductionProducerConfig(bootstrapServers);

        // SASL 설정
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
            String.format("org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"%s\" password=\"%s\";", username, password));

        // SSL 설정
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("ssl.truststore.location", "/path/to/kafka.client.truststore.jks");
        props.put("ssl.truststore.password", "truststore_password");

        return props;
    }
}
