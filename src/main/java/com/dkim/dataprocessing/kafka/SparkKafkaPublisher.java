package com.dkim.dataprocessing.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Properties;
import static org.apache.spark.sql.functions.*;

public class SparkKafkaPublisher {

    private final String kafkaBrokers;
    private final Properties kafkaProps;

    public SparkKafkaPublisher(String kafkaBrokers) {
        this.kafkaBrokers = kafkaBrokers;
        this.kafkaProps = createKafkaProperties();
    }

    /**
     * 기본 Kafka 설정
     */
    private Properties createKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBrokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        return props;
    }

    public void publishBatchToKafka(SparkSession session, Dataset<Row> dataset, String topicName) {
        try {
            System.out.println("=== 배치 모드 Kafka 발행 시작 ===");
            System.out.println("Topic: " + topicName);
            System.out.println("Records: " + dataset.count());

            Dataset<Row> kafkaData = prepareKafkaMessage(dataset, topicName);

            kafkaData.write()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaBrokers)
                .option("topic", topicName)
                .save();

            System.out.println("배치 Kafka 발행 완료");

        } catch (Exception e) {
            System.err.println("배치 Kafka 발행 실패: " + e.getMessage());
            throw new RuntimeException("Kafka 발행 실패", e);
        }
    }





    // === 헬퍼 메서드들 ===

    /**
     * Dataset을 Kafka 메시지 형태로 변환
     */
    private Dataset<Row> prepareKafkaMessage(Dataset<Row> dataset, String topicName) {
        return dataset
            .withColumn("kafka_timestamp", current_timestamp())
            .withColumn("kafka_topic", lit(topicName))
            .withColumn("value", to_json(struct(col("*"))))
            .select(col("value"));
    }


    /**
     * 배치별 Kafka 처리
     */
    private void processBatchForKafka(SparkSession session, Dataset<Row> batchDF,
                                      long batchId, String topicName) {
        try {
            System.out.println("배치 " + batchId + " Kafka 발행: " + batchDF.count() + "개 레코드");

            if (batchDF.isEmpty()) {
                return;
            }

            // 배치 메타데이터 추가
            Dataset<Row> enrichedBatch = batchDF
                .withColumn("batch_id", lit(batchId))
                .withColumn("processing_time", current_timestamp())
                .withColumn("record_count", lit(batchDF.count()));

            // Kafka로 발행
            publishBatchToKafka(session, enrichedBatch, topicName);

            // 성공 로깅
            System.out.println("배치 " + batchId + " Kafka 발행 완료");

        } catch (Exception e) {
            System.err.println("배치 " + batchId + " Kafka 발행 실패: " + e.getMessage());

            // 실패한 배치를 에러 토픽으로 발행
            try {
                Dataset<Row> errorBatch = batchDF
                    .withColumn("error_batch_id", lit(batchId))
                    .withColumn("error_message", lit(e.getMessage()))
                    .withColumn("error_timestamp", current_timestamp());

                publishBatchToKafka(session, errorBatch, topicName + "_batch_errors");
            } catch (Exception errorPublishException) {
                System.err.println("에러 배치 발행도 실패: " + errorPublishException.getMessage());
            }
        }
    }

    /**
     * 데이터셋 검증
     */
    private void validateDataset(Dataset<Row> dataset) {
        if (dataset.isEmpty()) {
            throw new IllegalArgumentException("Dataset이 비어있습니다");
        }

        long nullIdCount = dataset.filter(col("id").isNull()).count();
        if (nullIdCount > 0) {
            System.out.println("경고: NULL ID가 " + nullIdCount + "개 있습니다");
        }
    }

    /**
     * Kafka 연결 테스트
     */
    public boolean testKafkaConnection(SparkSession session) {
        try {
            // 테스트 메시지 발행
            Dataset<Row> testData = session.createDataFrame(
                java.util.Arrays.asList(
                    org.apache.spark.sql.RowFactory.create("test", "connection_test",
                        java.sql.Timestamp.valueOf("2023-01-01 00:00:00"))
                ),
                new StructType()
                    .add("id", DataTypes.StringType)
                    .add("message", DataTypes.StringType)
                    .add("timestamp", DataTypes.TimestampType)
            );

            publishBatchToKafka(session, testData, "test_topic");
            System.out.println("Kafka 연결 테스트 성공");
            return true;

        } catch (Exception e) {
            System.err.println("Kafka 연결 테스트 실패: " + e.getMessage());
            return false;
        }
    }
}