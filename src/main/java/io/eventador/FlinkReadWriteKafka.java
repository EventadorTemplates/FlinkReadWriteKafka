package io.eventador;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class FlinkReadWriteKafka {

    private static final int CHECKPOINT_INTERVAL = 300000; // 300 seconds
    private static final int RESTART_DELAY = 10000;
    private static final int RESTART_ATTEMPTS = 4;
    private static final String READ_TOPIC_PARAM = "read-topic";
    private static final String WRITE_TOPIC_PARAM = "write-topic";


    public static void main(String[] args) throws Exception {
        // Read parameters from command line
        final ParameterTool params = ParameterTool.fromArgs(args);

        if (params.getNumberOfParameters() < 4) {
            System.out.println("\nUsage: FlinkReadKafka --read-topic <topic> --write-topic <topic> --bootstrap.servers <kafka brokers> --group.id <groupid>");
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(RESTART_ATTEMPTS, RESTART_DELAY));
        env.enableCheckpointing(CHECKPOINT_INTERVAL); // 300 seconds
        env.getConfig().setGlobalJobParameters(params);

        Properties kafkaProperties = params.getProperties();
        String sourceTopic = params.getRequired(READ_TOPIC_PARAM);
        String writeTopic  = params.getRequired(WRITE_TOPIC_PARAM);


        DataStream<String> messageStream = env.addSource(createFlinkKafkaConsumer(sourceTopic, kafkaProperties))
                                              .name("Read from Kafka");

        // Print Kafka messages to stdout - will be visible in logs
        messageStream.print();


        // Write payload back to Kafka topic
        messageStream.addSink(createFlinkKafkaProducer(writeTopic, kafkaProperties))
                     .name("Write To Kafka");

        env.execute("FlinkReadWriteKafka");
    }



    private static FlinkKafkaConsumer createFlinkKafkaConsumer(String sourceTopic, Properties kafkaProperties){
        return new FlinkKafkaConsumer(sourceTopic, new SimpleStringSchema(), kafkaProperties);
    }

    private static FlinkKafkaProducer createFlinkKafkaProducer(String writeTopic, Properties kafkaProperties){
        return new FlinkKafkaProducer<String>(
                writeTopic,
                new ProducerStringSerializationSchema(writeTopic),
                kafkaProperties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );
    }


    private static class ProducerStringSerializationSchema implements KafkaSerializationSchema<String>{

        private String topic;

        public ProducerStringSerializationSchema(String topic) {
            super();
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
            return new ProducerRecord<byte[], byte[]>(topic, element.getBytes(StandardCharsets.UTF_8));
        }

    }


}
