package com.plniconplus.kafka.produce.kafka.produce;

import com.plniconplus.kafka.produce.dao.TransDao;
import com.plniconplus.kafka.produce.kafka.serializer.MasterKabupatenSerializer;
import com.plniconplus.kafka.produce.kafka.serializer.MasterProvinsiSerializer;
import com.plniconplus.kafka.produce.model.KirimMasterKabupatenDTO;
import com.plniconplus.kafka.produce.model.KirimMasterProvinsiDTO;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author RR
 * Date: 30/04/2025
 * Time: 15:49
 */

@Configuration
@Service
@Qualifier("kirimKabupatenProducer")
public class MasterKabupatenProduce {
    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String kafkaServer;

    @Value("${kafka.topic.kabupaten}")
    private String kafkaTopic;

    @Autowired
    private final TransDao dataTrans;

    private Producer<String, KirimMasterKabupatenDTO> producer;

    private ExecutorService executorService;
    private static final int RETRY_LIMIT = 5;
    private static final Logger log = LoggerFactory.getLogger(MasterKabupatenProduce.class);

    public MasterKabupatenProduce(TransDao dataTrans) {
        this.dataTrans = dataTrans;
    }

    @PostConstruct
    public void init() {
        try {
            log.info("Initializing Kafka Producer...");

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MasterKabupatenSerializer.class.getName());
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, "5");
            props.put(ProducerConfig.LINGER_MS_CONFIG, "50");
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
            props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");

            this.producer = new KafkaProducer<>(props);
            this.executorService = Executors.newVirtualThreadPerTaskExecutor();
            log.info("Kafka Producer initialized successfully");

        } catch (Exception e) {
            log.error("Failed to initialize Kafka Producer", e);
            throw new RuntimeException("Error initializing Kafka Producer", e);
        }
    }

    public void sendKafkaKirimMasterKabupaten(List<KirimMasterKabupatenDTO> data) {
        log.info("Sending batch of {} records to Kafka topic: {}", data.size(), kafkaTopic);

        for (KirimMasterKabupatenDTO dataKafka : data) {
            String key = dataKafka.getIdTransaksi();
            ProducerRecord<String, KirimMasterKabupatenDTO> record = new ProducerRecord<>(kafkaTopic, key, dataKafka);

            executorService.submit(() -> {
                int retry = 0;
                while (retry < RETRY_LIMIT) {
                    try {
                        Future<RecordMetadata> future = producer.send(record);
                        RecordMetadata metadata = future.get();
                        log.info("Message sent successfully: topic={}, offset={}", metadata.topic(), metadata.offset());
                        dataTrans.setDataKirimMasterKabupaten(key, 1, "BERHASIL KIRIM");
                        break;
                    } catch (ExecutionException | InterruptedException e) {
                        retry++;
                        log.error("Retry {}/{} failed to send message: {}", retry, RETRY_LIMIT, e.getMessage(), e);
                        if (retry == RETRY_LIMIT) {
                            dataTrans.setDataKirimMasterKabupaten(key, 99, "GAGAL KIRIM");
                        }
                    }
                }
            });
        }
        log.info("Batch processing completed for Kafka");
    }

    @PreDestroy
    public void closeProducer() {
        if (producer != null) {
            producer.close();
            log.info("Kafka Producer closed");
        }
        if (executorService != null) {
            executorService.shutdown();
            log.info("ExecutorService shut down");
        }
    }
}
