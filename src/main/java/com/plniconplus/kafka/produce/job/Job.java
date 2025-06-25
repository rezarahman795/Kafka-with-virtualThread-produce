package com.plniconplus.kafka.produce.job;

import com.plniconplus.kafka.produce.common.AppPropertiesUtil;
import com.plniconplus.kafka.produce.service.TransMasterKabupatenService;
import com.plniconplus.kafka.produce.service.TransMasterKecamatanService;
import com.plniconplus.kafka.produce.service.TransMasterKelurahanService;
import com.plniconplus.kafka.produce.service.TransMasterProvinsiService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author RR
 */


@Slf4j
@EnableScheduling
@Component
@EnableAsync
@EnableKafka
public class Job {

    @Autowired
    private TransMasterProvinsiService transMasterProvinsiService;

    @Autowired
    private TransMasterKabupatenService transMasterKabupatenService;

    @Autowired
    private TransMasterKecamatanService transMasterKecamatanService;

    @Autowired
    private TransMasterKelurahanService transMasterKelurahanService;


    @Autowired
    private AppPropertiesUtil appPropertiesUtil;

    private ExecutorService executorService;

    @PostConstruct
    public void init() {
        // Inisialisasi ExecutorService dengan virtual thread
        executorService = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Async
    @Scheduled(fixedRate = 10000) // Sesuaikan interval untuk memproses data
    public void doKirim() {
        executorService.submit(() -> {
            try {
                log.info("Starting Kafka data processing...");
                switch (appPropertiesUtil.jenis_kirim_kafka) {
                    case "MASTER_PROVINSI" -> transMasterProvinsiService.sendKafkaProvinsi();
                    case "MASTER_KABUPATEN" -> transMasterKabupatenService.sendKafkaKabupaten();
                    case "MASTER_KECAMATAN" -> transMasterKecamatanService.sendKafkaKecamatan();
                    case "MASTER_KELURAHAN" -> transMasterKelurahanService.sendKafkaKelurahan();
                }

            } catch (Exception e) {
                log.error("Error during Kafka processing", e);
            }
        });
    }

    @PreDestroy
    public void shutdown() {
        if (executorService != null) {
            executorService.shutdown();
            log.info("ExecutorService shut down initiated");
        }
    }
}
