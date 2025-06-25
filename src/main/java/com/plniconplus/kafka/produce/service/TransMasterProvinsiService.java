package com.plniconplus.kafka.produce.service;

import com.plniconplus.kafka.produce.dao.TransDao;
import com.plniconplus.kafka.produce.kafka.produce.MasterProvinsiProduce;
import com.plniconplus.kafka.produce.model.KirimMasterProvinsiDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author RR
 */

@Slf4j
@Service
public class TransMasterProvinsiService {

    @Autowired
    private TransDao transDao;


    @Autowired
    private MasterProvinsiProduce masterProvinsiProduce;

    private Queue<KirimMasterProvinsiDTO> dataQueue = new ConcurrentLinkedQueue<>();
    private static final int BATCH_SIZE = 1000; // Ukuran batch data

    public void sendKafkaProvinsi() throws Exception {
        List<KirimMasterProvinsiDTO> lst = transDao.getDataMasterProvinsi();
        log.info(">>>>>>>>> Data Kirim Master Provinsi= {}", lst.size());

        // Tambahkan semua data baru ke dalam queue
        lst.forEach(dataQueue::offer);

        // Kirim data dari queue
        processQueue();
    }

    private void processQueue() {
        while (!dataQueue.isEmpty()) {
            List<KirimMasterProvinsiDTO> batch = new ArrayList<>();

            // Ambil data sesuai ukuran batch
            for (int i = 0; i < BATCH_SIZE && !dataQueue.isEmpty(); i++) {
                KirimMasterProvinsiDTO data = dataQueue.poll();
                if (data != null) {
                    batch.add(data);
                }
            }

            if (!batch.isEmpty()) {
                try {
                    // Kirim data batch ke Kafka
                    masterProvinsiProduce.sendKafkaKirimMasterProvinsi(batch);
                    log.info("Sent batch of {} records to Kafka", batch.size());
                } catch (Exception ex) {
                    log.error("Failed to send batch to Kafka. Retrying...", ex);
                    // Retry: Kembalikan batch ke queue untuk dicoba lagi
                    dataQueue.addAll(batch);
                }
            }
        }
    }
}
