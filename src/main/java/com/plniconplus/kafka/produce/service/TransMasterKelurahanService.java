package com.plniconplus.kafka.produce.service;

import com.plniconplus.kafka.produce.dao.TransDao;
import com.plniconplus.kafka.produce.kafka.produce.MasterKecamatanProduce;
import com.plniconplus.kafka.produce.kafka.produce.MasterKelurahanProduce;
import com.plniconplus.kafka.produce.model.KirimMasterKecamatanDTO;
import com.plniconplus.kafka.produce.model.KirimMasterKelurahanDTO;
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
public class TransMasterKelurahanService {

    @Autowired
    private TransDao transDao;

    @Autowired
    private MasterKelurahanProduce masterKelurahanProduce;

    private Queue<KirimMasterKelurahanDTO> dataQueue = new ConcurrentLinkedQueue<>();
    private static final int BATCH_SIZE = 1000; // Ukuran batch data

    public void sendKafkaKelurahan() throws Exception {
        List<KirimMasterKelurahanDTO> lst = transDao.getDataMasterKelurahan();
        log.info(">>>>>>>>> Data Kirim Master Kelurahan= {}", lst.size());

        // Tambahkan semua data baru ke dalam queue
        lst.forEach(dataQueue::offer);

        // Kirim data dari queue
        processQueue();
    }

    private void processQueue() {
        while (!dataQueue.isEmpty()) {
            List<KirimMasterKelurahanDTO> batch = new ArrayList<>();

            // Ambil data sesuai ukuran batch
            for (int i = 0; i < BATCH_SIZE && !dataQueue.isEmpty(); i++) {
                KirimMasterKelurahanDTO data = dataQueue.poll();
                if (data != null) {
                    batch.add(data);
                }

            }

            if (!batch.isEmpty()) {
                try {
                    // Kirim data batch ke Kafka
                    masterKelurahanProduce.sendKafkaKirimMasterkelurahan(batch);
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
