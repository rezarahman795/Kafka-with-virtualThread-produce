package com.plniconplus.kafka.produce.service;

import com.plniconplus.kafka.produce.dao.TransDao;
import com.plniconplus.kafka.produce.kafka.produce.MasterKabupatenProduce;
import com.plniconplus.kafka.produce.kafka.produce.MasterProvinsiProduce;
import com.plniconplus.kafka.produce.model.KirimMasterKabupatenDTO;
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
public class TransMasterKabupatenService {

    @Autowired
    private TransDao transDao;


    @Autowired
    private MasterKabupatenProduce masterKabupatenProduce;

    private Queue<KirimMasterKabupatenDTO> dataQueue = new ConcurrentLinkedQueue<>();
    private static final int BATCH_SIZE = 1000; // Ukuran batch data

    public void sendKafkaKabupaten() throws Exception {
        List<KirimMasterKabupatenDTO> lst = transDao.getDataMasterKabupaten();
        log.info(">>>>>>>>> Data Kirim Master Kabupaten= {}", lst.size());

        // Tambahkan semua data baru ke dalam queue
        lst.forEach(dataQueue::offer);

        // Kirim data dari queue
        processQueue();
    }

    private void processQueue() {
        while (!dataQueue.isEmpty()) {
            List<KirimMasterKabupatenDTO> batch = new ArrayList<>();

            // Ambil data sesuai ukuran batch
            for (int i = 0; i < BATCH_SIZE && !dataQueue.isEmpty(); i++) {
                KirimMasterKabupatenDTO data = dataQueue.poll();
                if (data != null) {
                    batch.add(data);
                }
            }

            if (!batch.isEmpty()) {
                try {
                    // Kirim data batch ke Kafka
                    masterKabupatenProduce.sendKafkaKirimMasterKabupaten(batch);
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
