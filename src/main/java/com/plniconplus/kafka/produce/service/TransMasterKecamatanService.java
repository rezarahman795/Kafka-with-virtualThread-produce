package com.plniconplus.kafka.produce.service;

import com.plniconplus.kafka.produce.dao.TransDao;
import com.plniconplus.kafka.produce.kafka.produce.MasterKabupatenProduce;
import com.plniconplus.kafka.produce.kafka.produce.MasterKecamatanProduce;
import com.plniconplus.kafka.produce.model.KirimMasterKabupatenDTO;
import com.plniconplus.kafka.produce.model.KirimMasterKecamatanDTO;
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
public class TransMasterKecamatanService {

    @Autowired
    private TransDao transDao;


    @Autowired
    private MasterKecamatanProduce masterKecamatanProduce;

    private Queue<KirimMasterKecamatanDTO> dataQueue = new ConcurrentLinkedQueue<>();
    private static final int BATCH_SIZE = 1000; // Ukuran batch data

    public void sendKafkaKecamatan() throws Exception {
        List<KirimMasterKecamatanDTO> lst = transDao.getDataMasterKecamatan();
        log.info(">>>>>>>>> Data Kirim Master Kecamatan= {}", lst.size());

        // Tambahkan semua data baru ke dalam queue
        lst.forEach(dataQueue::offer);

        // Kirim data dari queue
        processQueue();
    }

    private void processQueue() {
        while (!dataQueue.isEmpty()) {
            List<KirimMasterKecamatanDTO> batch = new ArrayList<>();

            // Ambil data sesuai ukuran batch
            for (int i = 0; i < BATCH_SIZE && !dataQueue.isEmpty(); i++) {
                KirimMasterKecamatanDTO data = dataQueue.poll();
                if (data != null) {
                    batch.add(data);
                }
            }

            if (!batch.isEmpty()) {
                try {
                    // Kirim data batch ke Kafka
                    masterKecamatanProduce.sendKafkaKirimMasterkecamatan(batch);
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
