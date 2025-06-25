package com.plniconplus.kafka.produce.kafka.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.plniconplus.kafka.produce.model.KirimMasterKabupatenDTO;
import com.plniconplus.kafka.produce.model.KirimMasterKecamatanDTO;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author RR
 * Date: 30/04/2025
 * Time: 15:47
 */
public class MasterKecamatanSerializer implements Serializer<KirimMasterKecamatanDTO> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, KirimMasterKecamatanDTO dto) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(dto).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}
