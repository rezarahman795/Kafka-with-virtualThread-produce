package com.plniconplus.kafka.produce.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author RR
 */


@Component
public class AppPropertiesUtil {

    @Value("${jenis_kirim_kafka}")
    public String jenis_kirim_kafka;

}
