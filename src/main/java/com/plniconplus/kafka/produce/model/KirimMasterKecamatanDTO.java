package com.plniconplus.kafka.produce.model;

import lombok.Data;

import java.io.Serializable;

/**
 * @author RR
 * Date: 05/05/2025
 * Time: 14:21
 */

@Data
public class KirimMasterKecamatanDTO implements Serializable {

    private String idTransaksi;
    private String kd_kec;
    private String kd_kab;
    private String nama_prov;
    private String ptg_catat;
    private String tgl_catat;
    private Integer status_aktif;

}
