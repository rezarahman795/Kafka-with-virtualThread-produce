package com.plniconplus.kafka.produce.model;

import lombok.Data;

import java.io.Serializable;

/**
 * @author RR
 * Date: 05/05/2025
 * Time: 14:21
 */

@Data
public class KirimMasterKelurahanDTO implements Serializable {

    private String idTransaksi;
    private String kd_prov;
    private String nama_prov;
    private String kd_kab;
    private String nama_kab;
    private String kd_kec;
    private String nama_kec;
    private String kd_kel;
    private String nama_kel;
    private String unitupi;
    private String unitap;
    private String unitup;
    private String kd_pemda;
    private String ptg_catat;
    private String tgl_catat;
    private Integer status_aktif;

}
