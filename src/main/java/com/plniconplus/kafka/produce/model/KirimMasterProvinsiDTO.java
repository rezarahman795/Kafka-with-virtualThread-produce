package com.plniconplus.kafka.produce.model;

import lombok.Data;

import java.io.Serializable;

/**
 * @author RR
 * Date: 30/04/2025
 * Time: 15:45
 */

@Data
public class KirimMasterProvinsiDTO implements Serializable {

    private String idTransaksi;
    private String kd_prov;
    private String nama_prov;
    private String ptg_catat;
    private String tgl_catat;
    private Integer status_aktif;
}
