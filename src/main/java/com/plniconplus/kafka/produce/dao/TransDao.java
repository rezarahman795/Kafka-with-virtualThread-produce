package com.plniconplus.kafka.produce.dao;

import com.plniconplus.kafka.produce.model.KirimMasterKabupatenDTO;
import com.plniconplus.kafka.produce.model.KirimMasterKecamatanDTO;
import com.plniconplus.kafka.produce.model.KirimMasterKelurahanDTO;
import com.plniconplus.kafka.produce.model.KirimMasterProvinsiDTO;

import java.util.List;

/**
 * @author RR
 */

public interface TransDao {

    List<KirimMasterProvinsiDTO> getDataMasterProvinsi();

    public void setDataKirimMasterProvinsi(String id_transaksi,int status_kirim, String keterangan_kirim);

    List<KirimMasterKabupatenDTO> getDataMasterKabupaten();

    public void setDataKirimMasterKabupaten(String id_transaksi,int status_kirim, String keterangan_kirim);

    List<KirimMasterKecamatanDTO> getDataMasterKecamatan();

    public void setDataKirimMasterKecamatan(String id_transaksi,int status_kirim, String keterangan_kirim);

    List<KirimMasterKelurahanDTO> getDataMasterKelurahan();

    public void setDataKirimMasterKelurahan(String id_transaksi,int status_kirim, String keterangan_kirim);
}
