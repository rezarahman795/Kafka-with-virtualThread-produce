package com.plniconplus.kafka.produce.dao.impl;

import com.plniconplus.kafka.produce.dao.TransDao;
import com.plniconplus.kafka.produce.model.KirimMasterKabupatenDTO;
import com.plniconplus.kafka.produce.model.KirimMasterKecamatanDTO;
import com.plniconplus.kafka.produce.model.KirimMasterKelurahanDTO;
import com.plniconplus.kafka.produce.model.KirimMasterProvinsiDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author RR
 * Date: 30/04/2025
 * Time: 15:44
 */

@Slf4j
@Repository
public class TransDaoImpl implements TransDao {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public List<KirimMasterProvinsiDTO> getDataMasterProvinsi() {
        List<KirimMasterProvinsiDTO> lst = new ArrayList<>();
        try {
            Connection con = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
            String sql = "{? = call databaseanda()}";
            CallableStatement call = con.prepareCall(sql);
            call.registerOutParameter(1, Types.REF_CURSOR);
            call.execute();

            ResultSet rs = (ResultSet) call.getObject(1);
            while (rs.next()) {
                KirimMasterProvinsiDTO t = new KirimMasterProvinsiDTO();
                t.setKd_prov(rs.getString("KD_PROV"));
                t.setNama_prov(rs.getString("NAMA"));
                t.setPtg_catat(rs.getString("PTGCATAT"));
                t.setTgl_catat(rs.getString("TGLCATAT"));
                t.setIdTransaksi(rs.getString("ID_TRANSAKSI"));
                t.setStatus_aktif(rs.getInt("STATUS_AKTIF"));
                lst.add(t);
            }
            con.close();
            call.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error("~> Found Error Produce {}", ex.getMessage());
        }
        return lst;
    }

    @Override
    public void setDataKirimMasterProvinsi(String id_transaksi, int status_kirim, String keterangan_kirim) {
        Connection con = null;
        CallableStatement call = null;
        try {
            con = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
            String sql = "{? = call databaseanda(?,?,?,?)}";
            call = con.prepareCall(sql);
            call.registerOutParameter(1, Types.INTEGER);
            call.setString(2, id_transaksi);
            call.setInt(3, status_kirim);
            call.setString(4, keterangan_kirim);
            call.registerOutParameter(5, Types.VARCHAR);
            call.execute();
        } catch (Exception ex) {
            log.error("ERROR SET DATA KIRIM MASTER PROVINSI : {}", ex.getMessage(), ex);
        } finally {
            try {
                if (call != null) call.close();
                if (con != null) con.close();
            } catch (Exception closeEx) {
                log.error("ERROR CLOSING RESOURCES: {}", closeEx.getMessage(), closeEx);
            }
        }
    }

    @Override
    public List<KirimMasterKabupatenDTO> getDataMasterKabupaten() {
        List<KirimMasterKabupatenDTO> lst = new ArrayList<>();
        try {
            Connection con = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
            String sql = "{? = call databaseanda()}";
            CallableStatement call = con.prepareCall(sql);
            call.registerOutParameter(1, Types.REF_CURSOR);
            call.execute();

            ResultSet rs = (ResultSet) call.getObject(1);
            while (rs.next()) {
                KirimMasterKabupatenDTO t = new KirimMasterKabupatenDTO();
                t.setKd_kab(rs.getString("KD_KAB"));
                t.setKd_prov(rs.getString("KD_PROV"));
                t.setNama_prov(rs.getString("NAMA"));
                t.setPtg_catat(rs.getString("PTGCATAT"));
                t.setTgl_catat(rs.getString("TGLCATAT"));
                t.setIdTransaksi(rs.getString("ID_TRANSAKSI"));
                t.setStatus_aktif(rs.getInt("STATUS_AKTIF"));
                lst.add(t);
            }
            con.close();
            call.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error("~> Found Error Produce {}", ex.getMessage());
        }
        return lst;
    }

    @Override
    public void setDataKirimMasterKabupaten(String id_transaksi, int status_kirim, String keterangan_kirim) {
        Connection con = null;
        CallableStatement call = null;
        try {
            con = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
            String sql = "{? = call databaseanda(?,?,?,?)}";
            call = con.prepareCall(sql);
            call.registerOutParameter(1, Types.INTEGER);
            call.setString(2, id_transaksi);
            call.setInt(3, status_kirim);
            call.setString(4, keterangan_kirim);
            call.registerOutParameter(5, Types.VARCHAR);
            call.execute();
        } catch (Exception ex) {
            log.error("ERROR SET DATA KIRIM MASTER KABUPATEN : {}", ex.getMessage(), ex);
        } finally {
            try {
                if (call != null) call.close();
                if (con != null) con.close();
            } catch (Exception closeEx) {
                log.error("ERROR CLOSING RESOURCES: {}", closeEx.getMessage(), closeEx);
            }
        }
    }

    @Override
    public List<KirimMasterKecamatanDTO> getDataMasterKecamatan() {
        List<KirimMasterKecamatanDTO> lst = new ArrayList<>();
        try {
            Connection con = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
            String sql = "{? = call databaseanda()}";
            CallableStatement call = con.prepareCall(sql);
            call.registerOutParameter(1, Types.REF_CURSOR);
            call.execute();

            ResultSet rs = (ResultSet) call.getObject(1);
            while (rs.next()) {
                KirimMasterKecamatanDTO t = new KirimMasterKecamatanDTO();
                t.setKd_kec(rs.getString("KD_KEC"));
                t.setKd_kab(rs.getString("KD_KAB"));
                t.setNama_prov(rs.getString("NAMA"));
                t.setPtg_catat(rs.getString("PTGCATAT"));
                t.setTgl_catat(rs.getString("TGLCATAT"));
                t.setIdTransaksi(rs.getString("ID_TRANSAKSI"));
                t.setStatus_aktif(rs.getInt("STATUS_AKTIF"));
                lst.add(t);
            }
            con.close();
            call.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error("~> Found Error Produce {}", ex.getMessage());
        }
        return lst;
    }

    @Override
    public void setDataKirimMasterKecamatan(String id_transaksi, int status_kirim, String keterangan_kirim) {
        Connection con = null;
        CallableStatement call = null;
        try {
            con = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
            String sql = "{? = call databaseanda(?,?,?,?)}";
            call = con.prepareCall(sql);
            call.registerOutParameter(1, Types.INTEGER);
            call.setString(2, id_transaksi);
            call.setInt(3, status_kirim);
            call.setString(4, keterangan_kirim);
            call.registerOutParameter(5, Types.VARCHAR);
            call.execute();
        } catch (Exception ex) {
            log.error("ERROR SET DATA KIRIM MASTER KECAMATAN : {}", ex.getMessage(), ex);
        } finally {
            try {
                if (call != null) call.close();
                if (con != null) con.close();
            } catch (Exception closeEx) {
                log.error("ERROR CLOSING RESOURCES: {}", closeEx.getMessage(), closeEx);
            }
        }
    }

    @Override
    public List<KirimMasterKelurahanDTO> getDataMasterKelurahan() {
        List<KirimMasterKelurahanDTO> lst = new ArrayList<>();
        try {
            Connection con = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
            String sql = "{? = call databaseanda()}";
            CallableStatement call = con.prepareCall(sql);
            call.registerOutParameter(1, Types.REF_CURSOR);
            call.execute();

            ResultSet rs = (ResultSet) call.getObject(1);
            while (rs.next()) {
                KirimMasterKelurahanDTO t = new KirimMasterKelurahanDTO();
                t.setIdTransaksi(rs.getString("ID_TRANSAKSI"));
                t.setKd_prov(rs.getString("KD_PROV"));
                t.setNama_prov(rs.getString("NAMA_PROV"));
                t.setKd_kab(rs.getString("KD_KAB"));
                t.setNama_kab(rs.getString("NAMA_KAB"));
                t.setKd_kec(rs.getString("KD_KEC"));
                t.setNama_kec(rs.getString("NAMA_KEC"));
                t.setKd_kel(rs.getString("KD_KEL"));
                t.setNama_kel(rs.getString("NAMA_KEL"));
                t.setUnitupi(rs.getString("UNITUPI"));
                t.setUnitap(rs.getString("UNITAP"));
                t.setUnitup(rs.getString("UNITUP"));
                t.setKd_pemda(rs.getString("KDPEMDA"));
                t.setPtg_catat(rs.getString("PTGCATAT"));
                t.setTgl_catat(rs.getString("TGLCATAT"));
                t.setStatus_aktif(rs.getInt("STATUS_AKTIF"));
                lst.add(t);
            }
            con.close();
            call.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error("~> Found Error Produce {}", ex.getMessage());
        }
        return lst;
    }

    @Override
    public void setDataKirimMasterKelurahan(String id_transaksi, int status_kirim, String keterangan_kirim) {
        Connection con = null;
        CallableStatement call = null;
        try {
            con = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection();
            String sql = "{? = call databaseanda(?,?,?,?)}";
            call = con.prepareCall(sql);
            call.registerOutParameter(1, Types.INTEGER);
            call.setString(2, id_transaksi);
            call.setInt(3, status_kirim);
            call.setString(4, keterangan_kirim);
            call.registerOutParameter(5, Types.VARCHAR);
            call.execute();
        } catch (Exception ex) {
            log.error("ERROR SET DATA KIRIM MASTER KELURAHAN : {}", ex.getMessage(), ex);
        } finally {
            try {
                if (call != null) call.close();
                if (con != null) con.close();
            } catch (Exception closeEx) {
                log.error("ERROR CLOSING RESOURCES: {}", closeEx.getMessage(), closeEx);
            }
        }
    }
}
