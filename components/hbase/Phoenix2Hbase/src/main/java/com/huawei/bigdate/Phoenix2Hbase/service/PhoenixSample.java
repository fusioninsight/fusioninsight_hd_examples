package com.huawei.bigdate.Phoenix2Hbase.service;

import com.huawei.bigdate.Phoenix2Hbase.model.Vehicle;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Component
public class PhoenixSample {

    private final static Log LOG = LogFactory.getLog(PhoenixSample.class);
    Configuration conf = null;
    String URL = "";
    String tableName = "VEHICLE_INFO";
    public PhoenixSample( ) {

    }
    public PhoenixSample(Configuration conf,String tableName) {
        this.conf = conf;
        URL= "jdbc:phoenix:"+conf.get("hbase.zookeeper.quorum");
        System.out.println("url:"+URL);
        this.tableName = tableName;
    }

    /**
     * Create Table
     */
    public void testCreateTable() {
        LOG.info("Entering testCreateTable.");
        String createTableSQL =
                "CREATE TABLE IF NOT EXISTS "+tableName+" (id varchar primary key, checkpoint_id varchar, " +
                        "time varchar, plate_no varchar, plate_color integer, vehicle_color integer, " +
                        "vehicle_type varchar, vehicle_brand varchar, smoking integer, " +
                        "safebelt integer)";

        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            Connection conn = DriverManager.getConnection(URL);
            Statement stat = conn.createStatement();
            // Execute Create SQL
            stat.executeUpdate(createTableSQL);
            LOG.info("Create table successfully.");
        } catch (Exception e) {
            LOG.error("Create table failed.", e);
        }
        LOG.info("Exiting testCreateTable.");
    }

    /**
     * Put data
     * @throws ClassNotFoundException
     */
    public void testPut(Vehicle vehicle) throws ClassNotFoundException {
//        // Insert
//        String upsertSQL =
//                "UPSERT INTO "+tableName+"  VALUES(1,'John','100000', TO_DATE('1980-01-01','yyyy-MM-dd'))";
        String upsertSQL =
                "UPSERT INTO "+tableName+"  VALUES("+ vehicle.convertToString() + ")";

        try{
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            Connection conn = DriverManager.getConnection(URL);
            Statement stat = conn.createStatement();
            // Execute Update SQL
            stat.executeUpdate(upsertSQL);
            conn.commit();
            LOG.info("Put successfully.");
        }catch (Exception e) {
            LOG.error("Put failed.", e);
        }
        LOG.info("Exiting testPut.");
    }

    public List<Vehicle> testSelectFixedNumData(int num) throws ClassNotFoundException {
        LOG.info("Entering testMySelect.");

        List<Vehicle> results = new ArrayList<Vehicle>();
        String querySQL = "SELECT * FROM "+tableName+" LIMIT "+ num;
        Connection conn = null;
        PreparedStatement preStat = null;
        Statement stat = null;
        ResultSet result = null;
        try {
            // Create Connection
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(URL);
            // Create Statement
            stat = conn.createStatement();
            // Create PrepareStatement
            preStat = conn.prepareStatement(querySQL);
            // Execute query
//      preStat.setInt(1, 1);
            result = preStat.executeQuery();
            // Get result
            while (result.next()) {
                Vehicle vehicle = new Vehicle(result.getString("id"),result.getString("checkpoint_id"),
                        result.getString("time"),result.getString("plate_no"),
                        result.getInt("plate_color"),result.getInt("vehicle_color"),
                        result.getString("vehicle_type"),result.getString("vehicle_brand"),
                        result.getInt("smoking"),result.getInt("safebelt")
                );
                results.add(vehicle);
                System.out.println(vehicle.toString());
            }
            LOG.info("Select successfully.");
        } catch (Exception e) {
            LOG.error("Select failed.", e);
        } finally {
            if (null != result) {
                try {
                    result.close();
                } catch (Exception e2) {
                    LOG.error("Result close failed.", e2);
                }
            }
            if (null != stat) {
                try {
                    stat.close();
                } catch (Exception e2) {
                    LOG.error("Stat close failed.", e2);
                }
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (Exception e2) {
                    LOG.error("Connection close failed.", e2);
                }
            }
        }
        LOG.info("Exiting testSelect.");
        return results;
    }
    /**
     * Select Data
     */
    public Vehicle testSelectFromPlateNo(String plateNo) {
        Vehicle vehicle = new  Vehicle();
        // Query
        String querySQL = "SELECT * FROM "+tableName+"  WHERE plate_no = '"+plateNo+"'";
        Connection conn = null;
        PreparedStatement preStat = null;
        Statement stat = null;
        ResultSet result = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            // Create Connection
            conn = DriverManager.getConnection(URL);
            // Create Statement
            stat = conn.createStatement();
            // Create PrepareStatement
            preStat = conn.prepareStatement(querySQL);
            // Execute query
//            preStat.setString(1, id);
            result = preStat.executeQuery();

            // Get result
            while (result.next()) {
                vehicle = new Vehicle(result.getString("id"),result.getString("checkpoint_id"),
                        result.getString("time"),result.getString("plate_no"),
                        result.getInt("plate_color"),result.getInt("vehicle_color"),
                        result.getString("vehicle_type"),result.getString("vehicle_brand"),
                        result.getInt("smoking"),result.getInt("safebelt")
                );
                System.out.println(vehicle.toString());
            }
            LOG.info("Select successfully.");
        } catch (Exception e) {
            LOG.error("Select failed.", e);
        } finally {
            if (null != result) {
                try {
                    result.close();
                } catch (Exception e2) {
                    LOG.error("Result close failed.", e2);
                }
            }
            if (null != stat) {
                try {
                    stat.close();
                } catch (Exception e2) {
                    LOG.error("Stat close failed.", e2);
                }
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (Exception e2) {
                    LOG.error("Connection close failed.", e2);
                }
            }
        }
        LOG.info("Exiting testSelect.");
        return  vehicle ;
    }

    /**
     * Select Data
     */
    public Vehicle testSelectFromId(String id) {
        Vehicle vehicle = new Vehicle();
        // Query
        String querySQL = "SELECT * FROM "+tableName+"  WHERE id = '"+id+"'";
        Connection conn = null;
        PreparedStatement preStat = null;
        Statement stat = null;
        ResultSet result = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            // Create Connection
            conn = DriverManager.getConnection(URL);
            // Create Statement
            stat = conn.createStatement();
            // Create PrepareStatement
            preStat = conn.prepareStatement(querySQL);
            // Execute query
//            preStat.setString(1, id);
            result = preStat.executeQuery();

            // Get result
            while (result.next()) {
               vehicle = new Vehicle(result.getString("id"),result.getString("checkpoint_id"),
                        result.getString("time"),result.getString("plate_no"),
                        result.getInt("plate_color"),result.getInt("vehicle_color"),
                        result.getString("vehicle_type"),result.getString("vehicle_brand"),
                        result.getInt("smoking"),result.getInt("safebelt")
                );
                System.out.println(vehicle.toString());
            }
            LOG.info("Select successfully.");
        } catch (Exception e) {
            LOG.error("Select failed.", e);
        } finally {
            if (null != result) {
                try {
                    result.close();
                } catch (Exception e2) {
                    LOG.error("Result close failed.", e2);
                }
            }
            if (null != stat) {
                try {
                    stat.close();
                } catch (Exception e2) {
                    LOG.error("Stat close failed.", e2);
                }
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (Exception e2) {
                    LOG.error("Connection close failed.", e2);
                }
            }
        }
        LOG.info("Exiting testSelect.");
        return vehicle;
    }
    /**
     * Select Data
     */
    public List<Vehicle> testSelectFromSafebelt(int safebelt) {
        // Query
        List<Vehicle>  results = new ArrayList<Vehicle>();
        String querySQL = "SELECT * FROM "+tableName+"  WHERE safebelt = "+safebelt+"";
        Connection conn = null;
        PreparedStatement preStat = null;
        Statement stat = null;
        ResultSet result = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            // Create Connection
            conn = DriverManager.getConnection(URL);
            // Create Statement
            stat = conn.createStatement();
            // Create PrepareStatement
            preStat = conn.prepareStatement(querySQL);
            // Execute query
//            preStat.setString(1, id);
            result = preStat.executeQuery();

            // Get result
            while (result.next()) {
                Vehicle vehicle = new Vehicle(result.getString("id"),result.getString("checkpoint_id"),
                        result.getString("time"),result.getString("plate_no"),
                        result.getInt("plate_color"),result.getInt("vehicle_color"),
                        result.getString("vehicle_type"),result.getString("vehicle_brand"),
                        result.getInt("smoking"),result.getInt("safebelt")
                );
                results.add(vehicle);
                System.out.println(vehicle.toString());
            }
            LOG.info("Select successfully.");
        } catch (Exception e) {
            LOG.error("Select failed.", e);
        } finally {
            if (null != result) {
                try {
                    result.close();
                } catch (Exception e2) {
                    LOG.error("Result close failed.", e2);
                }
            }
            if (null != stat) {
                try {
                    stat.close();
                } catch (Exception e2) {
                    LOG.error("Stat close failed.", e2);
                }
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (Exception e2) {
                    LOG.error("Connection close failed.", e2);
                }
            }
        }
        LOG.info("Exiting testSelect.");
        return results;
    }
    /**
     * Select Data
     */
    public List<Vehicle> testSelectFromVehicleColorAndType(int color,String type) {
        // Query
        List<Vehicle>  results = new ArrayList<Vehicle>();
        String querySQL = "SELECT * FROM "+tableName+"  WHERE  vehicle_color = "+ color +" and vehicle_type = '"+type+"'";
        Connection conn = null;
        PreparedStatement preStat = null;
        Statement stat = null;
        ResultSet result = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            // Create Connection
            conn = DriverManager.getConnection(URL);
            // Create Statement
            stat = conn.createStatement();
            // Create PrepareStatement
            preStat = conn.prepareStatement(querySQL);
            // Execute query
//            preStat.setString(1, id);
            result = preStat.executeQuery();

            // Get result
            while (result.next()) {
                Vehicle vehicle = new Vehicle(result.getString("id"),result.getString("checkpoint_id"),
                        result.getString("time"),result.getString("plate_no"),
                        result.getInt("plate_color"),result.getInt("vehicle_color"),
                        result.getString("vehicle_type"),result.getString("vehicle_brand"),
                        result.getInt("smoking"),result.getInt("safebelt")
                );
                results.add(vehicle);
                System.out.println(vehicle.toString());
            }
            LOG.info("Select successfully.");
        } catch (Exception e) {
            LOG.error("Select failed.", e);
        } finally {
            if (null != result) {
                try {
                    result.close();
                } catch (Exception e2) {
                    LOG.error("Result close failed.", e2);
                }
            }
            if (null != stat) {
                try {
                    stat.close();
                } catch (Exception e2) {
                    LOG.error("Stat close failed.", e2);
                }
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (Exception e2) {
                    LOG.error("Connection close failed.", e2);
                }
            }
        }
        LOG.info("Exiting testSelect.");
        return results;
    }
    /**
     * Select Data
     */
    public List<Vehicle> testSelectFromTime(String time) {
        List<Vehicle>  results = new ArrayList<Vehicle>();
        String querySQL = "SELECT * FROM "+tableName+"  WHERE time LIKE '"+time+"%'";
        Connection conn = null;
        PreparedStatement preStat = null;
        Statement stat = null;
        ResultSet result = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            // Create Connection
            conn = DriverManager.getConnection(URL);
            // Create Statement
            stat = conn.createStatement();
            // Create PrepareStatement
            preStat = conn.prepareStatement(querySQL);
            // Execute query
//            preStat.setString(1, id);
            result = preStat.executeQuery();
            // Get result
            while (result.next()) {
                Vehicle vehicle = new Vehicle(result.getString("id"),result.getString("checkpoint_id"),
                        result.getString("time"),result.getString("plate_no"),
                        result.getInt("plate_color"),result.getInt("vehicle_color"),
                        result.getString("vehicle_type"),result.getString("vehicle_brand"),
                        result.getInt("smoking"),result.getInt("safebelt")
                );
                results.add(vehicle);
                System.out.println(vehicle.toString());
            }
            LOG.info("Select successfully.");
        } catch (Exception e) {
            LOG.error("Select failed.", e);
        } finally {
            if (null != result) {
                try {
                    result.close();
                } catch (Exception e2) {
                    LOG.error("Result close failed.", e2);
                }
            }
            if (null != stat) {
                try {
                    stat.close();
                } catch (Exception e2) {
                    LOG.error("Stat close failed.", e2);
                }
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (Exception e2) {
                    LOG.error("Connection close failed.", e2);
                }
            }
        }
        LOG.info("Exiting testSelect.");
        return results;
    }

    /**
     * Drop Table
     */
    public void testDrop() {
        // Delete table
        String dropTableSQL = "DROP TABLE "+tableName;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            Connection conn = DriverManager.getConnection(URL);
            Statement stat = conn.createStatement();
            stat.executeUpdate(dropTableSQL);
            conn.close();
            LOG.info("Drop successfully.");
        } catch (Exception e) {
            LOG.error("Drop failed.", e);
        }
        LOG.info("Exiting testDrop.");

    }
    /**
     * Delete data
     */
    public void testDelete(String id) {
        // Delete data
        String deleteDataSQL = "DELETE FROM " + tableName +   " WHERE id = '"+id+"'";
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            Connection conn = DriverManager.getConnection(URL);
            Statement stat = conn.createStatement();
            stat.executeUpdate(deleteDataSQL);
            conn.commit();
            conn.close();
            LOG.info("Delete successfully.");
        } catch (Exception e) {
            LOG.error("Delete failed.", e);
        }
        LOG.info("Exiting testDelete.");
    }
}
