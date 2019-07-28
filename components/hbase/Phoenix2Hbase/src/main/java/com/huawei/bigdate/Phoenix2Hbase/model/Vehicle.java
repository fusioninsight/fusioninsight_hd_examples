package com.huawei.bigdate.Phoenix2Hbase.model;

public class Vehicle {
//    id:"4p9tqo41ksu7"   #  string类型，随机字符串，行数据ID
//    checkpoint_id: "21001040"   #  string类型，卡口编号
//    time: "2019-03-01 10:57:46" # string类型，采集时间
//    plate_no: "粤S7N6W3" # string类型，车牌号
//    plate_color: 1	#  int类型，车牌号颜色： 白、灰、黑、红、紫、蓝、黄、绿、青、棕、粉红（0-10）
//    vehicle_color: 0 #  int类型，车辆颜色： 白、灰、黑、红、紫、蓝、黄、绿、青、棕、粉红（0-10）
//    vehicle_type：SUV # string类型，车辆类型
//    vehicle_brand："本田" # string类型，车辆品牌
//    smoking: 0 #  int类型，是否抽烟 1：是  0：否
//    safebelt: 1 #  int类型，是否系安全带 1：是  0：否

    public Vehicle(){
    }
    public Vehicle(String id, String checkpoint_id, String time, String plate_no, int plate_color, int vehicle_color, String vehicle_type, String vehicle_brand, int smoking, int safebelt) {
        this.id = id;
        this.checkpoint_id = checkpoint_id;
        this.time = time;
        this.plate_no = plate_no;
        this.plate_color = plate_color;
        this.vehicle_color = vehicle_color;
        this.vehicle_type = vehicle_type;
        this.vehicle_brand = vehicle_brand;
        this.smoking = smoking;
        this.safebelt = safebelt;
    }

    private String id;
    private String checkpoint_id;
    private String time;
    private String plate_no;
    private int plate_color;
    private int vehicle_color;
    private String vehicle_type;
    private String vehicle_brand;
    private int smoking;
    private int safebelt;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCheckpoint_id() {
        return checkpoint_id;
    }

    public void setCheckpoint_id(String checkpoint_id) {
        this.checkpoint_id = checkpoint_id;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getPlate_no() {
        return plate_no;
    }

    public void setPlate_no(String plate_no) {
        this.plate_no = plate_no;
    }

    public int getPlate_color() {
        return plate_color;
    }

    public void setPlate_color(int plate_color) {
        this.plate_color = plate_color;
    }

    public int getVehicle_color() {
        return vehicle_color;
    }

    public void setVehicle_color(int vehicle_color) {
        this.vehicle_color = vehicle_color;
    }

    public String getVehicle_type() {
        return vehicle_type;
    }

    public void setVehicle_type(String vehicle_type) {
        this.vehicle_type = vehicle_type;
    }

    public String getVehicle_brand() {
        return vehicle_brand;
    }

    public void setVehicle_brand(String vehicle_brand) {
        this.vehicle_brand = vehicle_brand;
    }

    public int getSmoking() {
        return smoking;
    }

    public void setSmoking(int smoking) {
        this.smoking = smoking;
    }

    public int getSafebelt() {
        return safebelt;
    }

    public void setSafebelt(int safebelt) {
        this.safebelt = safebelt;
    }

    @Override
    public String toString() {
        return "Vehicle{" +
                "id='" + id + '\'' +
                ", checkpoint_id='" + checkpoint_id + '\'' +
                ", time='" + time + '\'' +
                ", plate_no='" + plate_no + '\'' +
                ", plate_color=" + plate_color +
                ", vehicle_color=" + vehicle_color +
                ", vehicle_type='" + vehicle_type + '\'' +
                ", vehicle_brand='" + vehicle_brand + '\'' +
                ", smoking=" + smoking +
                ", safebelt=" + safebelt +
                '}';
    }

    public String convertToString() {
        return  "'" + id + '\'' +
                ",'" + checkpoint_id + '\'' +
                ",'" + time + '\'' +
                ",'" + plate_no + '\'' +
                "," + plate_color +
                "," + vehicle_color +
                ",'" + vehicle_type + '\'' +
                ",'" + vehicle_brand + '\'' +
                "," + smoking +
                "," + safebelt;
    }
}
