package com.huawei.bigdate.Phoenix2Hbase.controller;

import com.huawei.bigdate.Phoenix2Hbase.model.Vehicle;
import com.huawei.bigdate.Phoenix2Hbase.service.TestPhoenix;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/phoenix")
public class PhoenixController {

    @Autowired
    TestPhoenix testPhoenix;

    @GetMapping("/testcreatetable")
    public String testCreateTable(@RequestParam String tableName){
        testPhoenix.testCreateTable(tableName);
        return "Create table successfully";
    }

    @DeleteMapping("/testdrop")
    public String testDrop(@RequestParam String tableName){
        testPhoenix.testDrop(tableName);
        return "Drop table successfully";
    }

    @DeleteMapping("/testdelete")
    public String testDelete(@RequestParam String id, @RequestParam String tableName){
        testPhoenix.testDelete(id,tableName);
        return "Delete data successfully";
    }


    @PostMapping("/testput")
    public String testPut(
            @RequestParam  String id, @RequestParam String checkpoint_id,
            @RequestParam  String time,@RequestParam String plate_no,
            @RequestParam int plate_color, @RequestParam int vehicle_color,
            @RequestParam String vehicle_type, @RequestParam String vehicle_brand,
            @RequestParam int smoking, @RequestParam int safebelt,
            @RequestParam String tableName) {

        Vehicle vehicle = new Vehicle(id, checkpoint_id,time, plate_no, plate_color, vehicle_color,vehicle_type,vehicle_brand, smoking, safebelt);
        testPhoenix.testPut(vehicle,tableName);
        return "Put successfully";
    }

    @GetMapping("/testselect0")
    public List<Vehicle> testSelectFixedNumData(@RequestParam int num , @RequestParam String tableName){
        //    List<Vehicle>
        return testPhoenix.testSelectFixedNumData(num,tableName);
    }

    @GetMapping("/testselect1")
    public Vehicle testSelectFromId(@RequestParam String id,@RequestParam String tableName){
        return testPhoenix.testSelectFromId(id,tableName);
    }

    @GetMapping("/testselect2")
    public Vehicle testSelectFromPlateNo(@RequestParam String plateNo,@RequestParam String tableName){
        return testPhoenix.testSelectFromPlateNo(plateNo,tableName);
    }

    @GetMapping("/testselect3")
    public List<Vehicle> testSelectFromSafebelt(@RequestParam int safebelt ,@RequestParam String tableName){
        return testPhoenix.testSelectFromSafebelt(safebelt,tableName);
    }

    @GetMapping("/testselect4")
    public  List<Vehicle>  testSelectFromTime(@RequestParam String time ,@RequestParam String tableName){
        return  testPhoenix.testSelectFromTime(time,tableName);
    }

    @GetMapping("/testselect5")
    public  List<Vehicle>  testSelectFromVehicleColorAndType (@RequestParam int color,@RequestParam String type ,@RequestParam String tableName){
        return testPhoenix.testSelectFromVehicleColorAndType(color,type,tableName);
    }
}
