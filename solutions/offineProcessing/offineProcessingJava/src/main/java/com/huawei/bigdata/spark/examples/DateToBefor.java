package com.huawei.bigdata.spark.examples;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateToBefor {
      static long transformDate( String s) throws ParseException {
              SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
              long timeMillis = sdf.parse(s).getTime();
              return  timeMillis;
       }
      static long BeforeTime() {
              Calendar calendar = Calendar.getInstance();
              calendar.set(Calendar.YEAR,2018);
              calendar.set(Calendar.MONTH,1);
              calendar.set(Calendar.DAY_OF_MONTH,1);
//              calendar.add(Calendar.MONTH,-6);

              return  calendar.getTimeInMillis();
       }

}
