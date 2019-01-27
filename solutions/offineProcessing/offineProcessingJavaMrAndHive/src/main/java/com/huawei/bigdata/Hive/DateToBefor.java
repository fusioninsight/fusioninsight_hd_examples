package com.huawei.bigdata.Hive;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateToBefor {
    public static long TransformDate( String s) throws ParseException {
              SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
              long timeMillis = sdf.parse(s).getTime();
              return  timeMillis;
       }
    public static long BeforeTime() {
              Calendar calendar = Calendar.getInstance();
              calendar.add(Calendar.MONTH,-6);

              return  calendar.getTimeInMillis();
       }

}
