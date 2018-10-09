/*
 * Copyright Notice:
 *      Copyright  1998-2009, Huawei Technologies Co., Ltd.  ALL Rights Reserved.
 *
 *      Warning: This computer software sourcecode is protected by copyright law
 *      and international treaties. Unauthorized reproduction or distribution
 *      of this sourcecode, or any portion of it, may result in severe civil and
 *      criminal penalties, and will be prosecuted to the maximum extent
 *      possible under the law.
 */

package com.huawei.hadoop.oozie.examples;


public class OozieMainForNormal
{
       
    private static boolean UNSECURTY_CLUSTER = false;
    
       
    public static void main(String[] args)
    {   
    	
        try
        {
            OozieSample oozieSample = new OozieSample(UNSECURTY_CLUSTER);
            oozieSample.test();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        
        System.out.println("-----------finish Oozie -------------------");
    }
  
    
    
}
