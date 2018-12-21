package com.huawei.bigdata.spark.examples;

import org.apache.spark.launcher.SparkLauncher;

/**
  * Submit spark app.
  * args(0) is the mode to run spark app, eg yarn-client
  * args(1) is the path of spark app jar
  * args(2) is the main class of spark app
  * args(3...) is the parameters of spark app
  */
public class SparkLauncherExample {
    public static void main(String[] args) throws Exception {
        System.out.println("com.huawei.bigdata.spark.examples.SparkLauncherExample <mode> <jarParh> <app_main_class> <appArgs>");

        //使用此类以编程方式启动Spark应用程序。该类使用构建器模式允许客户端配置Spark应用程序并将其作为子进程启动。
        SparkLauncher launcher = new SparkLauncher();
        //为应用程序设置Spark主服务器。
        launcher.setMaster(args[0])//为应用程序设置Spark主服务器。
            .setAppResource(args[1]) //设置主应用程序资源。
            .setMainClass(args[2]);//设置Java / Scala应用程序的应用程序类名称。
        if (args.length > 3) {
            String[] list = new String[args.length - 3];//设置集合的大小
            for (int i = 3; i < args.length; i++) {
                //除去前三位，其余的存到list内
                list[i-3] = args[i];
            }
            // 设置应用程序args
            launcher.addAppArgs(list);
        }

        // 启动将启动已配置的Spark应用程序的子流程。
        Process process = launcher.launch();
        //获取Spark驱动程序日志
        new Thread(new ISRRunnable(process.getErrorStream())).start();
        int exitCode = process.waitFor();//进程等待
        System.out.println("Finished! Exit code is "  + exitCode);
    }
}

