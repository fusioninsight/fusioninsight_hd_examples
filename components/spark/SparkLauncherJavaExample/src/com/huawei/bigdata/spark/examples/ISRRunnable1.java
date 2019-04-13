package com.huawei.bigdata.spark.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Print the log of process
 */
 class ISRRunnable1 implements Runnable {
    private final BufferedReader reader;

    private ISRRunnable1(BufferedReader reader) {
        this.reader = reader;
    }

    public ISRRunnable1(InputStream inputStream) {
        this(new BufferedReader(new InputStreamReader(inputStream)));
    }

    public void run() {
        String line = null;
        try {
            line = reader.readLine();
            while (line != null) {
                System.out.println(line);
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            System.out.println("There is a exception when getting log: " + e);
        }
    }
}
