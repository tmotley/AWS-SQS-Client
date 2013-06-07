package com.mulesoft.demo.messaging.sqs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 *
 */
public class CSVReader {
    public static void readCSV(File csv, LineHandler lineHandler) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(csv));
        // push past the first line, assume it's the column headers
        reader.readLine();
        while (reader.ready())   {
            lineHandler.handle(reader.readLine());
        }
    }

    public interface LineHandler {
        public void handle(String line);
    }

    /**
     * Test
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        readCSV(new File("500.csv"), new LineHandler() {
            @Override
            public void handle(String line) {
                System.out.println(line);
            }
        });
    }

}
