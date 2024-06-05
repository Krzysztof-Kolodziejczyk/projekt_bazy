package org.example.utils;

import java.io.*;
import java.util.List;

public class CsvWriter {

    public static void writeToCsv(String directoryPath, String fileName, List<String> data) {
        File directory = new File(directoryPath);

        File csvFile = new File(directory, fileName);
        try {
            PrintWriter pw = new PrintWriter(csvFile);
            pw.println("postgres, mongo_simple, monogo_embedded");
            String rowAsString = String.join(", ", data);
            pw.println(rowAsString);
            pw.flush();
        } catch (FileNotFoundException e) {
            System.err.println("Could not find file: " + csvFile.getAbsolutePath());
        }
    }

    public static void writeToCsvPostgresStats(String directoryPath, String fileName, List<String> data) {
        File directory = new File(directoryPath);

        File csvFile = new File(directory, fileName);
        try {
            if (!csvFile.exists()) {
                csvFile.createNewFile();
                PrintWriter headerWriter = new PrintWriter(csvFile);
                headerWriter.println("postgres, mongo");
                headerWriter.close();
            }
            // Tworzymy PrintWriter z flagą append ustawioną na true
            PrintWriter pw = new PrintWriter(new FileOutputStream(csvFile, true));

            // Zapisujemy nowy wiersz
            String rowAsString = String.join(", ", data);
            pw.println(rowAsString);
            pw.flush();
            pw.close(); // Należy zamknąć PrintWriter

        } catch (FileNotFoundException e) {
            System.err.println("Could not find file: " + csvFile.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



}

