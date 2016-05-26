/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

import java.io.FileInputStream;
import java.util.Properties;

/**
 *
 * @author zinal_m
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            showHelpAndExit();
        }
        final Properties props = new Properties();
        try {
            final FileInputStream fis = new FileInputStream(args[0]);
            try {
                props.load(fis);
            } finally {
                fis.close();
            }
        } catch(Exception ex) {
            System.err.println("ERROR: failed to load property file [" + args[0] + "]");
            ex.printStackTrace(System.err);
            System.exit(1);
        }
        final OptimCalls oc = new OptimCalls(props);
        try {
            oc.open();
            final ConfigGenerator call = new ConfigGenerator(oc);
            call.setDataSourceName(args[1]);
            call.setTableList(ConfigGenerator.loadTablesFromFile(args[2]));
            call.run();
        } catch(Exception ex) {
            System.err.println("ERROR: utility execution failed");
            ex.printStackTrace(System.err);
            System.exit(1);
        } finally {
            oc.close();
        }
    }
    
    private static void showHelpAndExit() {
        System.err.println("USAGE: java -jar OptimBatcher.jar "
                + "OptimBatcher.properties data-source tables.txt");
        System.exit(1);
    }
}
