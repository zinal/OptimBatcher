/*
 * Sample code for IBM InfoSphere Optim software
 */
package com.ibm.optim.batcher;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Entry point of the OptimBatcher utility
 * @author zinal_m
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            showHelpAndExit();
        }
        final Properties globalProps = new Properties();
        final Properties jobProps = new Properties();
        try {
            final FileInputStream fis = new FileInputStream(args[0]);
            try {
                globalProps.load(fis);
            } finally {
                fis.close();
            }
        } catch(Exception ex) {
            System.err.println("ERROR: failed to load property file [" + args[0] + "]");
            ex.printStackTrace(System.err);
            System.exit(1);
        }
        try {
            final FileInputStream fis = new FileInputStream(args[1]);
            try {
                jobProps.load(fis);
            } finally {
                fis.close();
            }
        } catch(Exception ex) {
            System.err.println("ERROR: failed to load property file [" + args[1] + "]");
            ex.printStackTrace(System.err);
            System.exit(1);
        }
        try {
            final OptimConfig oc = new OptimConfig(globalProps, jobProps);
            try {
                oc.open();
                new OptimServiceBuilder(oc).run();
            } finally {
                oc.close();
            }
        } catch(Exception ex) {
            System.err.println("ERROR: utility execution failed");
            ex.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void showHelpAndExit() {
        System.err.println("USAGE: java -jar OptimBatcher.jar Global.properties Job.properties");
        System.exit(1);
    }

}
