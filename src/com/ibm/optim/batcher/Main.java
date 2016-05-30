/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
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
        final OptimCalls oc = new OptimCalls(globalProps, jobProps);
        try {
            oc.open();
            new ConfigGenerator(oc).run();
        } catch(Exception ex) {
            System.err.println("ERROR: utility execution failed");
            ex.printStackTrace(System.err);
            System.exit(1);
        } finally {
            oc.close();
        }
    }

    private static void showHelpAndExit() {
        System.err.println("USAGE: java -jar OptimBatcher.jar Global.properties Job.properties");
        System.exit(1);
    }

    private static List<String> loadTablesFromFile(String fname) throws Exception {
        final BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(fname), "UTF-8"));
        try {
            final List<String> ll = new ArrayList<>();
            String line;
            while ((line=br.readLine())!=null) {
                line = line.trim().toUpperCase();
                if (line.length() > 0 && !line.startsWith("#")) {
                    String[] tmp = line.split("[.]");
                    if (tmp.length!=2 || tmp[0].trim().length()==0 || tmp[1].trim().length()==0)
                        throw new IllegalArgumentException("Invalid table name: [" + line + "]");
                    ll.add(line);
                }
            }
            return ll;
        } finally {
            br.close();
        }
    }
}
