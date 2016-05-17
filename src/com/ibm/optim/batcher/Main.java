/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Properties;

/**
 *
 * @author zinal_m
 */
public class Main {
    
    private static final HashMap<String, Action> ACTIONS = new HashMap<>();
    static {
        ACTIONS.put("EXTRACT", new DoExtract());
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (args.length < 2) {
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
        final Action action = ACTIONS.get(args[1].trim().toUpperCase());
        if (action==null) {
            showHelpAndExit();
            throw new UnsupportedOperationException(); // UNREACHED
        }
        try {
            action.run(new OptimCalls(props, args));
        } catch(Exception ex) {
            
        }
    }
    
    private static void showHelpAndExit() {
        System.err.println("USAGE: java -jar OptimBatcher.jar OptimBatcher.properties ACTION ...\n"
                + "Valid actions and options:\n"
                + "    EXTRACT data-source tables.txt\n");
        System.exit(1);
    }
    
    private static interface Action {
        void run(OptimCalls oc) throws Exception;
    }
    
    private static class DoExtract implements Action {

        @Override
        public void run(OptimCalls oc) throws Exception {
            if (oc.getCallArgs().length != 2)
                showHelpAndExit();
        }

    }
    
}
