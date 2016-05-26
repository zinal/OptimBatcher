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
        ACTIONS.put(ActionExtract.NAME, new ActionExtract());
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
        final OptimCalls oc = new OptimCalls(props, args);
        try {
            action.run(oc);
        } catch(Exception ex) {
            System.err.println("ERROR: failed to execute action " + action.getName());
            ex.printStackTrace(System.err);
            System.exit(1);
        } finally {
            oc.close();
        }
    }
    
    private static void showHelpAndExit() {
        System.err.println("USAGE: java -jar OptimBatcher.jar OptimBatcher.properties ACTION ...\n"
                + "Valid actions and options:\n"
                + "    EXTRACT data-source tables.txt\n");
        System.exit(1);
    }
    
    private static interface Action {
        String getName();
        void run(OptimCalls oc) throws Exception;
    }

    private static class ActionExtract implements Action {
        
        public static final String NAME = "EXTRACT";

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public void run(OptimCalls oc) throws Exception {
            if (oc.getCallArgs().length != 2)
                showHelpAndExit();
            new DoExtract(oc).run();
        }

    }
    
}
