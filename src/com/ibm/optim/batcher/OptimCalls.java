/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

import java.util.Properties;

/**
 *
 * @author zinal_m
 */
public class OptimCalls {

    final Properties properties;
    final String optimPath;
    
    final String[] callArgs;
    
    public OptimCalls(Properties props, String[] args) {
        this.properties = props;
        this.optimPath = props.getProperty("optim.path");
        this.callArgs = new String[args.length-2];
        System.arraycopy(args, 2, this.callArgs, 0, this.callArgs.length);
    }

    public Properties getProperties() {
        return properties;
    }
    public String getOptimPath() {
        return optimPath;
    }
    public String[] getCallArgs() {
        return callArgs;
    }

}
