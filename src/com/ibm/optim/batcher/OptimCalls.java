/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 *
 * @author zinal_m
 */
public class OptimCalls implements Closeable {

    private final Properties properties;
    private final String optimPath;
    
    private final String[] callArgs;
    
    private Connection optimDirCnc;
    
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

    public void open() throws Exception {
        if (optimDirCnc!=null && optimDirCnc.isClosed()==false)
            return;
        final String clazzName = properties.getProperty("optimdir.jdbc.driver");
        if (clazzName!=null && clazzName.trim().length()>0) {
            Class.forName(clazzName.trim());
        }
        optimDirCnc = DriverManager.getConnection
            (properties.getProperty("optimdir.jdbc.url"),
             properties.getProperty("optimdir.jdbc.user"),
             properties.getProperty("optimdir.jdbc.password"));
        if (optimDirCnc.getAutoCommit())
            optimDirCnc.setAutoCommit(false);
    }
    
    @Override
    public void close() {
        if (optimDirCnc==null)
            return;
        try {
            if (optimDirCnc.isClosed()) {
                optimDirCnc = null;
                return;
            }
        } catch(Exception ex) {}
        try {
            if (optimDirCnc.isReadOnly()!=true)
                optimDirCnc.rollback();
        } catch(Exception ex) {}
        try {
            optimDirCnc.close();
        } catch(Exception ex) {}
        optimDirCnc = null;
    }

}
