/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

/**
 *
 * @author zinal_m
 */
public class OptimCalls implements Closeable {

    private final Properties properties;
    private final String optimPath;
    
    private Connection optimDirCnc;
    
    public OptimCalls(Properties props) {
        this.properties = props;
        this.optimPath = props.getProperty("optim.path");
    }

    public Properties getProperties() {
        return properties;
    }
    public String getOptimPath() {
        return optimPath;
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
    
    public boolean idExists(String id, ObjectTypes type) throws Exception {
        final String[] items = id.split("[.]");
        if (items.length!=2)
            throw new IllegalArgumentException("Invalid ID format: [" + id + "]");
        final PreparedStatement ps = optimDirCnc.prepareStatement(""
                + "SELECT COUNT(1) FROM PSTOBJ2 WHERE OBJ_ID=? AND OBJ_NAME=? AND OBJ_TYPE=?");
        try {
            ps.setString(1, items[0].toUpperCase());
            ps.setString(2, items[1].toUpperCase());
            ps.setString(3, type.getCode());
            final ResultSet rs = ps.executeQuery();
            try {
                return ( rs.next() && rs.getInt(1)>0 );
            } finally {
                rs.close();
            }
        } finally {
            ps.close();
        }
    }
    
    public String createId(String dataSource, String baseName, ObjectTypes type) throws Exception {
        if (dataSource.length() > 8)
            dataSource = dataSource.substring(0, 8);
        final String[] baseParts = baseName.split("[.]");
        if (baseParts.length>1)
            baseName = baseParts[1];
        if (baseName.length() > 12)
            baseName = baseName.substring(0, 12);
        String id = dataSource + "." + baseName;
        if (!idExists(id, type))
            return id;
        if (baseName.length() > 9)
            baseName = baseName.substring(0, 9);
        for (int i=0; i<99999; ++i) {
            id = dataSource + "." + baseName + String.format("%02d", i);
            if (!idExists(id, type))
                return id;
        }
        throw new IllegalArgumentException("Cannot create unique ID for DS=[" + dataSource
            + "], BN=[" + baseName + "], T=" + type);
    }

}
