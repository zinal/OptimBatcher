/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author zinal_m
 */
public class OptimCalls implements Closeable {

    private final Properties globalProps;
    private final Properties jobProps;

    // Used global properties (except Optim directory connection)
    private final String optimPath;
    private final String optimDirName;
    private final String importPath;
    private final String importRunAs;
    
    // Used job properties
    private final String jobGroup;
    private final String jobSourceAlias;
    private final String jobSourceSchema;
    private final String jobTargetAlias;
    private final String jobTargetSchema;
    private List<String> jobTableList;

    // Optim directory database connection
    private Connection optimDirCnc;

    public OptimCalls(Properties globalProps, Properties jobProps) {
        this.globalProps = globalProps;
        this.jobProps = jobProps;
        
        this.optimPath = globalProps.getProperty("optim.path");
        this.optimDirName = getProp(globalProps, "optim.dirname");
        this.importPath = getProp(globalProps, "import.path");
        this.importRunAs = getProp(globalProps, "import.runas");
        validateGlobalProps();
        
        this.jobGroup = getProp(jobProps, "optim.job.group");
        this.jobSourceAlias = jobProps.getProperty("optim.job.source.alias");
        this.jobSourceSchema = jobProps.getProperty("optim.job.source.schema");
        this.jobTargetAlias = jobProps.getProperty("optim.job.target.alias");
        this.jobTargetSchema = jobProps.getProperty("optim.job.target.schema");
        loadJobTableList();
        validateJobProps();
    }
    
    private static String getProp(Properties props, String name) {
        String val = props.getProperty(name);
        if (val!=null) {
            val = val.trim();
            if (val.length()==0)
                val = null;
        }
        return val;
    }

    private void validateGlobalProps() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void loadJobTableList() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void validateJobProps() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public final Properties getGlobalProps() {
        return globalProps;
    }
    public final Properties getJobProps() {
        return jobProps;
    }


    public final String getOptimPath() {
        return optimPath;
    }
    public final String getOptimDirName() {
        return optimDirName;
    }
    public final String getImportPath() {
        return importPath;
    }
    public final String getImportRunAs() {
        return importRunAs;
    }

    public final String getJobGroup() {
        if (jobGroup==null)
            return getJobSourceAlias();
        return jobGroup;
    }
    public final String getJobSourceAlias() {
        return jobSourceAlias;
    }
    public final String getJobSourceSchema() {
        return jobSourceSchema;
    }
    public final String getJobTargetAlias() {
        return jobTargetAlias;
    }
    public final String getJobTargetSchema() {
        return jobTargetSchema;
    }
    public final List<String> getJobTableList() {
        return jobTableList;
    }

    public void open() throws Exception {
        if (optimDirCnc!=null && optimDirCnc.isClosed()==false)
            return;
        final String clazzName = globalProps.getProperty("optimdir.jdbc.driver");
        if (clazzName!=null && clazzName.trim().length()>0) {
            Class.forName(clazzName.trim());
        }
        optimDirCnc = DriverManager.getConnection
            (globalProps.getProperty("optimdir.jdbc.url"),
             globalProps.getProperty("optimdir.jdbc.user"),
             globalProps.getProperty("optimdir.jdbc.password"));
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

    public String createId(String baseName, ObjectTypes type) throws Exception {
        String groupName = getJobGroup();
        if (groupName.length() > 8)
            groupName = groupName.substring(0, 8);
        final String[] baseParts = baseName.split("[.]");
        if (baseParts.length>1)
            baseName = baseParts[1];
        if (baseName.length() > 12)
            baseName = baseName.substring(0, 12);
        String id = groupName + "." + baseName;
        if (!idExists(id, type))
            return id;
        if (baseName.length() > 9)
            baseName = baseName.substring(0, 9);
        for (int i=0; i<99999; ++i) {
            id = groupName + "." + baseName + String.format("%02d", i);
            if (!idExists(id, type))
                return id;
        }
        throw new IllegalArgumentException("Cannot create unique ID for GN=[" + groupName
            + "], BN=[" + baseName + "], T=" + type);
    }

}
