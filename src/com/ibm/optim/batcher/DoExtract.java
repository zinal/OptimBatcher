/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author zinal_m
 */
public class DoExtract {

    private final OptimCalls oc;

    private String dataSourceName;
    private List<String> tableList;

    public DoExtract(OptimCalls oc) {
        this.oc = oc;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }
    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public List<String> getTableList() {
        return tableList;
    }
    public void setTableList(List<String> tableList) {
        this.tableList = tableList;
    }

    public void run() throws Exception {
        
    }

    public static List<String> loadTablesFromFile(String fname) throws Exception {
        final BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(fname), "UTF-8"));
        try {
            final List<String> ll = new ArrayList<>();
            String line;
            while ((line=br.readLine())!=null) {
                line = line.trim().toUpperCase();
                if (line.length() > 0 && !line.startsWith("#"))
                    ll.add(line);
            }
            return ll;
        } finally {
            br.close();
        }
    }
    
}
