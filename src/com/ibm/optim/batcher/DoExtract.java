/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

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

}
