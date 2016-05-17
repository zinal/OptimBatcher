/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

/**
 *
 * @author zinal_m
 */
final class DoExtract {

    private final OptimCalls oc;
    
    private final String dataSourceName;
    private final String tableListFileName;

    public DoExtract(OptimCalls oc) {
        this.oc = oc;
        this.dataSourceName = oc.getCallArgs()[0];
        this.tableListFileName = oc.getCallArgs()[1];
    }
    
    public void run() throws Exception {
        
    }

}
