/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

/**
 *
 * @author zinal_m
 */
public enum ObjectTypes {
    
    EXTRACT("E"),
    CONVERT("N"),
    ;
 
    final String code;
    
    ObjectTypes(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
    
}
