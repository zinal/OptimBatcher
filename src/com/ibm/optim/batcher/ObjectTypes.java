/*
 * Sample code for IBM InfoSphere Optim software
 */
package com.ibm.optim.batcher;

/**
 * List of Optim service types used by the OptimBatcher utility
 * @author zinal_m
 */
public enum ObjectTypes {
    
    EXTRACT("E"),
    CONVERT("N"),
    LOAD("L"),
    ;
 
    final String code;
    
    ObjectTypes(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }
    
}
