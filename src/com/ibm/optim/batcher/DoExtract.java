/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

import java.io.BufferedReader;
import java.io.FileInputStream;
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
        final StringBuilder sb = new StringBuilder();
        for (String tn : tableList) {
            addExtract(sb, tn);
        }
    }
    
    private void addExtract(StringBuilder sb, String tabName) throws Exception {
        final String eol = System.getProperty("line.separator");
        final String[] tabParts = tabName.split("[.]");
        if (tabParts.length!=2 || tabParts[0].trim().length()==0 || tabParts[1].trim().length()==0)
            throw new IllegalArgumentException("Invalid table name: [" + tabName + "]");
        sb.append("CREATE EXTR ");
        sb.append(oc.createId(dataSourceName, tabName, ObjectTypes.EXTRACT));
        sb.append(eol);
        sb.append("  DESC //Extract table ").append(dataSourceName)
                .append(".").append(tabName).append("//");
        sb.append(eol);
        sb.append("  XF //'")
                .append("EXTR-")
                .append(dataSourceName)
                .append(".")
                .append(tabName)
                .append(".XF")
                .append("'// ");
        sb.append(eol);
        sb.append("  LOCALAD (");
        sb.append(eol);
        sb.append("    SRCQUAL ").append(dataSourceName).append(tabParts[0]);
        sb.append(" START ").append(tabParts[1]);
        sb.append(" ADDTBLS N");
        sb.append(" MODCRIT N");
        sb.append(" ADCHGS Y");
        sb.append(" USENEW N");
        sb.append(" PNSSTATE N");
        sb.append(eol);
        sb.append(" TABLE (").append(tabParts[1]);
        sb.append(" REF N");
        sb.append(" PREDOP A");
        sb.append(" VARDELIM :");
        sb.append(" COLFLAG N");
        sb.append(" DAA N");
        sb.append(" UR N");
        sb.append(" )");
        sb.append(eol);
        sb.append("  )");
        sb.append(eol);
        sb.append("  PNSOVERRIDE N PNSOPT N");
        sb.append(eol);
        sb.append("  ALWAYSPROMPT N OPTION B");
        sb.append(eol);
        sb.append(";");
        sb.append(eol);
        sb.append(eol);
    }

    public static List<String> loadTablesFromFile(String fname) throws Exception {
        final BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(fname), "UTF-8"));
        try {
            final List<String> ll = new ArrayList<>();
            String line;
            while ((line=br.readLine())!=null) {
                line = line.trim().toUpperCase();
                if (line.length() > 0 && !line.startsWith("#")) {
                    String[] tmp = line.split("[.]");
                    if (tmp.length!=2 || tmp[0].trim().length()==0 || tmp[1].trim().length()==0)
                        throw new IllegalArgumentException("Invalid table name: [" + line + "]");
                    ll.add(line);
                }
            }
            return ll;
        } finally {
            br.close();
        }
    }
    
}
