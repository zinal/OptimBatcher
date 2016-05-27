/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author zinal_m
 */
public class ConfigGenerator {

    private final OptimCalls oc;

    private String dataSourceName;
    private List<String> tableList;

    public ConfigGenerator(OptimCalls oc) {
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
        final String tempPath = oc.getProperties().getProperty("import.dir");
        final File fin;
        if (tempPath==null || tempPath.trim().length()==0)
            fin = File.createTempFile("OptimBatcher", "_in.txt");
        else
            fin = File.createTempFile("OptimBatcher", "_in.txt", new File(tempPath));
        final FileOutputStream fos = new FileOutputStream(fin);
        try {
            fos.write(sb.toString().getBytes("UTF-8"));
        } finally {
            fos.close();
        }
        String foutPath = fin.getAbsolutePath();
        foutPath = foutPath.substring(0, foutPath.length()-7) + "_out.txt";
        importFile(fin, new File(foutPath));
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
        sb.append("  XF //'").append(makeExtractFileName(tabName)).append("'// ");
        sb.append(eol);
        sb.append("  LOCALAD (");
        sb.append(eol);
        sb.append("    SRCQUAL ").append(dataSourceName)
                .append(".").append(tabParts[0]);
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
        sb.append("  PNSSTART ").append(dataSourceName)
                .append(".").append(tabName);
        sb.append(eol);
        sb.append("  ALWAYSPROMPT N OPTION B");
        sb.append(eol);
        sb.append("  INCLPK Y INCLFK Y INCLIDX Y");
        sb.append(eol);
        sb.append("  COMPRESSFILE Y ROWLIMIT 0");
        sb.append(eol);
        sb.append(";");
        sb.append(eol);
        sb.append(eol);
    }
    
    private String makeExtractFileName(String tabName) {
        return "EXTR-" + dataSourceName + "." + tabName + ".XF";
    }

    private void importFile(File fin, File fout) throws Exception {
        final List<String> cmd = new ArrayList<>();
        final String runas = oc.getProperties().getProperty("import.runas");
        if (runas!=null && runas.trim().length()>0) {
            for (String item : runas.split(" ")) {
                final String cur = item.trim();
                if (cur.length() > 0)
                    cmd.add(cur);
            }
        }
        final String optimDir = oc.getProperties().getProperty("optim.directory");
        cmd.add(new File(oc.getOptimPath(), "PR0CMND.EXE").getAbsolutePath());
        cmd.add("/IMPORT");
        cmd.add("IN=" + fin.getAbsolutePath());
        if (optimDir!=null && optimDir.trim().length()>0)
            cmd.add("D="+optimDir.trim());
        cmd.add("O=" + fout.getAbsolutePath());
        cmd.add("ContinueOnError+");
        final ProcessBuilder pb = new ProcessBuilder(cmd);
        final Process proc = pb.start();
        int code = proc.waitFor();
        if (code!=0)
            throw new RuntimeException("Optim configuration import process failed with code " + code);
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
