/*
 * IBM proprietary
 */
package com.ibm.optim.batcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
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
        // Create a buffer with a set of Optim import commands
        final StringBuilder sb = new StringBuilder();
        for (String tn : tableList) {
            new Maker(tn).run(sb);
        }
        // Build unique temp filenames for input commands and output execution report
        final String tempPath = oc.getProperties().getProperty("import.dir");
        final File fin;
        if (tempPath==null || tempPath.trim().length()==0)
            fin = File.createTempFile("OptimBatcher", "_in.txt");
        else
            fin = File.createTempFile("OptimBatcher", "_in.txt", new File(tempPath));
        try {
            final FileOutputStream fos = new FileOutputStream(fin);
            try {
                fos.write(sb.toString().getBytes("UTF-8"));
            } finally {
                fos.close();
            }
            String foutPath = fin.getAbsolutePath();
            foutPath = foutPath.substring(0, foutPath.length()-7) + "_out.txt";
            final File fout = new File(foutPath);
            try {
                // Perform Optim import and display both commands and execution report
                importFile(fin, fout);
            } finally {
                fout.delete();
            }
        } finally {
            fin.delete();
        }
    }
    
    private void importFile(File fin, File fout) throws Exception {
        System.out.println();
        dumpFile(fin, "Optim input commands", "INP> ");

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
        
        System.out.println();
        System.out.println("********* BEGIN COMMAND DUMP **********");
        for (String item : cmd) {
            System.out.println("\t" + item);
        }
        System.out.println("********** END COMMAND DUMP ***********");

        final ProcessBuilder pb = new ProcessBuilder(cmd);
        final Process proc = pb.start();
        int code = proc.waitFor();

        System.out.println();
        dumpFile(fout, "Optim output logfile", "OUT> ");
        System.out.println();

        if (code!=0) {
            throw new RuntimeException("Optim configuration import process failed with code " + code);
        }
    }
    
    private void dumpFile(File f, String what, String prefix) throws Exception {
        System.out.println("*********** BEGIN FILE DUMP ***********");
        System.out.println("** File name: " + f.getAbsolutePath());
        System.out.println("** " + what);
        System.out.println("** ");
        if (!f.exists() || !f.canRead() || !f.isFile()) {
            System.out.println("** FILE DOES NOT EXIST!!!");
        } else {
            final BufferedReader br = new BufferedReader(new FileReader(f));
            try {
                String line;
                while ((line=br.readLine())!=null) {
                    System.out.println(prefix + line);
                }
            } finally {
                br.close();
            }
        }
        System.out.println("** ");
        System.out.println("************ END FILE DUMP ************");
    }
    
    private class Maker {
        final String eol;
        final String tabName;
        final String[] tabParts;
        final String extractServiceName;
        final String convertServiceName;
        
        Maker(String tabName) throws Exception {
            this.eol = System.getProperty("line.separator");
            this.tabName = tabName;
            this.tabParts = tabName.split("[.]");
            if (tabParts.length!=2 || tabParts[0].trim().length()==0 || tabParts[1].trim().length()==0)
                throw new IllegalArgumentException("Invalid table name: [" + tabName + "]");
            this.extractServiceName = oc.createId(dataSourceName, tabName, ObjectTypes.EXTRACT);
            this.convertServiceName = oc.createId(dataSourceName, tabName, ObjectTypes.CONVERT);
        }
        
        void run(StringBuilder sb) throws Exception {
            addExtract(sb);
            addConvert(sb);
        }

        private void addExtract(StringBuilder sb) throws Exception {
            sb.append("CREATE EXTR ");
            sb.append(extractServiceName);
            sb.append(eol);
            sb.append("  DESC //Extract table ").append(dataSourceName)
                    .append(".").append(tabName).append("//");
            sb.append(eol);
            sb.append("  XF //'").append(makeExtractFileName()).append("'// ");
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
            sb.append(";");
            sb.append(eol);
            sb.append(eol);
        }

        private void addConvert(StringBuilder sb)
                throws Exception {
            sb.append("CREATE CONV ");
            sb.append(convertServiceName);
            sb.append(eol);
            sb.append("  DESC //Transform table ").append(dataSourceName)
                    .append(".").append(tabName).append(" after data extraction")
                    .append("//");
            sb.append(eol);
            sb.append("  SRCXF //'").append(makeExtractFileName()).append("'// ");
            sb.append(eol);
            sb.append("  DESTXF //'").append(makeExtractFileName()).append("'// ");
            sb.append(eol);
            sb.append("  FORCEEDITTM N DELCNTLFILE N ");
            sb.append(eol);
            sb.append("  LOCALTM (");
            sb.append(eol);

            sb.append("  )");
            sb.append(eol);
            sb.append("  SHOWCURRENCY N SHOWAGE N");
            sb.append(eol);
            sb.append("  COMPRESSFILE Y INCL_FILEATTACH Y");
            sb.append(eol);
            sb.append("  CONVACTN ( USEACTN N ) ;");
            sb.append(eol);
        }

        private String makeExtractFileName() {
            return "EXTR-" + extractServiceName + ".XF";
        }
    }

}
