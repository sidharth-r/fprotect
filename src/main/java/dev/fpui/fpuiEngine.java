/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dev.fpui;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import org.apache.spark.launcher.SparkLauncher;

/**
 *
 * @author fprotect
 */
public class fpuiEngine {
    
    Runtime rt;
    
    SparkLauncher launchPrimFilter;
    SparkLauncher launchSecFilter;
    
    Process pPreProc;
    Process pPrimFilter;
    Process pSecFilter;
    Process pStreamSinkPrim;
    Process pStreamSinkSec;
    
    fpuiOutLogger outPrim;
    fpuiOutLogger outSec;
    fpuiOutLogger log;
    fpuiOutLogger stats;
    
    String sCmdPreprocStart;
    String sCmdStreamSinkStart;
    String sCmdPerfEval;
    
    fpuiProcOutWriter writerPreProc;
    fpuiProcOutWriter writerPreProcError;
    fpuiProcOutWriter writerPrimFilter;
    fpuiProcOutWriter writerPrimFilterError;
    fpuiProcOutWriter writerSecFilter;
    fpuiProcOutWriter writerSecFilterError;
    fpuiProcOutWriter writerStreamSinkPrim;
    fpuiProcOutWriter writerStreamSinkPrimError;
    fpuiProcOutWriter writerStreamSinkSec;
    fpuiProcOutWriter writerStreamSinkSecError;
    
    public fpuiEngine(Properties props, fpuiOutLogger[] outs)
    {
        rt = Runtime.getRuntime();
        
        outPrim = outs[0];
        outSec = outs[1];
        log = outs[2];
        log.setAutoRefresh(true);
        stats = outs[3];
        stats.setAutoRefresh(true);
        
        StringBuilder sb;
        
        String classPath = props.getProperty("fprotect.classpath");
        sb = new StringBuilder().append("java -cp ")
                .append(classPath)
                .append(" dev.finprotect.fpPreproc");
        sCmdPreprocStart = sb.toString();
        
        sb = new StringBuilder().append("java -cp ")
                .append(classPath)
                .append(" dev.finprotect.fpOutStreamDBSink");
        sCmdStreamSinkStart = sb.toString();
        
        sb = new StringBuilder().append("java -cp ")
                .append(classPath)
                .append(" dev.finprotect.fpPerfEval");
        sCmdPerfEval = sb.toString();
        
        String sparkHome = props.getProperty("spark.home");
        String sparkMaster = props.getProperty("spark.master");
        
        launchPrimFilter = new SparkLauncher()
                .setSparkHome(sparkHome)
                .setAppResource(classPath)
                .setMainClass("dev.finprotect.fpPrimFilter")
                .setMaster(sparkMaster);
        
        launchSecFilter = new SparkLauncher()
                .setSparkHome(sparkHome)
                .setAppResource(classPath)
                .setMainClass("dev.finprotect.fpSecFilter")
                .setMaster(sparkMaster);
    }
    
    public void setClassifier(String classifierModel)
    {
        launchSecFilter.addAppArgs(classifierModel);
    }
    
    public void start() throws IOException
    {
        log.write("Starting preprocessor.");
        log.writeDebug(sCmdPreprocStart);
        pPreProc = rt.exec(sCmdPreprocStart);
        writerPreProc = new fpuiProcOutWriter(pPreProc.getInputStream(),log);
        writerPreProc.start();
        writerPreProcError = new fpuiProcOutWriter(pPreProc.getErrorStream(),log);
        writerPreProcError.start();
        
        log.write("Starting primary filter.");
        pPrimFilter = launchPrimFilter.launch();
        writerPrimFilter = new fpuiProcOutWriter(pPrimFilter.getInputStream(),outPrim);
        writerPrimFilter.start();
        writerPrimFilterError = new fpuiProcOutWriter(pPrimFilter.getErrorStream(),outPrim);
        writerPrimFilterError.start();
        
        log.write("Starting secondary filter.");
        pSecFilter = launchSecFilter.launch();
        writerSecFilter = new fpuiProcOutWriter(pSecFilter.getInputStream(),outSec);
        writerSecFilter.start();
        writerSecFilterError = new fpuiProcOutWriter(pSecFilter.getErrorStream(),outSec);
        writerSecFilterError.start();
        
        log.write("Starting stream sinks.");
        log.writeDebug(sCmdStreamSinkStart+" fp_det_prim det_prim");
        pStreamSinkPrim = rt.exec(sCmdStreamSinkStart+" fp_det_prim det_prim");    
        writerStreamSinkPrim = new fpuiProcOutWriter(pStreamSinkPrim.getInputStream(),log);
        writerStreamSinkPrim.start();
        writerStreamSinkPrimError = new fpuiProcOutWriter(pStreamSinkPrim.getErrorStream(),log);
        writerStreamSinkPrimError.start();
        
        log.writeDebug(sCmdStreamSinkStart+" fp_det_sec det_sec");
        pStreamSinkSec = rt.exec(sCmdStreamSinkStart+" fp_det_sec det_sec");    
        writerStreamSinkSec = new fpuiProcOutWriter(pStreamSinkSec.getInputStream(),log);
        writerStreamSinkSec.start();
        writerStreamSinkSecError = new fpuiProcOutWriter(pStreamSinkSec.getErrorStream(),log);
        writerStreamSinkSecError.start();
    }
    
    public void stop() throws InterruptedException
    {
        log.write("Stopping.");
        
        writerPrimFilter.quit();
        writerPrimFilterError.quit();
        writerSecFilter.quit();
        writerSecFilterError.quit();
        writerStreamSinkPrim.quit();
        writerStreamSinkPrimError.quit();
        writerStreamSinkSec.quit();
        writerStreamSinkSecError.quit();
        
        pStreamSinkPrim.destroy();
        pStreamSinkSec.destroy();
        pSecFilter.destroy();
        pPrimFilter.destroy();
        pPreProc.destroy();
    }
    
    public void eval() throws IOException, InterruptedException
    {
        stats.clear();
        Process p = rt.exec(sCmdPerfEval);
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String ln;
        while((ln = reader.readLine()) != null)
        {
            stats.write(ln);
        }
        p.waitFor();
    }
    
}
