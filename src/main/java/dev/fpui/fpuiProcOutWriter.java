/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dev.fpui;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author fprotect
 */
public class fpuiProcOutWriter extends Thread
{
    InputStream stream;
    fpuiOutLogger log;
    boolean fQuit;
    
    public fpuiProcOutWriter(InputStream istream, fpuiOutLogger logger)
    {
        stream = istream;
        log = logger;
        fQuit = false;
    }
    
    public void run()
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String ln;
        while(!fQuit)
        {
            try {
                if((ln = reader.readLine()) != null)
                {
                    log.write(ln);
                }
            } catch (IOException ex) {
                Logger.getLogger(fpuiProcOutWriter.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    public void quit()
    {
        fQuit = true;
    }
}
