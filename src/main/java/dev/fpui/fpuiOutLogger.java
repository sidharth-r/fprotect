/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dev.fpui;

import javax.swing.JTextArea;

/**
 *
 * @author fprotect
 */
public class fpuiOutLogger{
    
    JTextArea to;
    
    boolean autoRefresh;

    fpuiOutLogger(JTextArea textOut)
    {
        to = textOut;
        autoRefresh = false;
    }

    public void write(String str)
    {
        to.append(str+"\n");
        if(autoRefresh)
            refresh();
    }
    
    public void writeDebug(String str)
    {
        to.append("DEBUG: "+str+"\n");
        if(autoRefresh)
            refresh();
    }
    
    public void clear()
    {
        to.setText("");
    }
    
    public void refresh()
    {
        to.update(to.getGraphics());
    }
    
    public void setAutoRefresh(boolean auto)
    {
        autoRefresh = auto;
    }
}