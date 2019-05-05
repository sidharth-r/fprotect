/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dev.fpui;

import dev.finprotect.*;
import java.lang.Exception;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.launcher.SparkLauncher;

/**
 *
 * @author fprotect
 */
public class fpUI extends javax.swing.JFrame {
    
    Process pPreProc;
    Process pPrimFilter;
    Process pSecFilter;
    Process pStreamSink;
    
    /**
     * Creates new form fpuiFrameMain
     */
    public fpUI() {
        initComponents();
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        panelMain = new javax.swing.JPanel();
        panelTextArea = new javax.swing.JScrollPane();
        textOutput = new javax.swing.JTextArea();
        buttonStart = new javax.swing.JButton();
        buttonStop = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setTitle("FProtect");
        setName("frameMain"); // NOI18N

        textOutput.setEditable(false);
        textOutput.setBackground(new java.awt.Color(255, 255, 255));
        textOutput.setColumns(20);
        textOutput.setLineWrap(true);
        textOutput.setRows(5);
        panelTextArea.setViewportView(textOutput);

        buttonStart.setText("Start");
        buttonStart.setName("buttonStart"); // NOI18N
        buttonStart.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                buttonStartMouseClicked(evt);
            }
        });

        buttonStop.setText("Stop");
        buttonStop.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                buttonStopMouseClicked(evt);
            }
        });

        javax.swing.GroupLayout panelMainLayout = new javax.swing.GroupLayout(panelMain);
        panelMain.setLayout(panelMainLayout);
        panelMainLayout.setHorizontalGroup(
            panelMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, panelMainLayout.createSequentialGroup()
                .addGap(148, 148, 148)
                .addGroup(panelMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(buttonStart)
                    .addComponent(buttonStop))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 264, Short.MAX_VALUE)
                .addComponent(panelTextArea, javax.swing.GroupLayout.PREFERRED_SIZE, 481, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap())
        );
        panelMainLayout.setVerticalGroup(
            panelMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelMainLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(panelMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(panelTextArea, javax.swing.GroupLayout.DEFAULT_SIZE, 457, Short.MAX_VALUE)
                    .addGroup(panelMainLayout.createSequentialGroup()
                        .addComponent(buttonStart)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(buttonStop)
                        .addGap(0, 0, Short.MAX_VALUE)))
                .addContainerGap())
        );

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(panelMain, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addContainerGap())
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(panelMain, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addContainerGap())
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void buttonStartMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_buttonStartMouseClicked
        try {
            pPreProc = Runtime.getRuntime().exec("java -cp '/home/fprotect/finprotect/fprotect/target/fprotect-0.1.jar' dev.finprotect.fpPreproc");
            if(pPreProc.isAlive())
                textOutput.append("Started Preprocessor.\n");
            else
                return;
            pPrimFilter = new SparkLauncher()
                    .setSparkHome("/home/fprotect/finprotect/spark-2.3.1-bin-hadoop2.7")
                    .setAppResource("/home/fprotect/finprotect/fprotect/target/fprotect-0.1.jar")
                    .setMainClass("dev.finprotect.fpPrimFilter")
                    .setMaster("local[*]")
                    .launch();
            if(pPrimFilter.isAlive())
                textOutput.append("Started Primary Filter.\n");
            else
                return;
            pSecFilter = new SparkLauncher()
                    .setSparkHome("/home/fprotect/finprotect/spark-2.3.1-bin-hadoop2.7")
                    .setAppResource("/home/fprotect/finprotect/fprotect/target/fprotect-0.1.jar")
                    .setMainClass("dev.finprotect.fpSecFilter")
                    .setMaster("local[*]")
                    .launch();
            if(pSecFilter.isAlive())
                textOutput.append("Started Secondary Filter.\n");
            else
                return;
            pStreamSink = Runtime.getRuntime().exec("java -cp '/home/fprotect/finprotect/fprotect/target/fprotect-0.1.jar' dev.finprotect.fpOutStreamDBSink");
            if(pStreamSink.isAlive())
                textOutput.append("Started Stream DB Sink.\n");
            else
                return;
            buttonStart.setEnabled(false);
        } catch (Exception ex) {
            Logger.getLogger(fpUI.class.getName()).log(Level.SEVERE, null, ex);
        }
    }//GEN-LAST:event_buttonStartMouseClicked

    private void buttonStopMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_buttonStopMouseClicked
        pPreProc.destroy();
        pPrimFilter.destroy();
        pSecFilter.destroy();
        pStreamSink.destroy();
        textOutput.append("Stopping.\n");
    }//GEN-LAST:event_buttonStopMouseClicked

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(fpUI.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(fpUI.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(fpUI.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(fpUI.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new fpUI().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton buttonStart;
    private javax.swing.JButton buttonStop;
    private javax.swing.JPanel panelMain;
    private javax.swing.JScrollPane panelTextArea;
    private javax.swing.JTextArea textOutput;
    // End of variables declaration//GEN-END:variables
}
