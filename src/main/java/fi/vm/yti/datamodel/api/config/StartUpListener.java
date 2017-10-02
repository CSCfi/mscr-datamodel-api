/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fi.vm.yti.datamodel.api.config;

import fi.vm.yti.datamodel.api.utils.GraphManager;
import fi.vm.yti.datamodel.api.utils.OPHCodeServer;
import fi.vm.yti.datamodel.api.utils.GroupManager;
import fi.vm.yti.datamodel.api.utils.NamespaceManager;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 *
 * @author malonen
 */
public class StartUpListener implements ServletContextListener {

    private static final Logger logger = Logger.getLogger(StartUpListener.class.getName());
            
    @Override
    public void contextInitialized(ServletContextEvent sce) {
        
        System.out.println("System is starting ...");
        logger.log(Level.INFO, "System is starting ...");
        
        initDefaultGraph();
        initDefaultGroups();
        initDefaultNamespaces();
        initCodeServers();
        //loadSchemesFromFinto();
        // createExportGraphs();
       //  runPeriodicUpdates();
        
    }
    
    @Override
    public void contextDestroyed(ServletContextEvent sce) {
       System.out.println("System is closing ...");
       logger.log(Level.INFO, "System is closing ...");
    }
    
    private static void initCodeServers() {
       
            OPHCodeServer codeServer = new OPHCodeServer("https://virkailija.opintopolku.fi/koodisto-service/rest/json/", true);
          //  OPHCodeServer testCodeServer = new OPHCodeServer("http://iowkoodisto.csc.fi:8080/koodisto-service/rest/json/", true);
            
            if(!codeServer.status) logger.warning("Code server was not initialized!");
         //   if(!testCodeServer.status) logger.warning("TESTCode server was not initialized!");
    
    }
    
    private static void runPeriodicUpdates() {
        /*
        LocalDateTime localNow = LocalDateTime.now();
        ZoneId currentZone = ZoneId.of("America/Los_Angeles");
        ZonedDateTime zonedNow = ZonedDateTime.of(localNow, currentZone);
        ZonedDateTime zonedNext5 ;
        zonedNext5 = zonedNow.withHour(5).withMinute(0).withSecond(0);
        if(zonedNow.compareTo(zonedNext5) > 0)
            zonedNext5 = zonedNext5.plusDays(1);

        Duration duration = Duration.between(zonedNow, zonedNext5);
        long initialDelay = duration.getSeconds();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1); */
        /* TODO: Create runnable to loadSchemesFromFinto() */
       /* scheduler.
                .scheduleAtFixedRate(FintoLoad), initalDelay,
                                      24*60*60, TimeUnit.SECONDS); */
        
    }
    
    private static void createExportGraphs() {
        
      GraphManager.updateAllExportGraphs();
        
    }
    
    private static void loadSchemesFromFinto() {
       // ConceptMapper.updateSchemesFromFinto();
    }
    
    private static void initDefaultGraph() {
         if(GraphManager.testDefaultGraph()) {
                logger.log(Level.INFO,"Default graph is initialized!");
         }
         else {
                logger.log(Level.WARNING,"Default graph is NOT initialized!");
                GraphManager.createDefaultGraph();
                if(GraphManager.testDefaultGraph())
                    logger.log(Level.INFO,"Created NEW DEFAULT graph!");
                else
                    logger.log(Level.WARNING,"Failed to create default graph!");
              }
    }
    private static void initDefaultGroups() {
         if(GroupManager.compareDefaultGroups()) {
                logger.log(Level.INFO,"Default groups are initialized!");
         }
         else {
                logger.log(Level.WARNING,"Default groups are NOT initialized!");
                
                GroupManager.createDefaultGroups();
                
                if(GroupManager.compareDefaultGroups())
                    logger.log(Level.INFO,"Created default groups!");
                else
                    logger.log(Level.WARNING,"Failed to create default groups!");
              }
    }
    
    private static void initDefaultNamespaces() {
        
        NamespaceManager.resolveDefaultNamespaceToTheCore();
        
    }
    
}
