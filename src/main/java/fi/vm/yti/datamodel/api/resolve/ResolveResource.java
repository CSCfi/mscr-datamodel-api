/*
 * Licensed under the European Union Public Licence (EUPL) V.1.1 
 */
package fi.vm.yti.datamodel.api.resolve;

import fi.vm.yti.datamodel.api.utils.GraphManager;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.HttpHeaders;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;

/**
 *
 * @author malonen
 */
public class ResolveResource extends HttpServlet {

    private static final Logger logger = Logger.getLogger(ResolveResource.class.getName());
   
    /**
     *
     * Redirects namespace to the Schema file or the application depending on the Accept header
     *
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        String accept = request.getHeader(HttpHeaders.ACCEPT);
        String acceptLang = request.getHeader(HttpHeaders.ACCEPT_LANGUAGE);
        Locale locale = Locale.forLanguageTag(acceptLang);
        String language = locale.getDefault().toString().substring(0,2).toLowerCase();
        
        String ifModifiedSince = request.getHeader(HttpHeaders.IF_MODIFIED_SINCE);
        Date modifiedSince = null;
        Date modified = null;
        SimpleDateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
        
        if(ifModifiedSince!=null) {
            try {
                modifiedSince = format.parse(ifModifiedSince);
            } catch (ParseException ex) {
                Logger.getLogger(ResolveResource.class.getName()).log(Level.SEVERE, null, ex);
            } 
        }
        
        Lang rdfLang = RDFLanguages.contentTypeToLang(accept);
    
        String requestURI = request.getRequestURI();
        String modelID = requestURI.substring(requestURI.lastIndexOf("/") + 1, requestURI.length());
        
        
        logger.info("Resolving resource:");
        logger.info(modelID);
        logger.info(accept);
        logger.info(language);
        
        
        if(modelID.contains(".")) {
            String fileExt = modelID.split(Pattern.quote("."))[1];
            if(fileExt.equals("jschema")) accept = "application/schema+json";
            else if(fileExt.equals("xml")) accept = "application/xml";
            if(rdfLang==null) rdfLang = RDFLanguages.filenameToLang(modelID);
            modelID = modelID.split(Pattern.quote("."))[0];
            if(modelID.contains("-")) {
                language = modelID.split(Pattern.quote("-"))[1];
                modelID = modelID.split(Pattern.quote("-"))[0];
            } else language = null;
        }
        
        String graphName = GraphManager.getServiceGraphNameWithPrefix(modelID);
        
         if(modifiedSince!=null) {
                modified = GraphManager.lastModified(graphName);
                if(modified!=null) {
                    String dateModified = format.format(modified);
                    if(modifiedSince.after(modified)) {
                        response.reset();
                        response.addHeader("Last-Modified", dateModified);
                        response.sendError(HttpServletResponse.SC_NOT_MODIFIED,"Last-Modified: "+dateModified);
                        return;
                    }
                }
        }
     
        if(graphName==null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        } else {
        
                if(accept!=null && accept.equals("application/schema+json") || accept.equals("application/xml")) {
                    String dis = "/rest/exportModel?graph="+graphName+"&content-type="+accept+(language==null?"":"&lang="+language);
                    logger.info("Redirecting to export: "+dis);
                    RequestDispatcher dispatcher = getServletContext().getRequestDispatcher(dis);
                    dispatcher.forward(request,response);
                }
                else if(rdfLang!=null) {
                    String dis = "/rest/exportModel?graph="+graphName+"&content-type="+rdfLang.getHeaderString();
                    logger.info("Redirecting to export: "+dis);
                    RequestDispatcher dispatcher = getServletContext().getRequestDispatcher(dis);
                    dispatcher.forward(request,response);
                } else {
                    logger.info("Redirecting to root");
                    response.sendRedirect("/#/model?urn="+graphName);
                }    
        } 
      
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}
