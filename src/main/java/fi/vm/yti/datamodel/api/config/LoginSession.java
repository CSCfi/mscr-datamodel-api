/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fi.vm.yti.datamodel.api.config;

import fi.vm.yti.datamodel.api.utils.ServiceDescriptionManager;

import java.util.HashMap;
import java.util.logging.Logger;
import javax.servlet.http.HttpSession;

/**
 *
 * @author malonen
 */
public class LoginSession implements LoginInterface {

    private HttpSession session;
    private static final Logger logger = Logger.getLogger(LoginSession.class.getName());
    
    public LoginSession(HttpSession httpSession) {
        this.session = httpSession;
    }

    @Override
    public boolean isLoggedIn() {
        return (session.getAttribute("mail")!=null);
    }


    @Override
    public boolean isInGroup(String group) {
        return session.getAttribute("group").toString().contains(group);
    }

    @Override
    public String getDisplayName() {
        return session.getAttribute("displayName").toString();
    }
    
    public boolean isSuperAdmin() {
        return session.getAttribute("group").toString().contains("https://tt.eduuni.fi/sites/csc-iow#IOW_ADMINS");
    }

    @Override
    public String getEmail() {
       return session.getAttribute("mail").toString();
    }

    @Override
    public HashMap<String,Boolean> getGroups() {
        
        /* Group string format: https://example.org#GROUOP_ADMINS;https://example.org#GROUP_MEMBERS;...;*/
        
        String[] groupString;
        
        if(ApplicationProperties.getDebugMode()) {
            groupString = ApplicationProperties.getDebugGroups().split(";");
        } else if(session.getAttribute("group")==null) {
            return null;
        } else {
            groupString = session.getAttribute("group").toString().split(";");
        }
        
        HashMap groups = new HashMap();

        for (int i = 0; i<groupString.length;i++){
            
            String[] myGroup = groupString[i].split("_");
            
            if(myGroup[0].startsWith(ApplicationProperties.getGroupDomain())) {
                if(myGroup[1].equals("ADMINS")) {
                    groups.put(myGroup[0], Boolean.TRUE); }
                else {
                    groups.put(myGroup[0], Boolean.FALSE); }
            }
        }
        
        return groups;
       
    }

    @Override
    public boolean hasRightToEditModel(String model) {
        
        if(this.getGroups()==null) return false;

        return ServiceDescriptionManager.isModelInGroup(model,this.getGroups());
    }
    
    @Override
    public boolean hasRightToEditGroup(String group) {
        
        if(this.getGroups()==null) return false;
        
        if(ApplicationProperties.getDebugMode()) return true;
       
        return isInGroup(group);
    }

    @Override
    public boolean isAdminOfGroup(String group) {
      
        if(this.getGroups()==null) return false;
        
        return session.getAttribute("group").toString().contains(group.concat("_ADMINS"));
        
    }
    
}
