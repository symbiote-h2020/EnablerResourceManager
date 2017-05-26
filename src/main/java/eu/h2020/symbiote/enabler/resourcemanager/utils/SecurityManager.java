package eu.h2020.symbiote.enabler.resourcemanager.utils;

import java.security.KeyStore;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import eu.h2020.symbiote.security.InternalSecurityHandler;
import eu.h2020.symbiote.security.certificate.CertificateVerificationException;
import eu.h2020.symbiote.security.exceptions.SecurityHandlerException;
import eu.h2020.symbiote.security.token.Token;
import eu.h2020.symbiote.security.exceptions.aam.TokenValidationException;
import eu.h2020.symbiote.security.session.AAM;
import eu.h2020.symbiote.security.exceptions.SecurityHandlerException;

@Component
public class SecurityManager {
	private static final Log log = LogFactory.getLog(SecurityManager.class);

	@Value("${symbiote.coreaam.url}")
	private String coreAAMUrl;

	@Value("${rabbit.host}")	
	private String rabbitMQHostIP;

	@Value("${rabbit.username}")
	private String rabbitMQUsername;	

	@Value("${rabbit.password}")
	private String rabbitMQPassword;

	@Value("${security.user}")
	private String userName;

	@Value("${security.password}")
	private String password;

	private InternalSecurityHandler securityHandler;
    private HashMap<String, AAM> aamsMap;

	@PostConstruct
	private void init() {
		securityHandler = new InternalSecurityHandler(coreAAMUrl, rabbitMQHostIP, rabbitMQUsername, rabbitMQPassword); 
		aamsMap = new HashMap<String, AAM>();
	}

	public Token requestCoreToken() throws SecurityException {
		log.info("Requesting core token");
		try{
			return securityHandler.requestCoreToken(userName, password);
		}
		catch (SecurityException e){
			log.info(e);
		}
		return null;
	}

	public void removeSavedTokens() throws SecurityException {
		log.info("Removing stored tokens");
		securityHandler.logout();
	}

	public Token requestPlatformToken(String platformId) throws SecurityException{

		requestCoreToken();
		log.info("Requesting platform token");

        AAM aam = aamsMap.get(platformId);
        
        if (aam == null) {
        	try {
	            rebuildAAMMap();
	        } catch (SecurityHandlerException e) {
              throw new SecurityException("Could not rebuildAAMMap due to: " + e);
	        }

            aam = aamsMap.get(platformId);
            if (aam == null)
              throw new SecurityException("The specified platform AAM with id = " + 
                                           platformId + " does not exist");
        }

		ArrayList<AAM> aamList = new ArrayList<AAM>();
		aamList.add(aam);

		Map<String, Token> tokens = securityHandler.requestForeignTokens(aamList);
		if (tokens != null) {
			Token token = tokens.get(platformId);
			if (token == null)
				throw new SecurityException("Token could not be acquired from platform AAM with id = " + 
	                                        platformId);
			else
				return token;
		} else
			throw new SecurityException("Token could not be acquired from platform AAM with id = " + 
	                                    platformId);
    }

	public boolean certificateValidation(KeyStore p12Certificate) throws SecurityException{
	    log.info("Validating certificate");
	    try{
		    return securityHandler.certificateValidation(p12Certificate);
		} catch (CertificateVerificationException e) {
		    log.error("error validating certificate");
	    }
	    return false;
	}

    private void rebuildAAMMap() throws SecurityHandlerException {
        List<AAM> listOfAAMs = securityHandler.getAvailableAAMs();

        for(Iterator iter = listOfAAMs.iterator(); iter.hasNext();) {
            AAM aam = (AAM) iter.next();
            aamsMap.put(aam.getAamInstanceId(), aam);
        }
        log.info(aamsMap);
    } 
}
