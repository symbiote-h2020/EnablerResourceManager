package eu.h2020.symbiote.enabler.resourcemanager.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.h2020.symbiote.security.ComponentSecurityHandlerFactory;
import eu.h2020.symbiote.security.commons.SecurityConstants;
import eu.h2020.symbiote.security.commons.Token;
import eu.h2020.symbiote.security.commons.credentials.AuthorizationCredentials;
import eu.h2020.symbiote.security.commons.credentials.HomeCredentials;
import eu.h2020.symbiote.security.commons.exceptions.custom.InvalidArgumentsException;
import eu.h2020.symbiote.security.commons.exceptions.custom.SecurityHandlerException;
import eu.h2020.symbiote.security.commons.exceptions.custom.ValidationException;
import eu.h2020.symbiote.security.communication.payloads.AAM;
import eu.h2020.symbiote.security.communication.payloads.SecurityRequest;
import eu.h2020.symbiote.security.handler.IComponentSecurityHandler;
import eu.h2020.symbiote.security.handler.ISecurityHandler;
import eu.h2020.symbiote.security.helpers.MutualAuthenticationHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Component responsible for dealing with Symbiote Tokens and checking access right for requests.
 *
 * @author mateuszl
 * @author vasgl
 */
@Component()
public class AuthorizationManager {

    private static Log log = LogFactory.getLog(AuthorizationManager.class);

    private String componentOwnerName;
    private String componentOwnerPassword;
    private String aamAddress;
    private String clientId;
    private String keystoreName;
    private String keystorePass;
    private Boolean securityEnabled;

    private IComponentSecurityHandler componentSecurityHandler;

    @Autowired
    public AuthorizationManager(@Value("${aam.deployment.owner.username}") String componentOwnerName,
                                @Value("${aam.deployment.owner.password}") String componentOwnerPassword,
                                @Value("${aam.environment.aamAddress}") String aamAddress,
                                @Value("${enablerResourceManager.environment.clientId}") String clientId,
                                @Value("${enablerResourceManager.environment.keystoreName}") String keystoreName,
                                @Value("${enablerResourceManager.environment.keystorePass}") String keystorePass,
                                @Value("${enablerResourceManager.security.enabled}") Boolean securityEnabled)
            throws SecurityHandlerException, InvalidArgumentsException {

        Assert.notNull(componentOwnerName,"componentOwnerName can not be null!");
        this.componentOwnerName = componentOwnerName;

        Assert.notNull(componentOwnerPassword,"componentOwnerPassword can not be null!");
        this.componentOwnerPassword = componentOwnerPassword;

        Assert.notNull(aamAddress,"aamAddress can not be null!");
        this.aamAddress = aamAddress;

        Assert.notNull(clientId,"clientId can not be null!");
        this.clientId = clientId;

        Assert.notNull(keystoreName,"keystoreName can not be null!");
        this.keystoreName = keystoreName;

        Assert.notNull(keystorePass,"keystorePass can not be null!");
        this.keystorePass = keystorePass;

        Assert.notNull(securityEnabled,"securityEnabled can not be null!");
        this.securityEnabled = securityEnabled;

        if (this.securityEnabled)
            enableSecurity();
    }

    public Map<String, String> requestHomeToken(String platformId) throws SecurityHandlerException {

        if (securityEnabled) {
            try {
                // Todo: Ask Mikolaj how I can get credentials from more than 1 home platform
                return componentSecurityHandler.generateSecurityRequestUsingLocalCredentials()
                        .getSecurityRequestHeaderParams();
            } catch (JsonProcessingException e) {
                log.error(e);
                throw new SecurityHandlerException("Failed to generate security request: " + e.getMessage());
            }
        } else {
            log.debug("Security is disabled. Returning empty Map");
            return new HashMap<>();
        }
    }


    public boolean verifyServiceResponse(HttpHeaders httpHeaders, String componentIdentifier, String platformId) {
        if (securityEnabled) {
            String serviceResponse = httpHeaders.get(SecurityConstants.SECURITY_RESPONSE_HEADER).get(0);

            if (serviceResponse == null)
                return false;
            else {
                try {
                    return componentSecurityHandler.isReceivedServiceResponseVerified(serviceResponse,
                            componentIdentifier, platformId);
                } catch (SecurityHandlerException e) {
                    log.info("Exception during serviceResponse verification", e);
                    return false;
                }
            }

        } else {
            log.debug("Security is disabled. Returning true");
            return true;
        }
    }


    private void enableSecurity() throws SecurityHandlerException {
        securityEnabled = true;
        componentSecurityHandler = ComponentSecurityHandlerFactory.getComponentSecurityHandler(
                aamAddress,
                keystoreName,
                keystorePass,
                clientId,
                aamAddress,
                false,
                componentOwnerName,
                componentOwnerPassword);

    }


    /**
     * Setters and Getters
     */

    public IComponentSecurityHandler getComponentSecurityHandler() {
        return componentSecurityHandler;
    }

    public void setComponentSecurityHandler(IComponentSecurityHandler componentSecurityHandler) {
        this.componentSecurityHandler = componentSecurityHandler;
    }
}
