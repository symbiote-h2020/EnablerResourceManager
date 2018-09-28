package eu.h2020.symbiote.enabler.resourcemanager.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.h2020.symbiote.security.ComponentSecurityHandlerFactory;
import eu.h2020.symbiote.security.commons.ComponentIdentifiers;
import eu.h2020.symbiote.security.commons.SecurityConstants;
import eu.h2020.symbiote.security.commons.exceptions.custom.SecurityHandlerException;
import eu.h2020.symbiote.security.handler.IComponentSecurityHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;


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
    public AuthorizationManager(@Value("${symbIoTe.component.username}") String componentOwnerName,
                                @Value("${symbIoTe.component.password}") String componentOwnerPassword,
                                @Value("${symbIoTe.localaam.url}") String aamAddress,
                                @Value("${platform.id}") String platformId,
                                @Value("${symbIoTe.component.keystore.path}") String keystoreName,
                                @Value("${symbIoTe.component.keystore.password}") String keystorePass,
                                @Value("${symbIoTe.aam.integration}") Boolean securityEnabled)
            throws SecurityHandlerException {

        Assert.notNull(componentOwnerName,"componentOwnerName can not be null!");
        this.componentOwnerName = componentOwnerName;

        Assert.notNull(componentOwnerPassword,"componentOwnerPassword can not be null!");
        this.componentOwnerPassword = componentOwnerPassword;

        Assert.notNull(aamAddress,"aamAddress can not be null!");
        this.aamAddress = aamAddress;

        Assert.notNull(platformId,"platformId can not be null!");
        this.clientId = ComponentIdentifiers.ENABLER_RESOURCE_MANAGER + "@" + platformId;

        Assert.notNull(keystoreName,"keystoreName can not be null!");
        this.keystoreName = keystoreName;

        Assert.notNull(keystorePass,"keystorePass can not be null!");
        this.keystorePass = keystorePass;

        Assert.notNull(securityEnabled,"securityEnabled can not be null!");
        this.securityEnabled = securityEnabled;

        if (this.securityEnabled)
            enableSecurity();
    }

    public Map<String, String> requestHomeToken() throws SecurityHandlerException {

        if (securityEnabled) {
            try {
                return componentSecurityHandler
                        .generateSecurityRequestUsingLocalCredentials()
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


    public boolean verifyServiceResponse(HttpHeaders httpHeaders, String componentId, String platformId) {
        if (securityEnabled) {
            String serviceResponse = null;

            if (httpHeaders.get(SecurityConstants.SECURITY_RESPONSE_HEADER) != null)
                serviceResponse = httpHeaders.get(SecurityConstants.SECURITY_RESPONSE_HEADER).get(0);

            if (serviceResponse == null)
                return false;
            else {
                try {
                    return componentSecurityHandler.isReceivedServiceResponseVerified(serviceResponse, componentId, platformId);
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
                keystoreName,
                keystorePass,
                clientId,
                aamAddress,
                componentOwnerName,
                componentOwnerPassword);

    }
}
