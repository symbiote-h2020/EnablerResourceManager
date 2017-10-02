package eu.h2020.symbiote.enabler.resourcemanager.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.h2020.symbiote.security.ComponentSecurityHandlerFactory;
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
            ISecurityHandler iSecurityHandler = componentSecurityHandler.getSecurityHandler();
            Set<AuthorizationCredentials> authorizationCredentialsSet = new HashSet<>();

            try {
                Map<String, AAM> availableAAMs = iSecurityHandler.getAvailableAAMs();
                // Todo: Change hardcoded values "username" and "password"
                iSecurityHandler.getCertificate(availableAAMs.get(platformId), "username", "password", clientId);
                Token homeToken = iSecurityHandler.login(availableAAMs.get(platformId));

                HomeCredentials homeCredentials = iSecurityHandler.getAcquiredCredentials().get(platformId).homeCredentials;

                authorizationCredentialsSet.add(new AuthorizationCredentials(homeToken, homeCredentials.homeAAM, homeCredentials));

                return MutualAuthenticationHelper.getSecurityRequest(authorizationCredentialsSet, false)
                        .getSecurityRequestHeaderParams();
            } catch (NoSuchAlgorithmException | ValidationException | JsonProcessingException e) {
                log.error(e);
                throw new SecurityHandlerException("Failed to generate security request: " + e.getMessage());
            }
        } else {
            log.debug("Security is disabled. Returning empty Map");
            return new HashMap<>();
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
