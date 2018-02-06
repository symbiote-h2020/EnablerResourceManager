package eu.h2020.symbiote.enabler.resourcemanager.utils;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.h2020.symbiote.security.ClientSecurityHandlerFactory;
import eu.h2020.symbiote.security.commons.SecurityConstants;
import eu.h2020.symbiote.security.commons.Token;
import eu.h2020.symbiote.security.commons.credentials.AuthorizationCredentials;
import eu.h2020.symbiote.security.commons.credentials.HomeCredentials;
import eu.h2020.symbiote.security.commons.exceptions.custom.InvalidArgumentsException;
import eu.h2020.symbiote.security.commons.exceptions.custom.SecurityHandlerException;
import eu.h2020.symbiote.security.commons.exceptions.custom.ValidationException;
import eu.h2020.symbiote.security.communication.payloads.AAM;
import eu.h2020.symbiote.security.handler.ISecurityHandler;
import eu.h2020.symbiote.security.helpers.MutualAuthenticationHelper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
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

    private String username;
    private String password;
    private String caamAddress;
    private String clientId;
    private String userId;
    private String keystoreName;
    private String keystorePass;
    private Boolean securityEnabled;

    private ISecurityHandler securityHandler;

    @Autowired
    public AuthorizationManager(@Value("${enablerResourceManager.environment.username}") String username,
                                @Value("${enablerResourceManager.environment.password}") String password,
                                @Value("${enablerResourceManager.environment.caamAddress}") String caamAddress,
                                @Value("${enablerResourceManager.environment.clientId}") String clientId,
                                @Value("${enablerResourceManager.environment.userId}") String userId,
                                @Value("${enablerResourceManager.environment.keystoreName}") String keystoreName,
                                @Value("${enablerResourceManager.environment.keystorePass}") String keystorePass,
                                @Value("${enablerResourceManager.security.enabled}") Boolean securityEnabled)
            throws SecurityHandlerException, InvalidArgumentsException, NoSuchAlgorithmException {

        Assert.notNull(username,"username can not be null!");
        this.username = username;

        Assert.notNull(password,"password can not be null!");
        this.password = password;

        Assert.notNull(caamAddress,"caamAddress can not be null!");
        this.caamAddress = caamAddress;

        Assert.notNull(clientId,"clientId can not be null!");
        this.clientId = clientId;

        Assert.notNull(userId,"userId can not be null!");
        this.userId = userId;

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

        platformId = SecurityConstants.CORE_AAM_INSTANCE_ID;

        if (securityEnabled) {
            try {
                Set<AuthorizationCredentials> authorizationCredentialsSet = new HashSet<>();
                Map<String, AAM> availableAAMs = securityHandler.getAvailableAAMs();

                log.info("Getting certificate for " + availableAAMs.get(platformId).getAamInstanceId());
                securityHandler.getCertificate(availableAAMs.get(platformId), username, password, clientId);

                log.info("Getting token from " + availableAAMs.get(platformId).getAamInstanceId());
                Token homeToken = securityHandler.login(availableAAMs.get(platformId));

                HomeCredentials homeCredentials = securityHandler.getAcquiredCredentials().get(platformId).homeCredentials;
                authorizationCredentialsSet.add(new AuthorizationCredentials(homeToken, homeCredentials.homeAAM, homeCredentials));

                return MutualAuthenticationHelper.getSecurityRequest(authorizationCredentialsSet, false)
                        .getSecurityRequestHeaderParams();

            } catch (JsonProcessingException | NoSuchAlgorithmException | ValidationException e) {
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
                    return MutualAuthenticationHelper.isServiceResponseVerified(
                            serviceResponse, securityHandler.getComponentCertificate(componentId, platformId));
                } catch (NoSuchAlgorithmException | CertificateException | SecurityHandlerException e) {
                    log.info("Exception during serviceResponse verification", e);
                    return false;
                }
            }

        } else {
            log.debug("Security is disabled. Returning true");
            return true;
        }
    }


    private void enableSecurity() throws SecurityHandlerException, NoSuchAlgorithmException {
        TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    public void checkClientTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }

                    public void checkServerTrusted(
                            java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
        };
        // Install the all-trusting trust manager
        SSLContext sc = SSLContext.getInstance("SSL");
        try {
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
        } catch (KeyManagementException e) {
            e.printStackTrace();
        }
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

        securityEnabled = true;
        securityHandler = ClientSecurityHandlerFactory.getSecurityHandler(caamAddress, keystoreName, keystorePass, userId);

    }


    /**
     * Setters and Getters
     */

    public ISecurityHandler getSecurityHandler() {
        return securityHandler;
    }
}
