package eu.h2020.symbiote.enabler.resourcemanager.aams;


import eu.h2020.symbiote.security.certificate.Certificate;
import eu.h2020.symbiote.security.constants.AAMConstants;
import eu.h2020.symbiote.security.enums.IssuingAuthorityType;
import eu.h2020.symbiote.security.enums.ValidationStatus;
import eu.h2020.symbiote.security.exceptions.aam.JWTCreationException;
import eu.h2020.symbiote.security.exceptions.aam.TokenValidationException;
import eu.h2020.symbiote.security.payloads.CheckRevocationResponse;
import eu.h2020.symbiote.security.payloads.Credentials;
import eu.h2020.symbiote.security.session.AAM;
import eu.h2020.symbiote.security.token.Token;
import eu.h2020.symbiote.security.token.jwt.JWTEngine;
import eu.h2020.symbiote.security.token.jwt.JWTClaims;
import eu.h2020.symbiote.security.exceptions.aam.MalformedJWTException;
import eu.h2020.symbiote.core.ci.QueryResponse;
import eu.h2020.symbiote.core.ci.QueryResourceResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;


/**
 * Dummy REST service mimicking exposed AAM features required by SymbIoTe users and reachable via CoreInterface in the Core and Interworking Interfaces on Platforms' side.
 *
 * @author Mikołaj Dobski (PSNC)
 */
@RestController
@WebAppConfiguration
public class DummyAAMRestListeners {
    private static final Log log = LogFactory.getLog(DummyAAMRestListeners.class);

    @Autowired
    @Qualifier("symbIoTeCoreUrl")
    private String symbIoTeCoreUrl;

    public DummyAAMRestListeners() {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }

    @RequestMapping(method = RequestMethod.GET, path = AAMConstants.AAM_GET_CA_CERTIFICATE)
    public String getRootCertificate() throws NoSuchProviderException, KeyStoreException, IOException,
            UnrecoverableKeyException, NoSuchAlgorithmException, CertificateException {
        log.info("invoked get token public");
        final String ALIAS = "test aam keystore";
        KeyStore ks = KeyStore.getInstance("PKCS12", "BC");
        ks.load(new FileInputStream("./src/test/resources/TestAAM.keystore"), "1234567".toCharArray());
        X509Certificate x509Certificate = (X509Certificate) ks.getCertificate("test aam keystore");
        StringWriter signedCertificatePEMDataStringWriter = new StringWriter();
        JcaPEMWriter pemWriter = new JcaPEMWriter(signedCertificatePEMDataStringWriter);
        pemWriter.writeObject(x509Certificate);
        pemWriter.close();
        return signedCertificatePEMDataStringWriter.toString();
    }

    @RequestMapping(method = RequestMethod.POST, path = AAMConstants.AAM_LOGIN, produces =
            "application/json", consumes = "application/json")
    public ResponseEntity<?> doLogin(@RequestBody Credentials credential) {
        log.info("User trying to login " + credential.getUsername() + " - " + credential.getPassword());
        try {
            final String ALIAS = "test aam keystore";
            KeyStore ks = KeyStore.getInstance("PKCS12", "BC");
            ks.load(new FileInputStream("./src/test/resources/TestAAM.keystore"), "1234567".toCharArray());
            Key key = ks.getKey(ALIAS, "1234567".toCharArray());

            HashMap<String, String> attributes = new HashMap<>();
            attributes.put("name", "test2");
            String tokenString = JWTEngine.generateJWTToken(credential.getUsername(), attributes, ks.getCertificate
                    (ALIAS).getPublicKey().getEncoded(), IssuingAuthorityType.CORE, DateUtil.addDays(new Date(), 1)
                    .getTime(), "securityHandlerTestAAM", ks.getCertificate(ALIAS).getPublicKey(), (PrivateKey) key);

            Token coreToken = new Token(tokenString);

            HttpHeaders headers = new HttpHeaders();
            headers.add(AAMConstants.TOKEN_HEADER_NAME, coreToken.getToken());

            /* Finally issues and return foreign_token */
            return new ResponseEntity<>(headers, HttpStatus.OK);
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException |
                UnrecoverableKeyException | JWTCreationException | NoSuchProviderException | TokenValidationException
                e) {
            log.error(e);
        }
        return null;
    }


    @RequestMapping(method = RequestMethod.POST, path = AAMConstants.AAM_CHECK_HOME_TOKEN_REVOCATION,
            produces = "application/json;charset=UTF-8", consumes = "application/json;charset=UTF-8")
    public ResponseEntity<CheckRevocationResponse> checkTokenRevocation(@RequestHeader(AAMConstants
            .TOKEN_HEADER_NAME) String token) {
        log.info("Checking token revocation " + token);
        // todo implement... for the moment returns valid
        String status = "VALID";

        try {
            JWTClaims claims = JWTEngine.getClaimsFromToken(token);
            status = claims.getAtt().get("status");

            log.info("Status = " + status);
        } catch (MalformedJWTException e) {
            log.info(e); 
        }
        
        switch(status) {
            case "VALID_OFFLINE": {
                return new ResponseEntity<>(new CheckRevocationResponse
                        (ValidationStatus.VALID_OFFLINE), HttpStatus.OK);
            }
            case "EXPIRED": {
                return new ResponseEntity<>(new CheckRevocationResponse
                        (ValidationStatus.EXPIRED), HttpStatus.OK);
            }
            case "REVOKED": {
                return new ResponseEntity<>(new CheckRevocationResponse
                        (ValidationStatus.REVOKED), HttpStatus.OK);
            }
            case "INVALID": {
                return new ResponseEntity<>(new CheckRevocationResponse
                        (ValidationStatus.INVALID), HttpStatus.OK);
            }
            case "NULL": {
                return new ResponseEntity<>(new CheckRevocationResponse
                        (ValidationStatus.NULL), HttpStatus.OK);
            }
            default: {
                return new ResponseEntity<>(new CheckRevocationResponse
                        (ValidationStatus.VALID), HttpStatus.OK);
            }
        }
        // return new ResponseEntity<>(new CheckRevocationResponse
        //         (ValidationStatus.VALID), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.POST, path = AAMConstants.AAM_REQUEST_FOREIGN_TOKEN, produces =
            "application/json;charset=UTF-8", consumes = "application/json;charset=UTF-8")
    public ResponseEntity<?> requestForeignToken(@RequestHeader(AAMConstants.TOKEN_HEADER_NAME) String
                                                         homeTokenString) {
        log.info("Requesting foreign (core or platform) token, received home token " + homeTokenString);
        try {
            final String ALIAS = "test aam keystore";
            KeyStore ks = KeyStore.getInstance("PKCS12", "BC");
            ks.load(new FileInputStream("./src/test/resources/TestAAM.keystore"), "1234567".toCharArray());
            Key key = ks.getKey(ALIAS, "1234567".toCharArray());

            HashMap<String, String> attributes = new HashMap<>();
            attributes.put("fname1", "fvalue1");
            attributes.put("fname2", "fvalue2");
            attributes.put("fname3", "fvalue3");
            String tokenString = JWTEngine.generateJWTToken("foreign", attributes, ks.getCertificate(ALIAS)
                    .getPublicKey().getEncoded(), IssuingAuthorityType.CORE, DateUtil.addDays(new Date(), 1).getTime
                    (), "securityHandlerTestAAM", ks.getCertificate(ALIAS).getPublicKey(), (PrivateKey) key);

            Token foreignToken = new Token(tokenString);
            HttpHeaders headers = new HttpHeaders();
            headers.add(AAMConstants.TOKEN_HEADER_NAME, foreignToken.getToken());

            /* Finally issues and return foreign_token */
            return new ResponseEntity<>(headers, HttpStatus.OK);
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException |
                UnrecoverableKeyException | NoSuchProviderException | JWTCreationException | TokenValidationException
                e) {
            log.error(e);
        }
        return null;
    }

    @RequestMapping(value = AAMConstants.AAM_GET_AVAILABLE_AAMS, method = RequestMethod.GET, produces = "application/json")
    public ResponseEntity<List<AAM>> getAvailableAAMs() {
        
        log.info("Requesting available AAMs");
        
        List<AAM> availableAAMs = new ArrayList<>();

        try {
            // Core AAM
            Certificate coreCertificate = new Certificate("coreCertTestValue");

            // adding core aam info to the response
            availableAAMs.add(new AAM("http://localhost:8080", "Test Platform 1", "platform1", coreCertificate));
            availableAAMs.add(new AAM("http://localhost:8080", "Test Platform 2", "platform2", coreCertificate));
            availableAAMs.add(new AAM("http://localhost:8080", "Test Platform 3", "platform3", coreCertificate));
            availableAAMs.add(new AAM("http://localhost:8080", "Test Platform 4", "platform4", coreCertificate));
            availableAAMs.add(new AAM("http://localhost:8080", "Test Platform 5", "platform5", coreCertificate));

            return new ResponseEntity<>(availableAAMs, HttpStatus.OK);
        } catch (Exception e) {
            log.error(e);
            return new ResponseEntity<>(new ArrayList<AAM>(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(method = RequestMethod.GET, value = "/query")
    public ResponseEntity search(@RequestParam("location_name") String location ) {
        
        log.info("Search request");

        ObjectMapper mapper = new ObjectMapper();
        QueryResponse response = new QueryResponse();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        switch (location) {
            case "Paris": {
                ArrayList<QueryResourceResult> responseResources = new ArrayList<QueryResourceResult>();

                QueryResourceResult resource1 = new QueryResourceResult();
                resource1.setId("resource1");
                resource1.setPlatformId("platform1");
                responseResources.add(resource1);

                QueryResourceResult resource2 = new QueryResourceResult();
                resource2.setId("resource2");
                resource2.setPlatformId("platform2");
                responseResources.add(resource2);

                QueryResourceResult resource3 = new QueryResourceResult();
                resource3.setId("resource3");
                resource3.setPlatformId("platform3");
                responseResources.add(resource3);

                response.setResources(responseResources);

                try {
                    String responseInString = mapper.writeValueAsString(response);
                 
                    log.info("Server received request for sensors in Paris");
                    log.info("Server woke up and will answer with " + responseInString);
                } catch (JsonProcessingException e) {
                    log.info(e.toString());
                    return new ResponseEntity<>(headers, HttpStatus.BAD_REQUEST);
                }

                break;
            }
            case "Athens": {

                ArrayList<QueryResourceResult> responseResources = new ArrayList<QueryResourceResult>();

                QueryResourceResult resource4 = new QueryResourceResult();
                resource4.setId("resource4");
                resource4.setPlatformId("platform4");
                responseResources.add(resource4);

                QueryResourceResult resource5 = new QueryResourceResult();
                resource5.setId("resource5");
                resource5.setPlatformId("platform5");
                responseResources.add(resource5);

                response.setResources(responseResources);

                try {
                    String responseInString = mapper.writeValueAsString(response);
                 
                    log.info("Server received request for sensors in Athens");
                    log.info("Server woke up and will answer with " + responseInString);
                } catch (JsonProcessingException e) {
                    log.info(e.toString());
                    return new ResponseEntity<>(headers, HttpStatus.BAD_REQUEST);
                }

                break;
            }
            case "Zurich": {
                return new ResponseEntity<>(headers, HttpStatus.BAD_REQUEST);
            }
        }

        return new ResponseEntity<>(response, headers, HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/resourceUrls")
    public ResponseEntity getResourceUrls(@RequestParam("id") String resourceId, @RequestHeader("X-Auth-Token") String token) {
        
        log.info("Requesting resource url for resource with id: " + resourceId);

        HashMap<String, String> response = new HashMap<String, String>();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        if (token != null) {
            response.put(resourceId, symbIoTeCoreUrl + "/Sensors('" + resourceId + "')");
        } else {
            return new ResponseEntity<>("", headers, HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<>(response, headers, HttpStatus.OK);
    }

    static public class DateUtil
    {
        public static Date addDays(Date date, int days)
        {
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.DATE, days); //minus number would decrement the days
            return cal.getTime();
        }
    }
}

