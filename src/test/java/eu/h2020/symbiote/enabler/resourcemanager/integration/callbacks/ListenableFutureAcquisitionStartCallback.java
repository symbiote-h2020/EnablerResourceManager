package eu.h2020.symbiote.enabler.resourcemanager.integration.callbacks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerAcquisitionStartResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.fail;

/**
 * Created by vasgl on 7/26/2017.
 */
public class ListenableFutureAcquisitionStartCallback implements ListenableFutureCallback<ResourceManagerAcquisitionStartResponse> {

    private static Log log = LogFactory
            .getLog(ListenableFutureAcquisitionStartCallback.class);

    private AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef;
    private String test;
    private ObjectMapper mapper = new ObjectMapper();

    public ListenableFutureAcquisitionStartCallback(String test, AtomicReference<ResourceManagerAcquisitionStartResponse> resultRef) {
        this.test = test;
        this.resultRef = resultRef;
        mapper = new ObjectMapper();
    }

    public void onSuccess(ResourceManagerAcquisitionStartResponse result) {
        try {
            log.info(test + ": Successfully received response = " + mapper.writeValueAsString(result));
        } catch (JsonProcessingException e) {
            log.info(e.toString());
        }
        resultRef.set(result);

    }

    public void onFailure(Throwable ex) {
        fail("Accessed the element which does not exist");
    }
}