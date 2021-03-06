package eu.h2020.symbiote.enabler.resourcemanager.integration.callbacks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.enabler.messaging.model.ResourceManagerUpdateResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.fail;

/**
 * Created by vasgl on 7/26/2017.
 */
public class ListenableFutureUpdateCallback implements ListenableFutureCallback<ResourceManagerUpdateResponse> {

    private static Log log = LogFactory
            .getLog(ListenableFutureUpdateCallback.class);

    private AtomicReference<ResourceManagerUpdateResponse> resultRef;
    private String test;
    private ObjectMapper mapper = new ObjectMapper();

    public ListenableFutureUpdateCallback(String test, AtomicReference<ResourceManagerUpdateResponse> resultRef) {
        this.test = test;
        this.resultRef = resultRef;
        mapper = new ObjectMapper();
    }

    public void onSuccess(ResourceManagerUpdateResponse result) {
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