package eu.h2020.symbiote.enabler.resourcemanager.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.h2020.symbiote.enabler.messaging.model.CancelTaskResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.fail;

/**
 * Created by vasgl on 7/26/2017.
 */
public class ListenableFutureCancelCallback implements ListenableFutureCallback<CancelTaskResponse> {

    private static Log log = LogFactory
            .getLog(ListenableFutureCancelCallback.class);

    private AtomicReference<CancelTaskResponse> resultRef;
    private String test;
    private ObjectMapper mapper = new ObjectMapper();

    public ListenableFutureCancelCallback(String test, AtomicReference<CancelTaskResponse> resultRef) {
        this.test = test;
        this.resultRef = resultRef;
        mapper = new ObjectMapper();
    }

    public void onSuccess(CancelTaskResponse result) {
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