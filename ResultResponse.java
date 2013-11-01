package com.a.identity.search;
/**
 * Created with IntelliJ IDEA.
 * User: aminerounak
 * Date: 9/24/13
 * Time: 1:23 PM
 * To change this template use File | Settings | File Templates.
 */

import com.google.api.client.util.Key;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ResultResponse {

    private final static Logger LOG = LoggerFactory.getLogger(ResultResponse.class);


    @Key
    private Boolean ok = false;

    public Boolean getOk() {
        return ok;
    }

    public void setOk(Boolean ok) {

        this.ok = ok == null ? false : ok;
    }
}
