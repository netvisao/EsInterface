package c.a.i.search;
/**
 * Created with IntelliJ IDEA.
 * User: aminerounak
 * Date: 9/23/13
 * Time: 5:53 PM
 * To change this template use File | Settings | File Templates.
 */

import com.google.api.client.util.Key;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CountResponse {

    private final static Logger LOG = LoggerFactory.getLogger(CountResponse.class);

    public CountResponse() {
    }

    @Key
    private int count;

    public long getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
