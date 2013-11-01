package c.a.i.search;
/**
 * Created with IntelliJ IDEA.
 * User: aminerounak
 * Date: 10/2/13
 * Time: 1:15 PM
 * To change this template use File | Settings | File Templates.
 */

import com.google.api.client.util.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class Hits {

    private final static Logger LOG = LoggerFactory.getLogger(Hits.class);


    public static class Result {
        @Key("_index")
        private String index;
        @Key("_type")
        private String type;
        @Key("_id")
        private String id;
        @Key("_score")
        private float score;
        @Key("_source")
        private Map<String, ?> source;

        public Result() {};

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public float getScore() {
            return score;
        }

        public void setScore(float score) {
            this.score = score;
        }

        public Map<String, ?> getSource() {
            return source;
        }

        public void setSource(Map<String, ?> source) {
            this.source = source;
        }
    }

    @Key("hits")
    private List<Result> results;

    @Key
    private int total;

    @Key("max_score")
    private Float maxScore;

    public Hits() {};

    public List<Result> getResults() {
        return results;
    }

    public void setResults(List<Result> results) {
        this.results = results;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public Float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(Float maxScore) {
        this.maxScore = maxScore;
    }
}
