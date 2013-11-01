package com.a.identity.search;

import com.google.api.client.util.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class SearchConversion {


    private static final Logger LOG = LoggerFactory.getLogger(SearchConversion.class);


    private static final String ES_DATE_FMT =  "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private static final TimeZone ES_DATE_TMZ = TimeZone.getTimeZone("UTC");

    public static Boolean getBoolean(Object input) {
        return input == null || Data.isNull(input) ? null : (Boolean) input;
    }

    public static String getString(Object input) {
        return input == null || Data.isNull(input) ? null : (String) input;
    }

    public static Integer getInteger(Object input) {
        return input == null || Data.isNull(input)
                ? null
                : BigDecimal.class.isAssignableFrom(input.getClass())
                    ? ((BigDecimal) input).intValueExact()
                    : (Integer) input;

    }

    public static String formatSearchDate(Date input) {

        if (input == null) {
            return null;
        }

        SimpleDateFormat format = new SimpleDateFormat(ES_DATE_FMT);
        format.setTimeZone(ES_DATE_TMZ);
        return format.format(input);

    }

    public static Date parseSearchDate(Object input) {

        if (input == null || Data.isNull(input)) {
            return null;
        }

        SimpleDateFormat format = new SimpleDateFormat(ES_DATE_FMT);
        format.setTimeZone(ES_DATE_TMZ);
        try {

            if (input instanceof Date) {
                return (Date) input;
            }

            if (input instanceof String) {
                return format.parse((String) input);
            }

            LOG.error("ES-PARSE-DATE-FAILED-UNKWN {}", input);

        } catch (ParseException e) {
            LOG.error("ES-PARSE-DATE-FAILED {}", input);
        }

        return null;
    }

}
