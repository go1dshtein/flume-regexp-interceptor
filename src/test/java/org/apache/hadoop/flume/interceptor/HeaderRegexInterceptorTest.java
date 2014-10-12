package org.apache.hadoop.flume.interceptor;

import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;


@RunWith(JUnit4.class)
public class HeaderRegexInterceptorTest {

    private static final String BODY = "this is my body";
    private static final String HEADER_KEY = "filename";
    private static final String HEADER_VALUE = "testfile-on-12.10.2014.txt";
    private static final String REGEX_PATTERN =
	"^(?<basename>\\w+)-on-(?<date>\\d+\\.\\d+\\.\\d+)\\.txt";
    private Map<String,String> groupToStringMap;

    private Event event;
    private Map<String, String> headers;
    private HeaderRegexInterceptor interceptor;

    @Before
    public void prepare() {

	groupToStringMap = new HashMap<String, String>();
	groupToStringMap.put("basename", "basename");
	groupToStringMap.put("date", "timestamp");

        headers = new HashMap<String, String>(1);
        headers.put(HEADER_KEY, HEADER_VALUE);

        event = new JSONEvent();
        event.setBody(BODY.getBytes());
        event.setHeaders(headers);

        interceptor = new HeaderRegexInterceptor(
	    HEADER_KEY, REGEX_PATTERN, groupToStringMap);
        interceptor.initialize();
    }

    @Test
    public void testInterception(){

        Event interceptedEvent =
                interceptor.intercept(event);

        assertEquals("event body should not have been altered",
                BODY,
                new String(interceptedEvent.getBody()));

	assertEquals("event filename header should not have been altered",
                HEADER_VALUE,
                interceptedEvent.getHeaders().get(HEADER_KEY));

        assertEquals("header should now contain basename",
		"testfile",
                interceptedEvent.getHeaders().get("basename"));

        assertEquals("header should now contain timestamp",
		"12.10.2014",
                interceptedEvent.getHeaders().get("timestamp"));
    }

}
