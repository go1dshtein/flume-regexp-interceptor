package org.apache.hadoop.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


public class HeaderRegexInterceptor implements Interceptor {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(HeaderRegexInterceptor.class);

    private String header;
    private Pattern pattern;
    private Map<String, String> groupToStringMap;

    public HeaderRegexInterceptor(String header, String regex,
		    		  Map<String, String> groupToStringMap) {
        this.header = header;
	this.pattern = Pattern.compile(regex);
	this.groupToStringMap = groupToStringMap;
    }

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
	String headerValue = headers.get(header);
	if (headerValue == null) {
	    LOGGER.debug("Could not find header '{}'", header);
	    return event;
	}

	Matcher matcher = pattern.matcher(headerValue);
	if (!matcher.matches()) {
	    LOGGER.debug("There are no groups: {}", headerValue);
	    return event;
	}

	for (Map.Entry<String,String> entry: groupToStringMap.entrySet()) {
	    try {
		String value = matcher.group(entry.getKey());
		LOGGER.debug("Header {} => {}", entry.getValue(), value);
		headers.put(entry.getValue(), value);
	    }
	    catch (IllegalArgumentException e) {
		LOGGER.warn("Could not find group '{}'", entry.getKey());
		continue;
	    }
	}

        event.setHeaders(headers);
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        List<Event> interceptedEvents =
                new ArrayList<Event>(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
       	    interceptedEvents.add(interceptedEvent);
        }

        return interceptedEvents;
    }

    @Override
    public void close() {
    }

    public static class Builder
            implements Interceptor.Builder {

	public static final String HEADER_KEY = "header";
	public static final String REGEX_KEY = "regex";
	public static final String SUB_GROUP_KEY = "group.";

        private String header;
	private String regex;
	private Map<String,String> groupToStringMap;

        @Override
        public void configure(Context context) {
            header = context.getString(HEADER_KEY);
            regex = context.getString(REGEX_KEY);
	    groupToStringMap = context.getSubProperties(SUB_GROUP_KEY);
        }

        @Override
        public Interceptor build() {
            return new HeaderRegexInterceptor(header, regex, groupToStringMap);
        }
    }
}
