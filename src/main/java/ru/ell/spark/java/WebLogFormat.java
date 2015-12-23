package ru.ell.spark.java;

import org.joda.time.DateTime;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;

/**
 * Utility class for web log analyzer
 */
public class WebLogFormat {
    public final static Pattern LOG_PATTERN = Pattern.compile("(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d+)Z\\s\\S+\\s(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}):.+\\s\"(?:[A-Z]+)\\s(\\S+)\\sHTTP/");
    public final static int TIME = 1;
    public final static int IP = 2;
    public final static int URL = 3;

    public static Long time(String time) {
        return new DateTime(time).toDate().getTime();
    }

    public static String path(String url) throws MalformedURLException {
        return new URL(url).getPath();
    }
}
