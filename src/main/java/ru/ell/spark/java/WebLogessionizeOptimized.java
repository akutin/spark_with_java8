package ru.ell.spark.java;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * Restore sessions in web log
 */
public class WebLogessionizeOptimized {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: WebLogessionizeOptimized [timeout in seconds] [input path] [output path]");
            System.exit(1);
        }
        final long timeout = Long.valueOf(args[0]) * 1000;

        final SparkConf sparkConf = new SparkConf().setAppName("Java8 Web Log Optimized");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        final JavaRDD<String> log = sc.textFile(args[1]);
        final JavaPairRDD<String, List<Session>> byIp = log
            .map(WebLogFormat.LOG_PATTERN::matcher)
            .filter(Matcher::find)
            .mapToPair(m -> new Tuple2(
                        m.group(WebLogFormat.IP),
                        Lists.newArrayList(
                                new Session(
                                        WebLogFormat.time(m.group(WebLogFormat.TIME)),
                                        UrlKey.valueOf(WebLogFormat.path(m.group(WebLogFormat.URL)))
                                )
                        )
                )
            )
            .reduceByKey( merge(timeout));

        final JavaRDD<Tuple2<String,Session>> flatSessions = byIp.flatMap(
                    ip -> ip._2().stream().filter(s -> s.getUrls().size() > 1).map(s -> new Tuple2<>(ip._1(), s)).collect(Collectors.toList())
                );


        flatSessions
//              .sortBy( t -> t._2().getUrls().size(), false, 1) // by number of hits
                .sortBy( t -> t._2().getEnd() - t._2().getStart(), false, 1) // by length
                .map( t -> String.format("%s\t%TT\t%TT\t%d", t._1(), t._2().getStart(), t._2().getEnd(), t._2().getUrls().size()))
                .saveAsTextFile(args[2]);

        sc.stop();
    }

    /**
     * URL key as url.hashCode + url.reverse.hashCode
     */
    public static class UrlKey implements Serializable {
        private final int hashCode;
        private final int reverseHashCode;

        public UrlKey(int hashCode, int reverseHashCode) {
            this.hashCode = hashCode;
            this.reverseHashCode = reverseHashCode;
        }

        public static UrlKey valueOf(String path) {
            return new UrlKey( path.hashCode(), new StringBuilder(path).reverse().hashCode());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            UrlKey urlKey = (UrlKey) o;

            if (hashCode != urlKey.hashCode) return false;
            return reverseHashCode == urlKey.reverseHashCode;
        }

        @Override
        public int hashCode() {
            int result = hashCode;
            result = 31 * result + reverseHashCode;
            return result;
        }
    }

    public static Function2<List<Session>, List<Session>, List<Session>> merge(long timeout) {
        return (session1,session2) -> {
            final List<Session> output = new LinkedList<>();
            final List<Session> sessions = new ArrayList<>(session1.size() + session2.size());
            sessions.addAll(session1);
            sessions.addAll(session2);
            Collections.sort(sessions, (o1, o2) -> o1.getStart().compareTo(o2.getStart()));

            Session previous = sessions.get(0);
            for (final Session next : sessions.subList(1, sessions.size())) {
                if (next.getStart() - previous.getEnd() > timeout) {
                    // expired
                    output.add(previous);
                    previous = next;
                } else {
                    // merge with ongoing
                    previous = Session.merge(previous, next);
                }
            }
            output.add(previous);
            return output;
        };
    }

    public static class Session implements Serializable {
        private final Long start;
        private final Long end;
        private final Set<UrlKey> urls;

        public Session(Long start, UrlKey startUrl) {
            this( start, start, startUrl);
        }

        public Session(Long start, Long end, UrlKey startUrl) {
            this.start = start;
            this.end = end;
            this.urls = new HashSet<>();
            urls.add(startUrl);
        }

        public Session(Long start, Long end, Set<UrlKey> urls) {
            this.start = start;
            this.end = end;
            this.urls = urls;
        }

        public Long getStart() {
            return start;
        }

        public Long getEnd() {
            return end;
        }

        public Set<UrlKey> getUrls() {
            return urls;
        }

        public static Session merge(Session s1, Session s2) {
            final Set<UrlKey> merged = new HashSet<>(s1.getUrls());
            merged.addAll(s2.getUrls());
            return new Session( s1.getStart(), s2.getEnd(), merged);
        }

        public String toString() {
            return String.format("%TT-%TT,hits=%d", start, end, urls.size());
        }
    }
}
