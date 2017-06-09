/**
 * Created by yuwang on 6/8/17.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

/*
* example result in one window
*
* startTime= Wed Jul 22 02:40:16 EDT 2015
  endTime= Wed Jul 22 02:55:16 EDT 2015
  average session time= 8
  Total unique URL visits per session: 9612
  The most engaged users: [(119.81.61.166,2712)]
*
*
* */


public class Analyzer {
    private static Function2<Long, Long, Long> SUM_REDUCER = (a, b) -> a + b;
    // set 15 min as the interval
    private static final int MINUTE_INTERVAL = 15;
    private static final String LOG_FILE = "2015_07_22_mktplace_shop_web_log_sample.log";

    private static class ValueComparator<K, V>
            implements Comparator<Tuple2<K, V>>, Serializable {
        private Comparator<V> comparator;

        public ValueComparator(Comparator<V> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
            return comparator.compare(o1._2(), o2._2());
        }
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("WebLog Analyzer").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logLines = sc.textFile(LOG_FILE);

        // load the entire log from the file to RDD
        JavaRDD<ELBAccessLog> accessLogs = logLines.map(ELBAccessLog::parseFromLogLine).filter(log -> log != null).cache();

        // find the time range in the entire log
        Date startTime = accessLogs.map(ELBAccessLog::getDateTimeString).min(Comparator.naturalOrder());
        Date lastTime = accessLogs.map(ELBAccessLog::getDateTimeString).max(Comparator.naturalOrder());


        // slide the 15 min moving window towards the end
        while (startTime.before(lastTime)) {
            Date endTime = new Date();
            endTime.setTime(startTime.getTime() + MINUTE_INTERVAL * 60 * 1000);

            // get the session tuples per each IP
            List<Tuple2<String, Long>> sessionsCount = accessLogs.filter(log -> log.getDateTimeString().after(startTime) && log.getDateTimeString().before(endTime))
                    .mapToPair(log -> new Tuple2<>(log.getClientIpAddress(), 1L))
                    .reduceByKey(SUM_REDUCER)
                    .collect();

            long totalHit = sessionsCount.stream().map(tup -> tup._2()).reduce((a, b) -> a + b).orElse(0L);
            long totalIp = sessionsCount.stream().map(tup -> tup._1()).distinct().count();
            // if totalIp gets 0, assign it as 1L, to avoid zero divide exception
            if (totalIp == 0L) totalIp = 1L;

            // get total distinct URL, store in a List
            List<String> totalDitinctUrl = accessLogs.filter(log -> log.getDateTimeString().after(startTime) && log.getDateTimeString().before(endTime))
                    .map(ELBAccessLog::getUserAgent)
                    .distinct().collect();

            // get the highest IP, store in a List of size == 1
            List<Tuple2<String, Long>> ipAddressCount =
                    accessLogs.filter(log -> log.getDateTimeString().after(startTime) && log.getDateTimeString().before(endTime))
                            .mapToPair(log -> new Tuple2<>(log.getClientIpAddress(), 1L))
                            .reduceByKey(SUM_REDUCER)
                            .top(1, new ValueComparator<>(Comparator.<Long>naturalOrder()));

            System.out.println("startTime= " + startTime.toString());
            System.out.println("endTime= " + endTime.toString());
            System.out.println("average session time= " + totalHit / totalIp);
            System.out.println(String.format("Total unique URL visits per session: %s", totalDitinctUrl.size()));
            System.out.println(String.format("The most engaged users: %s", ipAddressCount));

            startTime.setTime(endTime.getTime());

        }

        sc.stop();
    }
}