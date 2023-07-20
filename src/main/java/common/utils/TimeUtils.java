package common.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TimeUtils {

    private static final DateTimeFormatter tsFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public static String formatTs(Long ts) {
        if (ts < Integer.MIN_VALUE) {
            return "No Watermark";
        }
        if (ts < 0) {
            System.out.println(ts);
            return "1970-01-01 08:00:00.000";
        }
        Instant instant = Instant.ofEpochMilli(ts);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        return localDateTime.format(tsFormatter);
    }
}
