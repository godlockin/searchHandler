package searchhandler.common.utils;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;

public class DateUtils {

    public static final String DATE_YYYY_MM_DD = "yyyy-MM-dd";
    public static final String DATE_YYYYMMDD = "yyyyMMdd";
    public static final String DATE_YYYYMMDD_HHMMSS = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_DATE_FORMAT = DATE_YYYYMMDD_HHMMSS;

    public static String formatDate(Date date) {

        return formatDate(date, DEFAULT_DATE_FORMAT);
    }

    public static String formatDate(Date date, String format) {
        if (null == date) {
            return null;
        }

        SimpleDateFormat sf = new SimpleDateFormat(format);
        return sf.format(date);
    }

    public static Date getDateDiff(String num, String timeUnit) {
        return getDateDiff(new Date(), num, timeUnit);
    }

    public static Date getDateDiff(Date base, String num, String timeUnit) {
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDateTime localDateTime = base.toInstant().atZone(zoneId).toLocalDateTime();
        localDateTime = localDateTime.plus(Long.valueOf(num), ChronoUnit.valueOf(timeUnit));
        return Date.from(localDateTime.atZone(zoneId).toInstant());
    }
}
