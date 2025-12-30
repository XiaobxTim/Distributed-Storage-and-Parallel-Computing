import java.util.Calendar;

/**
 * 时间编码工具类 (26位压缩版)
 * 1. 日期：使用相对 Epoch 的天数 (12位，覆盖约11年)
 * 2. 时间：仅编码有效交易时段 (14位，剔除中午休市和非交易时间)
 * 结构：[未用(6位)] [日期偏移(12位)] [时间索引(14位)]
 */
public class CompactTimeUtil {

    // ---------------- 配置参数 ----------------
    // 基准年份，可根据数据实际年份调整，12位日期支持从此年份开始的 +11 年
    private static final int BASE_YEAR = 2014;

    // 位移常量
    private static final int DATE_SHIFT = 14;
    private static final int TIME_MASK = 0x3FFF; // 14位掩码 (16383)
    private static final int DATE_MASK = 0xFFF;  // 12位掩码 (4095)

    // 交易时段秒数定义 (用于时间映射)
    private static final int AM_START_SEC = 9 * 3600;        // 09:00:00
    private static final int AM_END_SEC = 11 * 3600 + 30 * 60; // 11:30:00
    private static final int PM_START_SEC = 13 * 3600;       // 13:00:00
    private static final int PM_END_SEC = 15 * 3600;         // 15:00:00

    private static final int AM_DURATION = AM_END_SEC - AM_START_SEC; // 9000s

    /**
     * 编码为 26 位整数
     */
    public static int encode(int tradingDay, int tradeTime) {
        // 1. 计算日期偏移 (稀疏映射，性能极高)
        int year = tradingDay / 10000;
        int month = (tradingDay % 10000) / 100;
        int day = tradingDay % 100;

        int yearOffset = year - BASE_YEAR;
        if (yearOffset < 0 || yearOffset > 11) {
            // 12位日期极限是 11年 (4096/372 ≈ 11)
            throw new IllegalArgumentException("年份超出范围，当前基准支持 " + BASE_YEAR + "-" + (BASE_YEAR + 11));
        }

        // 使用 "年*372 + 月*31 + 日" 的稀疏映射，避免复杂的日历计算
        // 372 = 12 * 31，保证月份和日期可逆且单调
        int dateCode = yearOffset * 372 + (month - 1) * 31 + (day - 1);
        if (dateCode > DATE_MASK) {
            throw new IllegalArgumentException("日期超出编码范围");
        }

        // 2. 计算时间索引 (HHMMSS -> 秒 -> 映射索引)
        int totalSeconds = convertToSecOfDay(tradeTime);
        int timeCode = mapSecondsToCode(totalSeconds);

        // 3. 组合
        return (dateCode << DATE_SHIFT) | timeCode;
    }

    /**
     * 将 HHMMSS 转换为当天的绝对秒数
     */
    public static int convertToSecOfDay(int time) {
        int hour = time / 10000;
        int minute = (time % 10000) / 100;
        int second = time % 100;
        return hour * 3600 + minute * 60 + second;
    }

    /**
     * 核心压缩逻辑：将绝对秒数映射为连续的紧凑索引
     * 跳过 11:30-13:00 的午休时间
     */
    private static int mapSecondsToCode(int sec) {
        if (sec < AM_START_SEC) return 0; // 开盘前统一映射为0

        if (sec <= AM_END_SEC) {
            return sec - AM_START_SEC; // 早盘直接映射
        } else if (sec < PM_START_SEC) {
            return AM_DURATION; // 午休期间统一映射为早盘结束点
        } else if (sec <= PM_END_SEC) {
            return AM_DURATION + (sec - PM_START_SEC) + 1; // 午盘接续在早盘后
        } else {
            // 收盘后，取最大值 (约16200)
            return AM_DURATION + (PM_END_SEC - PM_START_SEC) + 1;
        }
    }

    /**
     * 解码日期
     */
    public static int decodeTradingDay(int compactTime) {
        int dateCode = (compactTime >>> DATE_SHIFT) & DATE_MASK;

        int yearOffset = dateCode / 372;
        int remainder = dateCode % 372;
        int monthIndex = remainder / 31;
        int dayIndex = remainder % 31;

        int year = yearOffset + BASE_YEAR;
        int month = monthIndex + 1;
        int day = dayIndex + 1;

        return year * 10000 + month * 100 + day;
    }

    /**
     * 解码时间 (还原为 HHMMSS)
     */
    public static int decodeTradeTime(int compactTime) {
        int timeCode = compactTime & TIME_MASK;
        int secOfDay;

        if (timeCode <= AM_DURATION) {
            secOfDay = AM_START_SEC + timeCode;
        } else {
            secOfDay = PM_START_SEC + (timeCode - AM_DURATION - 1);
        }

        int hour = secOfDay / 3600;
        int remaining = secOfDay % 3600;
        int minute = remaining / 60;
        int second = remaining % 60;

        return hour * 10000 + minute * 100 + second;
    }

    // 兼容原有的辅助方法
    public static int getDayCode(int compactTime) {
        return compactTime >>> DATE_SHIFT;
    }

    public static String getMMDD(int compactTime) {
        int dateCode = (compactTime >>> DATE_SHIFT) & DATE_MASK;
        int remainder = dateCode % 372;
        int month = remainder / 31 + 1;
        int day = remainder % 31 + 1;
        return String.format("%02d%02d", month, day);
    }

    // 保持比较逻辑不变
    public static int compare(int a, int b) {
        return Integer.compareUnsigned(a, b);
    }
}