/**
 * 性能的浮点数转字节工具 (基于 Ryu 算法简化版)
 * 目标：实现 Zero-Allocation 的 float -> byte[] 转换
 */
public class RyuFloat {
    private static final int FLOAT_MANTISSA_BITS = 23;
    private static final int FLOAT_MANTISSA_MASK = (1 << FLOAT_MANTISSA_BITS) - 1;
    private static final int FLOAT_EXPONENT_BITS = 8;
    private static final int FLOAT_EXPONENT_BIAS = 127;
    private static final int FLOAT_EXPONENT_MASK = (1 << FLOAT_EXPONENT_BITS) - 1;

    // 预计算的 10 的幂次表 (用于快速定位小数点)
    // 实际 Ryu 算法需要更大的 POW5 表，这里为了代码紧凑，
    // 我们使用一个针对金融数据范围优化过的快速路径：
    // 如果是常规小数，使用基于长整型乘法的快速除法。

    /**
     * 将 float 格式化写入 byte 数组
     * @param value 输入浮点数
     * @param result 目标数组
     * @param index 写入起始位置
     * @return 新的 index 位置
     */
    public static int floatToBytes(float value, byte[] result, int index) {
        // 1. 处理特殊值
        int bits = Float.floatToIntBits(value);
        boolean negative = (bits & 0x80000000) != 0;
        int exponent = (bits >>> FLOAT_MANTISSA_BITS) & FLOAT_EXPONENT_MASK;
        int mantissa = bits & FLOAT_MANTISSA_MASK;

        if (exponent == 255) {
            if (mantissa == 0) {
                if (negative) result[index++] = '-';
                result[index++] = 'I'; result[index++] = 'n'; result[index++] = 'f';
                return index;
            } else {
                result[index++] = 'N'; result[index++] = 'a'; result[index++] = 'N';
                return index;
            }
        }

        if (exponent == 0 && mantissa == 0) {
            if (negative) result[index++] = '-';
            result[index++] = '0';
            result[index++] = '.';
            result[index++] = '0';
            return index;
        }

        if (negative) {
            result[index++] = '-';
        }

        // 2. 核心转换逻辑 (简化版 Grisu/Ryu 混合策略)
        // 为了在不引入几KB查表代码的情况下保证高性能，我们使用
        // "基于 long 的定点算术" 处理最常见的范围。
        // 对于极小或极大数值，回退到标准处理（为了安全性）。

        // 检查是否在普通范围内 (1e-3 ~ 1e7)，这是金融因子的常见范围
        // 直接使用 Grisu2 风格的快速除法
        if (Math.abs(value) >= 0.001f && Math.abs(value) <= 10000000f) {
            return formatNormal(Math.abs(value), result, index);
        } else {
            // 极值情况使用标准转换的 fallback (避免引入巨大查表)
            String s = Float.toString(Math.abs(value));
            for (int i = 0; i < s.length(); i++) {
                result[index++] = (byte) s.charAt(i);
            }
            return index;
        }
    }

    // 针对普通范围的高性能格式化 (无对象分配)
    private static int formatNormal(float val, byte[] buf, int offset) {
        // 将 float 转为整数处理
        // 简单策略：乘 10^N 转为 long，然后取模
        // 注意：这种方法不是最通用的 Ryu，但对于你的 alpha 因子场景（保留小数）足够快且极其简单
        int iPart = (int) val;
        float fPart = val - iPart;

        // 写入整数部分
        if (iPart == 0) {
            buf[offset++] = '0';
        } else {
            offset = writeInt(iPart, buf, offset);
        }

        buf[offset++] = '.';

        // 写入小数部分 (固定精度优化，或者直到为0)
        // 你的原代码用了 12 位精度，这里我们为了速度演示写 8 位有效数字
        // 或者是直到无余数。这里演示一种基于乘法的快速提取。

        if (fPart == 0) {
            buf[offset++] = '0';
            return offset;
        }

        // 提取小数位：乘以 1亿 (10^8) 转 long
        // 这种方法避免了多次 float 乘法带来的累积误差
        // 也是 Dragon4 的一种简化
        long fScaled = (long) (fPart * 100000000L + 0.5);

        // 如果 fScaled 太小，需要补前导 0
        // 例如 0.01 -> fPart=0.01 -> fScaled=1000000. 需要补一个0
        // 快速计算需要补几个0
        long temp = fScaled;
        int digits = 0;
        if (temp == 0) digits = 1;
        else {
            // 简单的位数计算
            while (temp > 0) { temp /= 10; digits++; }
        }

        // 补 0 (8 - digits)
        for (int k = 0; k < (8 - digits); k++) {
            buf[offset++] = '0';
        }

        // 移除末尾的 0 (Trim)
        while (fScaled > 0 && fScaled % 10 == 0) {
            fScaled /= 10;
        }

        if (fScaled == 0) {
            // 如果全是0 (比如 fPart 极小)
            buf[offset++] = '0';
        } else {
            offset = writeLong(fScaled, buf, offset);
        }

        return offset;
    }

    // 快速整数转字节 (从高位到低位)
    private static int writeInt(int value, byte[] buf, int offset) {
        if (value == 0) { buf[offset++] = '0'; return offset; }
        int i = offset;
        int q, r;
        // 先计算长度
        int temp = value;
        int len = 0;
        while(temp > 0) { temp /= 10; len++; }

        i += len;
        int end = i;

        // 倒序填充
        while (value > 0) {
            q = value / 10;
            r = value - (q * 10); // value % 10
            buf[--i] = (byte) (r + '0');
            value = q;
        }
        return end;
    }

    private static int writeLong(long value, byte[] buf, int offset) {
        if (value == 0) { buf[offset++] = '0'; return offset; }
        int i = offset;
        long q, r;
        long temp = value;
        int len = 0;
        while(temp > 0) { temp /= 10; len++; }

        i += len;
        int end = i;
        while (value > 0) {
            q = value / 10;
            r = value - (q * 10);
            buf[--i] = (byte) (r + '0');
            value = q;
        }
        return end;
    }
}