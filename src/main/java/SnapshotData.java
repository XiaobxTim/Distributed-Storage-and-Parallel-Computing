import java.util.Arrays;

/**
 * 快照数据容器
 * 1. Structure Peeling: 将数组炸开为独立字段，消除数组边界检查和双重寻址
 * 2. Manual Loop Unrolling: 解析逻辑完全手动展开
 */
public class SnapshotData {
    public int tradingDay;
    public long tradeTime;
    public int code;

    // 总量数据
    public long tBidVol;
    public long tAskVol;

    // --- 结构炸开 (仅保留前5档用于因子计算，提升缓存局部性) ---
    // Bid Prices
    public long bp0, bp1, bp2, bp3, bp4;
    // Bid Volumes
    public long bv0, bv1, bv2, bv3, bv4;
    // Ask Prices
    public long ap0, ap1, ap2, ap3, ap4;
    // Ask Volumes
    public long av0, av1, av2, av3, av4;

    private final int[] cursor = new int[1];

    public void reset() {
        tradingDay = 0;
        tradeTime = 0;
        code = 0;
        tBidVol = 0;
        tAskVol = 0;
        // 基本类型字段不需要显式归零，覆盖写入即可
    }

    public void copyFrom(SnapshotData s) {
        this.tradingDay = s.tradingDay;
        this.tradeTime = s.tradeTime;
        this.code = s.code;
        this.tBidVol = s.tBidVol;
        this.tAskVol = s.tAskVol;

        this.bp0 = s.bp0; this.bp1 = s.bp1; this.bp2 = s.bp2; this.bp3 = s.bp3; this.bp4 = s.bp4;
        this.bv0 = s.bv0; this.bv1 = s.bv1; this.bv2 = s.bv2; this.bv3 = s.bv3; this.bv4 = s.bv4;
        this.ap0 = s.ap0; this.ap1 = s.ap1; this.ap2 = s.ap2; this.ap3 = s.ap3; this.ap4 = s.ap4;
        this.av0 = s.av0; this.av1 = s.av1; this.av2 = s.av2; this.av3 = s.av3; this.av4 = s.av4;
    }

    public boolean parseFromBytes(byte[] data, int offset, int length) {
        cursor[0] = offset;
        final int maxLen = data.length;
        if (offset + length > maxLen) return false;

        try {
            // Index 0: TradingDay
            this.tradingDay = FastParser.parseDate8(data, cursor[0]);
            cursor[0] += 9;

            // Index 1: TradeTime
            this.tradeTime = FastParser.parseLong(data, cursor);

            // Index 2, 3: Skip recvTime, MIC
            FastParser.skipFields(data, cursor, 2);

            // Index 4: Code
            this.code = FastParser.parseStockCodeToInt(data, cursor);

            // Index 5-11: Skip
            FastParser.skipFields(data, cursor, 7);

            // Index 12, 13
            this.tBidVol = FastParser.parseLong(data, cursor);
            this.tAskVol = FastParser.parseLong(data, cursor);

            // Index 14-16: Skip
            FastParser.skipFields(data, cursor, 3);

            // --- 5. 解析前5档 (手动展开) ---
            // Level 1 (Index 17-20)
            bp0 = FastParser.parseLong(data, cursor);
            bv0 = FastParser.parseLong(data, cursor);
            ap0 = FastParser.parseLong(data, cursor);
            av0 = FastParser.parseLong(data, cursor);

            // Level 2
            bp1 = FastParser.parseLong(data, cursor);
            bv1 = FastParser.parseLong(data, cursor);
            ap1 = FastParser.parseLong(data, cursor);
            av1 = FastParser.parseLong(data, cursor);

            // Level 3
            bp2 = FastParser.parseLong(data, cursor);
            bv2 = FastParser.parseLong(data, cursor);
            ap2 = FastParser.parseLong(data, cursor);
            av2 = FastParser.parseLong(data, cursor);

            // Level 4
            bp3 = FastParser.parseLong(data, cursor);
            bv3 = FastParser.parseLong(data, cursor);
            ap3 = FastParser.parseLong(data, cursor);
            av3 = FastParser.parseLong(data, cursor);

            // Level 5
            bp4 = FastParser.parseLong(data, cursor);
            bv4 = FastParser.parseLong(data, cursor);
            ap4 = FastParser.parseLong(data, cursor);
            av4 = FastParser.parseLong(data, cursor);

            return true;
        } catch (Exception e) {
            return false;
        }
    }
}