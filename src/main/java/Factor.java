import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Factor implements Writable {
    private float[] factorValues = new float[20];
    private int count;
    private static final float EPSILON = 1e-7f;

    public Factor() {}

    public void copyFrom(Factor other) {
        System.arraycopy(other.factorValues, 0, this.factorValues, 0, 20);
        this.count = other.count;
    }

    /**
     * 1. 访问扁平化字段 (s.bp0)
     * 2. 除法转乘法 (Reciprocal)
     */
    public void calculateFrom(SnapshotData s, SnapshotData p) {
        // --- 1. 基础聚合 (寄存器级变量) ---
        long sumBidVol = s.bv0 + s.bv1 + s.bv2 + s.bv3 + s.bv4;
        long sumAskVol = s.av0 + s.av1 + s.av2 + s.av3 + s.av4;
        long totalVol = sumBidVol + sumAskVol;
        long totalTVol = s.tBidVol + s.tAskVol;

        // --- 2. 预计算倒数 (除法 -> 乘法) ---
        // CPU 做 FDIV 约需 20-40 周期，FMUL 仅需 3-5 周期
        float invSumBid = 1.0f / (sumBidVol + EPSILON);
        float invSumAsk = 1.0f / (sumAskVol + EPSILON);
        float invTotalVol = 1.0f / (totalVol + EPSILON);
        float invTotalTVol = 1.0f / (totalTVol + EPSILON);

        double wBidPrice = s.bp0*s.bv0 + s.bp1*s.bv1 + s.bp2*s.bv2 + s.bp3*s.bv3 + s.bp4*s.bv4;
        double wAskPrice = s.ap0*s.av0 + s.ap1*s.av1 + s.ap2*s.av2 + s.ap3*s.av3 + s.ap4*s.av4;

        float midPrice = (float)((s.ap0 + s.bp0) * 0.5); // *0.5 比 /2.0 快

        // --- 3. 填充因子 (利用倒数乘法) ---
        float[] f = this.factorValues; // 本地引用

        f[0] = s.ap0 - s.bp0;
        f[1] = f[0] / (midPrice + EPSILON); // 这里保留除法，midPrice 变化大不适合预计算
        f[2] = midPrice;
        f[3] = (s.bv0 - s.av0) * 1.0f / (s.bv0 + s.av0 + EPSILON);

        // 优化：除法转乘法
        f[4] = (sumBidVol - sumAskVol) * invTotalVol;
        f[5] = sumBidVol;
        f[6] = sumAskVol;
        f[7] = sumBidVol - sumAskVol;
        f[8] = sumBidVol * invSumAsk;
        f[9] = (s.tBidVol - s.tAskVol) * invTotalTVol;

        f[10] = (float)(wBidPrice * invSumBid);
        f[11] = (float)(wAskPrice * invSumAsk);
        f[12] = (float)((wBidPrice + wAskPrice) * invTotalVol);
        f[13] = f[11] - f[10];
        f[14] = f[7] * 0.2f;

        f[15] = calculateAsymmetry(s);
        f[19] = f[0] * invTotalVol;

        // 变动因子 (利用 Dummy 对象的 tradeTime=0 特性)
        if (p.tradeTime != 0) {
            calculateChangeFactors(s, p, midPrice, sumBidVol, sumAskVol);
        } else {
            f[16] = 0;
            f[17] = 0;
            f[18] = 0;
        }
    }

    private float calculateAsymmetry(SnapshotData s) {
        // 使用乘法代替除法
        double wBid = s.bv0 + s.bv1*0.5 + s.bv2*0.333333 + s.bv3*0.25 + s.bv4*0.2;
        double wAsk = s.av0 + s.av1*0.5 + s.av2*0.333333 + s.av3*0.25 + s.av4*0.2;
        return (float)((wBid - wAsk) / (wBid + wAsk + EPSILON));
    }

    private void calculateChangeFactors(SnapshotData c, SnapshotData p, float currMid, long currSumBid, long currSumAsk) {
        float[] f = this.factorValues;

        f[16] = c.ap0 - p.ap0;

        float prevMid = (float)((p.ap0 + p.bp0) * 0.5);
        f[17] = currMid - prevMid;

        long prevSumBid = p.bv0 + p.bv1 + p.bv2 + p.bv3 + p.bv4;
        long prevSumAsk = p.av0 + p.av1 + p.av2 + p.av3 + p.av4;

        float prevDepthRatio = (float)(prevSumBid / (double)(prevSumAsk + EPSILON));
        float currDepthRatio = (float)(currSumBid / (double)(currSumAsk + EPSILON));

        f[18] = currDepthRatio - prevDepthRatio;
    }

    public static boolean hasInvalidValue(float[] values) {
        for (float val : values) {
            if (Float.isNaN(val) || Float.isInfinite(val)) return true;
        }
        return false;
    }

    // Merge, Write, Read...
    public void merge(Factor other) {
        for (int i = 0; i < 20; i++) this.factorValues[i] += other.factorValues[i];
        this.count += other.count;
    }
    public float[] getFactorValues() { return factorValues; }
    public void setCount(int c) { this.count = c; }
    public int getCount() { return count; }

    public double[] getAverageFactors() {
        double[] avg = new double[20];
        for (int i = 0; i < 20; i++) avg[i] = count > 0 ? factorValues[i] / count : 0;
        return avg;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (float v : factorValues) out.writeFloat(v);
        out.writeInt(count);
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < 20; i++) factorValues[i] = in.readFloat();
        count = in.readInt();
    }
}