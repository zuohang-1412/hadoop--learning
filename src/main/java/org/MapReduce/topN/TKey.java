package org.MapReduce.topN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 自定义类型必须实现序列化/反序列化和比较器
public class TKey implements WritableComparable<TKey> {
    private int year;
    private int month;
    private int day;

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public void setWd(int wd) {
        this.wd = wd;
    }

    public int getDay() {
        return day;
    }

    public int getWd() {
        return wd;
    }

    private int wd;

    @Override
    public int compareTo(TKey that) {
        // the value 0 if x == y; a value less than 0 if x < y; and a value greater than 0 if x > y
        int y1 = Integer.compare(this.year, that.getYear());
        if (y1 ==0) {
            int m1 = Integer.compare(this.month, that.getMonth());
            if (m1 == 0) {
                return Integer.compare(this.day, that.getDay());
            }
            return m1;
        }
        return y1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
    // 序列化
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(wd);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    // 反 序列化
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.wd = in.readInt();
    }
}
