package com.spark.study.secondarySort;


import scala.math.Ordered;

import java.io.Serializable;

/**
 *
 * 自定义的二次排序的key
 * 按照第一列进行排序
 * 如果第一列相等，就按照第二列进行排序
 *
 * @author yangqian
 * @date 2021/5/9
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {

    private static final long serialVersionUID = -2506786728405704686L;

    private int first;
    private int second;

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean $greater(SecondarySortKey other) {
        if (this.first > other.getFirst()) {
            return true;
        } else if (this.first == other.first && this.second > other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey other) {
        if (this.$greater(other)) {
            return true;
        } else if (this.first == other.getFirst() && this.second == other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less(SecondarySortKey other) {
        if (this.first < other.getFirst()) {
            return true;
        } else if (this.first == other.getFirst() && this.second < other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey other) {
        if (this.$less(other)) {
            return true;
        } else if (this.first == other.getFirst() && this.second == other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public int compare(SecondarySortKey other) {
        if (this.first - other.getFirst() != 0) {
            return this.first - other.getFirst();
        } else if (this.first - other.getFirst() == 0) {
            return this.second - other.getSecond();
        }
        return 0;
    }

    @Override
    public int compareTo(SecondarySortKey other) {
        if (this.first - other.getFirst() != 0) {
            return this.first - other.getFirst();
        } else if (this.first - other.getFirst() == 0) {
            return this.second - other.getSecond();
        }
        return 0;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return "SecondarySortKey{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
