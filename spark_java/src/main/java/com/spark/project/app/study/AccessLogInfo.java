package com.spark.project.app.study;

import java.io.Serializable;

/**
 * @author frankq
 * @date 2021/6/28
 */
public class AccessLogInfo implements Serializable {
    private static final long serialVersionUID = -3814432611337155209L;

    /**
     * 时间戳
     */
    private long timestamp;
    /**
     * 上行流量
     */
    private long upTraffic;
    /**
     * 下行流量
     */
    private long downTraffic;

    public AccessLogInfo() {
    }

    public AccessLogInfo(long timestamp, long upTraffic, long downTraffic) {
        this.timestamp = timestamp;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getUpTraffic() {
        return upTraffic;
    }

    public void setUpTraffic(long upTraffic) {
        this.upTraffic = upTraffic;
    }

    public long getDownTraffic() {
        return downTraffic;
    }

    public void setDownTraffic(long downTraffic) {
        this.downTraffic = downTraffic;
    }
}
