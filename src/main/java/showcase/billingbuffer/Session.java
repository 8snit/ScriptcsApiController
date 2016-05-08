package showcase.billingbuffer;

import java.io.Serializable;
import java.util.Date;

public final class Session implements Serializable {
    public final String sessionId;
    public final String sessionIP;
    public final boolean isPrepaid;
    public final long uploadOverall;
    public final long uploadAllowed;
    public final long uploadBilled;
    public final long downloadOverall;
    public final long downloadAllowed;
    public final long downloadBilled;
    public final Date creationTimestamp;
    public final String creationOrigin;
    public final Date deletionTimestamp;
    public final String deletionOrigin;
    public final Date lastUsageTimestamp;
    public final String lastUsageOrigin;
    public final Date lastBillingTimestamp;
    public final String lastBillingOrigin;

    public Session(String sessionId, String sessionIP, boolean isPrepaid, long uploadOverall, long uploadAllowed, long uploadBilled, long downloadOverall, long downloadAllowed, long downloadBilled, Date creationTimestamp, String creationOrigin, Date deletionTimestamp, String deletionOrigin, Date lastUsageTimestamp, String lastUsageOrigin, Date lastBillingTimestamp, String lastBillingOrigin) {
        this.sessionId = sessionId;
        this.sessionIP = sessionIP;
        this.isPrepaid = isPrepaid;
        this.uploadOverall = uploadOverall;
        this.uploadAllowed = uploadAllowed;
        this.uploadBilled = uploadBilled;
        this.downloadOverall = downloadOverall;
        this.downloadAllowed = downloadAllowed;
        this.downloadBilled = downloadBilled;
        this.creationTimestamp = creationTimestamp;
        this.creationOrigin = creationOrigin;
        this.deletionTimestamp = deletionTimestamp;
        this.deletionOrigin = deletionOrigin;
        this.lastUsageTimestamp = lastUsageTimestamp;
        this.lastUsageOrigin = lastUsageOrigin;
        this.lastBillingTimestamp = lastBillingTimestamp;
        this.lastBillingOrigin = lastBillingOrigin;
    }

    @Override
    public String toString() {
        return "Session{" +
                "sessionId='" + sessionId + '\'' +
                ", sessionIP='" + sessionIP + '\'' +
                ", isPrepaid=" + isPrepaid +
                ", uploadOverall=" + uploadOverall +
                ", uploadAllowed=" + uploadAllowed +
                ", uploadBilled=" + uploadBilled +
                ", downloadOverall=" + downloadOverall +
                ", downloadAllowed=" + downloadAllowed +
                ", downloadBilled=" + downloadBilled +
                ", creationTimestamp=" + creationTimestamp +
                ", creationOrigin='" + creationOrigin + '\'' +
                ", deletionTimestamp=" + deletionTimestamp +
                ", deletionOrigin='" + deletionOrigin + '\'' +
                ", lastUsageTimestamp=" + lastUsageTimestamp +
                ", lastUsageOrigin='" + lastUsageOrigin + '\'' +
                ", lastBillingTimestamp=" + lastBillingTimestamp +
                ", lastBillingOrigin='" + lastBillingOrigin + '\'' +
                '}';
    }
}