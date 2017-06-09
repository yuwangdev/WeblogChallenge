/**
 * Created by yuwang on 6/8/17.
 */

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ELBAccessLog implements Serializable {

    private Date dateTimeString;
    private String elbName;
    private String clientIpAddress;
    private int clientPort;
    private String backendIpAddress;
    private int backendPort;
    private String sourceIpAddress;
    private int sourcePort;
    private float requestProcessingTime;
    private float backendProcessingTime;
    private float responseProcessingTime;
    private float elbStatusCode;
    private float backendStatusCode;
    private long receivedBytes;
    private long sentBytes;
    private String request;
    private String userAgent;
    private String sslCipher;
    private String sslProtocol;

    //timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol

    private static final String LOG_ENTRY_PATTERN = "([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*):([0-9]*) ([.0-9]*) ([.0-9]*) ([.0-9]*) (-|[0-9]*) (-|[0-9]*) ([-0-9]*) ([-0-9]*) \\\"([^ ]*) ([^ ]*) (- |[^ ]*)\\\" (\\\"[^\\\"]*\\\") ([A-Z0-9-]+) ([A-Za-z0-9.-]*)";
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public ELBAccessLog(
            Date timestamp,
            String elb,
            String clientIpAddress,
            String clientPort,
            String backendIpAddress,
            String backendPort,
            String requestProcessingTime,
            String backendProcessingTime,
            String responseProcessingTime,
            String elbStatusCode,
            String backendStatusCode,
            String receivedBytes,
            String sentBytes,
            String request,
            String userAgent,
            String sslCipher,
            String sslProtocol
    ) {


        this.dateTimeString = timestamp;
        this.elbName = elb;
        this.setClientIpAddress(clientIpAddress);
        this.clientPort = Integer.parseInt(clientPort);
        this.backendIpAddress = backendIpAddress;
        this.backendPort = Integer.parseInt(backendPort);
        this.requestProcessingTime = Float.parseFloat(requestProcessingTime);
        this.backendProcessingTime = Float.parseFloat(backendProcessingTime);
        this.responseProcessingTime = Float.parseFloat(responseProcessingTime);
        this.elbStatusCode = Integer.parseInt(elbStatusCode);
        this.backendStatusCode = Integer.parseInt(backendStatusCode);
        this.receivedBytes = Long.parseLong(receivedBytes);
        this.setSentBytes(Long.parseLong(sentBytes));
        this.setRequest(request);
        this.setUserAgent(userAgent);
        this.setSslCipher(sslCipher);
        this.setSslProtocol(sslProtocol);
    }

    public static ELBAccessLog parseFromLogLine(String logline) {
        Matcher m = PATTERN.matcher(logline);

        if (!m.find()) {
            return null;
        }

        return new ELBAccessLog(
                convertToTimestamp(m.group(1)),
                m.group(2),
                m.group(3),
                m.group(4),
                m.group(5),
                m.group(6),
                m.group(7),
                m.group(8),
                m.group(9),
                m.group(10),
                m.group(11),
                m.group(12),
                m.group(13),
                m.group(14),
                m.group(15),
                m.group(16),
                m.group(17)
        );
    }


    public void setSslProtocol(String sslProtocol) {
        this.sslProtocol = sslProtocol;
    }


    public void setSslCipher(String sslCipher) {
        this.sslCipher = sslCipher;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }


    public void setSentBytes(long sentBytes) {
        this.sentBytes = sentBytes;
    }


    public void setRequest(String request) {
        this.request = request;
    }

    public String getClientIpAddress() {
        return clientIpAddress;
    }

    public void setClientIpAddress(String clientIpAddress) {
        this.clientIpAddress = clientIpAddress;
    }

    public static Date convertToTimestamp(String tr) {
        //2015-07-22T09:00:28.019143Z
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            java.util.Date parsedDate = dateFormat.parse(tr);
            Date timestamp = new Date(parsedDate.getTime());
            return timestamp;
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        return null;

    }


    public Date getDateTimeString() {
        return dateTimeString;
    }
}