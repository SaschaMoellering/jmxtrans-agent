package org.jmxtrans.agent;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import static org.jmxtrans.agent.util.ConfigurationUtils.getString;

/**
 * Created by sascha.moellering on 27/03/2015.
 */
public class CloudWatchOutputWriter extends AbstractOutputWriter implements OutputWriter {

    public final static String SETTING_CW_NAMESPACE = "namespace";
    public final static int SETTING_SOCKET_CONNECT_TIMEOUT_IN_MILLIS_DEFAULT_VALUE = 500;

    public static final String METADATA_URL = "http://169.254.169.254/latest/dynamic/instance-identity/document";
    public static final String REGION = "region";
    public static final String ENCODING = "UTF-8";

    protected String namespace;
    protected Region awsRegion;
    private AmazonCloudWatchClient cloudWatchClient;
    private int socketConnectTimeoutInMillis = SETTING_SOCKET_CONNECT_TIMEOUT_IN_MILLIS_DEFAULT_VALUE;

    @Override
    public void postConstruct(Map<String, String> settings) {
        super.postConstruct(settings);

        try {
            namespace = getString(settings, SETTING_CW_NAMESPACE, null);

            // Configuring the CloudWatch client
            // Credentials are loaded from the Amazon EC2 Instance Metadata Service

            cloudWatchClient = new AmazonCloudWatchClient(new InstanceProfileCredentialsProvider());
            awsRegion = Region.getRegion(Regions.fromName(getRegion()));
            cloudWatchClient.setRegion(awsRegion);

            logger.log(getInfoLevel(), "CloudWatchOutputWriter is configured with Region " + awsRegion.toString() +
                    ", namespace=" + namespace);
        }

        catch (IOException exc) {
            logger.log(Level.SEVERE, "Could not connect to AWS Metadata service", exc);
        }
    }

    @Override
    public void writeInvocationResult(@Nonnull String invocationName, @Nullable Object value) throws IOException {
        writeQueryResult(invocationName, null, value);
    }

    @Override
    public void writeQueryResult(@Nonnull String metricName, @Nullable String metricType, @Nullable Object value) throws IOException {
        PutMetricDataRequest metricDataRequest = new PutMetricDataRequest();
        metricDataRequest.setNamespace(namespace);
        List<MetricDatum> metricDatumList = new ArrayList<MetricDatum>();

        MetricDatum metricDatum = new MetricDatum();
        metricDatum.setMetricName(metricName);

        // Converts the Objects to Double-values for CloudWatch
        metricDatum.setValue(convertToDouble(value));
        metricDatum.setTimestamp(new Date());

        metricDatumList.add(metricDatum);
        metricDataRequest.setMetricData(metricDatumList);

        cloudWatchClient.putMetricData(metricDataRequest);
    }

    /**
     * Determines the region of the EC2-instance by parsing the instance metadata.
     * For more information on instance metadata take a look at <a href="http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AESDG-chapter-instancedata.html">Instance Metadata and User Data</a></>
     *
     * @return The region as String
     * @throws IOException If the instance meta-data is not available
     */
    String getRegion() throws IOException {

        BufferedReader in = null;
        try {
            String region = "";

            // URL of the instance metadata service
            URL url = new URL(METADATA_URL);
            URLConnection conn = url.openConnection();
            conn.setConnectTimeout(SETTING_SOCKET_CONNECT_TIMEOUT_IN_MILLIS_DEFAULT_VALUE);
            String inputLine;

            in = new BufferedReader(
                    new InputStreamReader(
                            conn.getInputStream(), ENCODING));
            while ((inputLine = in.readLine()) != null) {
                if (inputLine.contains(REGION)) {
                    String[] splitLine = inputLine.split(":");
                    region = splitLine[1].replaceAll("\"", "").replaceAll(",", "").trim();
                }
            }

            return region;
        } finally {
            if (in != null)
                in.close();
        }
    }

    /**
     * Converts Objects to Doubles for CloudWatch
     *
     * @param obj The object to convert
     * @return The Double-value
     */
    private Double convertToDouble(Object obj) {
        Double d = null;
        if (obj instanceof Double) {
            d = (Double) obj;
        } else if (obj instanceof Long) {
            d = ((Long) obj).doubleValue();
        } else if (obj instanceof Integer) {
            d = ((Integer) obj).doubleValue();
        } else {
            logger.log(Level.SEVERE, "There is no converter from " + obj.getClass().getName() + " to Double ");
        }

        return d;
    }

    @Override
    public String toString() {
        return "CloudWatchOutputWriter{" +
                ", " + awsRegion.toString() +
                ", namespace='" + namespace + '\'' +
                '}';
    }
}
