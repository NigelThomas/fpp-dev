package com.sqlstream.udx;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class UdxCallAPI {

    public static String MODEL_NAME = "fppml";
    public static String MODEL_VERSION = "1.0-alpha";

	protected static final Logger tracer = Logger.getLogger("com.sqlstream.utility.JsonBatchArray");
	protected static String endpoint = null;

	/**
	 * Construct a PreparedStatement containing the columns in the following order: ROWTIME, visitId, eval, signals, userId, browserName, osName, networkIp
	 *
	 * @param resultSet
	 * @param preparedStatement
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	public static void addFeatureParams(ResultSet resultSet, PreparedStatement preparedStatement) throws SQLException {
		// Use ResultSetMetaData to get input column count
		final ResultSetMetaData setMetaData = resultSet.getMetaData();
		final int inputSetColumnCount = setMetaData.getColumnCount();

		// Stay in here as long as there are rows to process. In the case
		// of a stream this could be forever.
		while (resultSet.next()) {
			List<Object> outputColumns = new ArrayList<>();

			// Add columns existing in the ResultSet (visitId, signals, context)
			for (int i=1; i<=inputSetColumnCount; i++) {
			    Object o = resultSet.getObject(i);
				outputColumns.add(o);
				// tracer.info("[" + i + "] " + setMetaData.getColumnName(i) + ": " + o);
			}

			// Add userId
			String contexts = (String) resultSet.getObject("eval" );
			outputColumns.addAll(extractProperties(contexts,
			"$.userId",
			"$.events",
			"$.flag001",
			"$.flag002",
			"$.usergroups"));

			// Add browserName, osName, networkIp
			String signals = (String) resultSet.getObject("signals" );
			outputColumns.addAll(extractProperties(signals,
			"$.computedSignals.browserName",
			"$.computedSignals.browserVersion",
			"$.platform.signals.isRooted",
			"$.computedSignals.networkIp",
			"$.computedSignals.osFamily",
			"$.computedSignals.osName",
			"$.computedSignals.osReleaseDateOrder",
			"$.computedSignals.osVersionOfFamily",
			"$.computedSignals.osVersion"));

			// Add the screen Size properties (that differ between mobile and browser)
			outputColumns.add(extractProperty(signals, "$.device.signals.screenHeight", "$.signals.screen.height"));
			outputColumns.add(extractProperty(signals, "$.device.signals.screenWidth", "$.signals.screen.width"));
			outputColumns.addAll(extractProperties(signals,
			"$.computedSignals.userAgent",
			"$.device.signals.fingerprint",
			"$.device.signals.manufacturer",
			"$.device.signals.model",
			"$.location.signals.source",
			"$.network.signals.hwAddress",
			"$.network.signals.type",
			"$.platformSettings.signals.isBluetoothEnabled",
			"$.platformSettings.signals.isLocationEnabled",
			"$.platformSettings.signals.isWifiEnabled",
			"$.platform.signals.family"));

			// Add Neustar properties
			String neustar = (String) resultSet.getObject("neustar" );
			outputColumns.addAll(extractProperties(neustar,
			"$.area_code",
			"$.city",
			"$.postal_code",
			"$.time_zone",
			"$.country_code",
			"$.country",
			"$.state_code",
			"$.state",
			"$.continent",
			"$.dma",
			"$.latitude",
			"$.longitude",
			"$.msa",
			"$.region",
			"$.sld",
			"$.tld",
			"$.asn",
			"$.carrier",
			"$.connection_type",
			"$.hosting_facility",
			"$.ip_routing_type",
			"$.line_speed",
			"$.organization",
			"$.proxy_level",
			"$.proxy_type",
			"$.anonymizer_status",
			"$.ip_type"
			));

			outputColumns.add((String)resultSet.getObject("visitId")); // transactionId
			outputColumns.add((String)resultSet.getObject("tenantId")); // tenant
			outputColumns.add(extractProperty(signals, "$.device.signals.fingerprint", "$.signals.ecookie")); // deviceId

			// Also Add dayOfWeek and Hour of Day (computed here for the PoC, must be moved to data source ASAP)
			Date now = new Date();
			GregorianCalendar calendar = new GregorianCalendar();
			calendar.setTime(now);
			outputColumns.add(calendar.get(Calendar.DAY_OF_WEEK));
			outputColumns.add(calendar.get(Calendar.HOUR_OF_DAY));

			// Set columns in the preparedStatement
			for (int i=0; i<outputColumns.size(); i++) {
				// tracer.info("[" + (1+i) + "] : " + outputColumns.get(i));
				preparedStatement.setObject(1+i, outputColumns.get(i));
			}

			preparedStatement.executeUpdate();
		}
	}

	/**
     * Request a score value [0..1] from DSS API
     *
	 * @param resultSet       result of SQL CURSOR operator
	 * @param preparedStatement used to supply output columns
	 * @throws SQLException
	 */
	public static void addScore(ResultSet resultSet, PreparedStatement preparedStatement) throws SQLException {
		try {
			// Use ResultSetMetaData to get input column count
			final ResultSetMetaData setMetaData = resultSet.getMetaData();
            final int inputSetColumnCount = setMetaData.getColumnCount();

			// Stay in here as long as there are rows to process.
            // In the case of a stream this could be forever.
			while (resultSet.next()) {
                List<Object> outputColumns = new ArrayList<Object>();

                // Add columns existing in the ResultSet (visitId, signals, context)
                for (int i=1; i<=inputSetColumnCount; i++) {
                    Object o = resultSet.getObject(i);
                    outputColumns.add(o);
                    // tracer.info("[" + i + "] " + setMetaData.getColumnName(i) + ": " + o);
                }


                // TODO IMPLEMENT THE CALL TO DSS

//				String responseBody = sendPost( getEndpoint(), String.format(
//					"{\"userId\":\"%s\",\"new_line\":{\"w24h_u_userId_count\":\"%s\",\"w6h_u_userId_count\":\"%s\",\"w1h_u_userId_count\":\"%s\",\"w24h_o_osName0_score\":\"%s\",\"w24h_n_networkIp0_score\":\"%s\",\"w24h_b_BrowserName0_score\":\"%s\"}}",
//					resultSet.getObject("context_client" ),
//					resultSet.getObject("w24h_userId_count"),
//					resultSet.getObject("w6h_userId_count"),
//					resultSet.getObject("w1h_userId_count"),
//					resultSet.getObject("w24h_osName_count"),
//					resultSet.getObject("w24h_networkIp_count"),
//					resultSet.getObject("w24h_browserName_count")));
//
//				if (responseBody != null) {
//					Object score = extractProperty(responseBody, "$.response.results[0].data.score");
//					if (score != null) {
//						outputColumns.add(String.format("{\"modelName\":\"%s\",\"modelVersion\":\"%s\",\"value\":%.2f}", UdxCallAPI.MODEL_NAME, UdxCallAPI.MODEL_VERSION, score));
//					} else {
//						outputColumns.add(String.format("{\"modelName\":\"%s\",\"modelVersion\":\"%s\",\"value\":null}", UdxCallAPI.MODEL_NAME, UdxCallAPI.MODEL_VERSION));
//					}
//				} else {
//					outputColumns.add(String.format("{\"modelName\":\"%s\",\"modelVersion\":\"%s\",\"value\":null,\"msg\": \"DSS response body is null\"}", UdxCallAPI.MODEL_NAME, UdxCallAPI.MODEL_VERSION));
//					tracer.severe( String.format("model (%sv %s) evaluation failed: DSS response body is null", MODEL_NAME, MODEL_VERSION));
//				}

				outputColumns.add(String.format("{\"modelName\":\"%s\",\"modelVersion\":\"%s\",\"value\":%.2f}", UdxCallAPI.MODEL_NAME, UdxCallAPI.MODEL_VERSION, 0.5));

                // Set columns in the preparedStatement
                for (int i=0; i<outputColumns.size(); i++) {
                	Object o = outputColumns.get(i);
                    // tracer.info(String.format("[%d] %s", 1+i, o));
                    preparedStatement.setObject(1+i, o);
                }

				// Pass the row on.
                preparedStatement.executeUpdate();
			}
		} catch (Exception e) {
			tracer.log(Level.SEVERE, "Exception addScore", e);
		}
	}

	protected static Object extractProperty(String json, String... paths) {
		return extractProperty(Configuration.defaultConfiguration().jsonProvider().parse(json), paths);
	}

	protected static Object extractProperty(Object document, String... paths) {
		for (String p : paths) {
			try {
				return JsonPath.read(document, p);
			} catch (PathNotFoundException e) {
				// Skip this property, but try the next one
			}
		}
		return null;
	}

	protected static List extractProperties(String json, String... paths) {
		ArrayList<Object> result = new ArrayList<>();
		Object document = Configuration.defaultConfiguration().jsonProvider().parse(json);

		for (String p : paths) {
			result.add(extractProperty(document, p));
		}

		return result;
	}

	protected static String sendPost(String urlStr, String jsonRequest) throws IOException {
		int responseCode = -1;
		URL url = new URL(urlStr);

		HttpURLConnection conn = (HttpURLConnection) url.openConnection();

		// Add request header
		conn.setRequestMethod("POST");
		conn.setRequestProperty("User-Agent", "fpp/1.0");
		conn.setRequestProperty("Content-Type", "application/json");

		// Send post request
		conn.setDoOutput(true);
		DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
		wr.writeBytes(jsonRequest);
		wr.flush();
		wr.close();

		tracer.info("POST " + url);
		tracer.info("BODY " + jsonRequest);
		responseCode = conn.getResponseCode();

		if (responseCode < 300) {
			// get response if there is one
			BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();

			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();

			String responseBody = response.toString();
			tracer.info(String.format("RESP %s %s",responseCode, responseBody));
			return responseBody;
		}

		tracer.info("RESP" + responseCode);
		return null;
	}

	protected static String getEndpoint() throws IOException {
		if (endpoint == null) {
			InputStream is = null;
			try {
				String pathToHomeDirectory = System.getProperty("HOME", "/home/sqlstream");
				tracer.info(pathToHomeDirectory);
				is = new FileInputStream(pathToHomeDirectory + "/config/fpp-udx.properties");
				Properties prop = new Properties();
				prop.load(is);
				endpoint = prop.getProperty("fpp_udx_endpoint");
			} finally {
				if (is != null) {
					is.close();
				}
			}
		}

		return endpoint;
	}
}