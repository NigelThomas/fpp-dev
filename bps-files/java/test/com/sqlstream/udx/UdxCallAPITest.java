package com.sqlstream.udx;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.*;

import static org.mockito.Mockito.*;

public class UdxCallAPITest {

    @Test
    public void testAddFeatureParamsForBrowserUseCase() throws SQLException {
        final int[] rowCount = {1};
        String rowTime = "2019-09-24 13:24:28.297";
        String visitId = "81964afd-483f-4a82-8f19-8672c16259c1";
        String signals = "{\"ecookie\":\"00609dfb-409c-43fa-9a6a-4fde6f2e8703\",\"signals\":{\"navigator\":{\"userAgent\":\"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\"},\" screen  \":{\"height\":1080,\" width  \":1920}},\"computedSignals\":{\"networkIp\":\"749f4ee478b4368cb066f4cd5f080ecff6f58b4d529696c476c2176f15d226f9\",\"userAgent\":\"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\",\"browserName\":\"chrome\",\"browserVersion\":\"74.0.3729.169\",\"osName\":\"windows\",\"osVersion\":\"7\",\"osFamily\":\"Windows\",\"osVersionOfFamily\":\"7\",\"osReleaseDateOrder\":3},\" neustar  \":{\"continent\":\"europe\",\"country\":\"france\",\"country_code\":\"fr\",\"country_cf\":99,\" region  \":\"ile-de-france\",\"state\":\"seine-saint-denis\",\"state_cf\":\"80\",\"city\":\"la plaine-saint-denis\",\"city_cf\":\"61\",\"postal_code\":\"93534\",\"time_zone\":\"1\",\"latitude\":\"48.9085\",\"longitude\":\"2.3638\",\"connection_type\":\"tx\",\"line_speed\":\"high\",\"ip_routing_type\":\"fixed\",\"asn\":\"62044\",\"organization\":\"zscaler  inc.\",\"carrier\":\"zscaler switzerland gmbh\",\"hosting_facility\":\"true\",\"ip_address\":\"165.225.76.126\"}}";
        String eval = "{\"tenantId\":\"mybank\",\"userId\":\"2e681e4f3e7cb5bca1e8cee6e4355bd748d5e1cc666ff1b7eb648d869b04eaae\",\"time\":1569331468184,\"matchedPolicyName\":\"Default policy\",\"visitId\":\"81964afd-483f-4a82-8f19-8672c16259c1\",\"getDecisionDetails\":{},\"context\":{\"events\":\"login\",\"usergroups\":\"pilot_user\",\"customerEmailAddress\":\"b63436deff20130b9551d89c5d417bdf9ddd2382bf7ccda6897720feb1275049\",\"userId\":\"2e681e4f3e7cb5bca1e8cee6e4355bd748d5e1cc666ff1b7eb648d869b04eaae\",\"client\":\"web\"},\"decision\":{\"access\":\"allowed\",\"auth\":[{\"type\":\"password\"}]}}";

        ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);

        Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(4);
        Mockito.when(resultSetMetaData.getColumnName(1)).thenReturn("ROWTIME");
        Mockito.when(resultSetMetaData.getColumnName(2)).thenReturn("visitId");
        Mockito.when(resultSetMetaData.getColumnName(3)).thenReturn("eval");
        Mockito.when(resultSetMetaData.getColumnName(4)).thenReturn("signals");

        ResultSet resultSet = mock(ResultSet.class);
        Mockito.when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        Mockito.when(resultSet.next()).thenAnswer(invocation -> rowCount[0]-->0);
        Mockito.when(resultSet.getObject(1)).thenReturn(rowTime);
        Mockito.when(resultSet.getObject(2)).thenReturn(visitId);
        Mockito.when(resultSet.getObject(3)).thenReturn(eval);
        Mockito.when(resultSet.getObject(4)).thenReturn(signals);

        Mockito.when(resultSet.getObject("ROWTIME")).thenReturn(rowTime);
        Mockito.when(resultSet.getObject("visitId")).thenReturn(visitId);
        Mockito.when(resultSet.getObject("eval")).thenReturn(eval);
        Mockito.when(resultSet.getObject("signals")).thenReturn(signals);

        PreparedStatement preparedStatement = mock(PreparedStatement.class);

        UdxCallAPI.addFeatureParams(resultSet, preparedStatement);

        verify(preparedStatement, times(1)).setObject(Mockito.eq(1), Mockito.eq(rowTime));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(2), Mockito.eq(visitId));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(3), Mockito.eq(eval));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(4), Mockito.eq(signals));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(5), Mockito.eq("2e681e4f3e7cb5bca1e8cee6e4355bd748d5e1cc666ff1b7eb648d869b04eaae")); // userId
        verify(preparedStatement, times(1)).setObject(Mockito.eq(6), Mockito.eq("chrome")); // browserName
        verify(preparedStatement, times(1)).setObject(Mockito.eq(7), Mockito.eq("windows")); // osName
        verify(preparedStatement, times(1)).setObject(Mockito.eq(8), Mockito.eq("749f4ee478b4368cb066f4cd5f080ecff6f58b4d529696c476c2176f15d226f9")); // networkIp
    }

    @Test
    public void testAddScoreForBrowserUseCase() throws SQLException {

        final int[] rowCount = {1};
        String rowTime = "2019-09-24 13:24:28.297";
        String visitId = "81964afd-483f-4a82-8f19-8672c16259c1";
        String signals = "{\"ecookie\":\"00609dfb-409c-43fa-9a6a-4fde6f2e8703\",\"signals\":{\"navigator\":{\"userAgent\":\"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\"},\" screen  \":{\"height\":1080,\" width  \":1920}},\"computedSignals\":{\"networkIp\":\"749f4ee478b4368cb066f4cd5f080ecff6f58b4d529696c476c2176f15d226f9\",\"userAgent\":\"Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36\",\"browserName\":\"chrome\",\"browserVersion\":\"74.0.3729.169\",\"osName\":\"windows\",\"osVersion\":\"7\",\"osFamily\":\"Windows\",\"osVersionOfFamily\":\"7\",\"osReleaseDateOrder\":3},\" neustar  \":{\"continent\":\"europe\",\"country\":\"france\",\"country_code\":\"fr\",\"country_cf\":99,\" region  \":\"ile-de-france\",\"state\":\"seine-saint-denis\",\"state_cf\":\"80\",\"city\":\"la plaine-saint-denis\",\"city_cf\":\"61\",\"postal_code\":\"93534\",\"time_zone\":\"1\",\"latitude\":\"48.9085\",\"longitude\":\"2.3638\",\"connection_type\":\"tx\",\"line_speed\":\"high\",\"ip_routing_type\":\"fixed\",\"asn\":\"62044\",\"organization\":\"zscaler  inc.\",\"carrier\":\"zscaler switzerland gmbh\",\"hosting_facility\":\"true\",\"ip_address\":\"165.225.76.126\"}}";
        String eval = "{\"tenantId\":\"mybank\",\"userId\":\"2e681e4f3e7cb5bca1e8cee6e4355bd748d5e1cc666ff1b7eb648d869b04eaae\",\"time\":1569331468184,\"matchedPolicyName\":\"Default policy\",\"visitId\":\"81964afd-483f-4a82-8f19-8672c16259c1\",\"getDecisionDetails\":{},\"context\":{\"events\":\"login\",\"usergroups\":\"pilot_user\",\"customerEmailAddress\":\"b63436deff20130b9551d89c5d417bdf9ddd2382bf7ccda6897720feb1275049\",\"userId\":\"2e681e4f3e7cb5bca1e8cee6e4355bd748d5e1cc666ff1b7eb648d869b04eaae\",\"client\":\"web\"},\"decision\":{\"access\":\"allowed\",\"auth\":[{\"type\":\"password\"}]}}";
        String userId = "2e681e4f3e7cb5bca1e8cee6e4355bd748d5e1cc666ff1b7eb648d869b04eaae";
        String browserName = "chrome";
        String osName = "windows";
        String networkIp = "749f4ee478b4368cb066f4cd5f080ecff6f58b4d529696c476c2176f15d226f9";

        ResultSetMetaData resultSetMetaData = mock(ResultSetMetaData.class);

        Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(20);
        Mockito.when(resultSetMetaData.getColumnName(1)).thenReturn("ROWTIME");
        Mockito.when(resultSetMetaData.getColumnName(2)).thenReturn("visitId");
        Mockito.when(resultSetMetaData.getColumnName(3)).thenReturn("eval");
        Mockito.when(resultSetMetaData.getColumnName(4)).thenReturn("signals");
        Mockito.when(resultSetMetaData.getColumnName(5)).thenReturn("userId");
        Mockito.when(resultSetMetaData.getColumnName(6)).thenReturn("browserName");
        Mockito.when(resultSetMetaData.getColumnName(7)).thenReturn("osName");
        Mockito.when(resultSetMetaData.getColumnName(8)).thenReturn("networkIp");
        Mockito.when(resultSetMetaData.getColumnName(9)).thenReturn("w24h_userId_count");
        Mockito.when(resultSetMetaData.getColumnName(10)).thenReturn("w6h_userId_count");
        Mockito.when(resultSetMetaData.getColumnName(11)).thenReturn("w1h_userId_count");
        Mockito.when(resultSetMetaData.getColumnName(12)).thenReturn("w24h_browserName_count");
        Mockito.when(resultSetMetaData.getColumnName(13)).thenReturn("w24h_networkIp_count");
        Mockito.when(resultSetMetaData.getColumnName(14)).thenReturn("w24h_osName_count");
        Mockito.when(resultSetMetaData.getColumnName(15)).thenReturn("w6h_browserName_count");
        Mockito.when(resultSetMetaData.getColumnName(16)).thenReturn("w6h_networkIp_count");
        Mockito.when(resultSetMetaData.getColumnName(17)).thenReturn("w6h_osName_count");
        Mockito.when(resultSetMetaData.getColumnName(18)).thenReturn("w1h_browserName_count");
        Mockito.when(resultSetMetaData.getColumnName(19)).thenReturn("w1h_networkIp_count");
        Mockito.when(resultSetMetaData.getColumnName(20)).thenReturn("w1h_osName_count");

        ResultSet resultSet = mock(ResultSet.class);
        Mockito.when(resultSet.getMetaData()).thenReturn(resultSetMetaData);
        Mockito.when(resultSet.next()).thenAnswer(invocation -> rowCount[0]-->0);
        Mockito.when(resultSet.getObject(1)).thenReturn(rowTime);
        Mockito.when(resultSet.getObject(2)).thenReturn(visitId);
        Mockito.when(resultSet.getObject(3)).thenReturn(eval);
        Mockito.when(resultSet.getObject(4)).thenReturn(signals);
        Mockito.when(resultSet.getObject(5)).thenReturn(userId);
        Mockito.when(resultSet.getObject(6)).thenReturn(browserName);
        Mockito.when(resultSet.getObject(7)).thenReturn(osName);
        Mockito.when(resultSet.getObject(8)).thenReturn(networkIp);
        Mockito.when(resultSet.getObject(9)).thenReturn(1);
        Mockito.when(resultSet.getObject(10)).thenReturn(2);
        Mockito.when(resultSet.getObject(11)).thenReturn(3);
        Mockito.when(resultSet.getObject(12)).thenReturn(10);
        Mockito.when(resultSet.getObject(13)).thenReturn(20);
        Mockito.when(resultSet.getObject(14)).thenReturn(30);
        Mockito.when(resultSet.getObject(15)).thenReturn(40);
        Mockito.when(resultSet.getObject(16)).thenReturn(50);
        Mockito.when(resultSet.getObject(17)).thenReturn(60);
        Mockito.when(resultSet.getObject(18)).thenReturn(70);
        Mockito.when(resultSet.getObject(19)).thenReturn(80);
        Mockito.when(resultSet.getObject(20)).thenReturn(90);

        Mockito.when(resultSet.getObject("ROWTIME")).thenReturn(rowTime);
        Mockito.when(resultSet.getObject("visitId")).thenReturn(visitId);
        Mockito.when(resultSet.getObject("eval")).thenReturn(eval);
        Mockito.when(resultSet.getObject("signals")).thenReturn(signals);
        Mockito.when(resultSet.getObject("userId")).thenReturn(userId);
        Mockito.when(resultSet.getObject("browserName")).thenReturn(browserName);
        Mockito.when(resultSet.getObject("osName")).thenReturn(osName);
        Mockito.when(resultSet.getObject("networkIp")).thenReturn(networkIp);
        Mockito.when(resultSet.getObject("w24h_userId_count")).thenReturn(1);
        Mockito.when(resultSet.getObject("w6h_userId_count")).thenReturn(2);
        Mockito.when(resultSet.getObject("w1h_userId_count")).thenReturn(3);
        Mockito.when(resultSet.getObject("w24h_browserName_count")).thenReturn(10);
        Mockito.when(resultSet.getObject("w24h_networkIp_count")).thenReturn(20);
        Mockito.when(resultSet.getObject("w24h_osName_count")).thenReturn(30);
        Mockito.when(resultSet.getObject("w6h_browserName_count")).thenReturn(40);
        Mockito.when(resultSet.getObject("w6h_networkIp_count")).thenReturn(50);
        Mockito.when(resultSet.getObject("w6h_osName_count")).thenReturn(60);
        Mockito.when(resultSet.getObject("w1h_browserName_count")).thenReturn(70);
        Mockito.when(resultSet.getObject("w1h_networkIp_count")).thenReturn(80);
        Mockito.when(resultSet.getObject("w1h_osName_count")).thenReturn(90);

        PreparedStatement preparedStatement = mock(PreparedStatement.class);

        UdxCallAPI.addScore(resultSet, preparedStatement);

        verify(preparedStatement, times(1)).setObject(Mockito.eq(1), Mockito.eq(rowTime));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(2), Mockito.eq(visitId));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(3), Mockito.eq(eval));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(4), Mockito.eq(signals));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(5), Mockito.eq("2e681e4f3e7cb5bca1e8cee6e4355bd748d5e1cc666ff1b7eb648d869b04eaae")); // userId
        verify(preparedStatement, times(1)).setObject(Mockito.eq(6), Mockito.eq("chrome")); // browserName
        verify(preparedStatement, times(1)).setObject(Mockito.eq(7), Mockito.eq("windows")); // osName
        verify(preparedStatement, times(1)).setObject(Mockito.eq(8), Mockito.eq("749f4ee478b4368cb066f4cd5f080ecff6f58b4d529696c476c2176f15d226f9")); // networkIp

        verify(preparedStatement, times(1)).setObject(Mockito.eq(9), Mockito.eq(1));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(10), Mockito.eq(2));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(11), Mockito.eq(3));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(12), Mockito.eq(10));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(13), Mockito.eq(20));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(14), Mockito.eq(30));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(15), Mockito.eq(40));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(16), Mockito.eq(50));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(17), Mockito.eq(60));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(18), Mockito.eq(70));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(19), Mockito.eq(80));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(20), Mockito.eq(90));
        verify(preparedStatement, times(1)).setObject(Mockito.eq(21), Mockito.eq(String.format("{\"modelName\":\"%s\",\"modelVersion\":\"%s\",\"score\":%.2f}",UdxCallAPI.MODEL_NAME, UdxCallAPI.MODEL_VERSION, 0.5)));
    }
}
