/**
 * This file is part of the Goobi viewer Connector - OAI-PMH and SRU interfaces for digital objects.
 *
 * Visit these websites for more information.
 *          - http://www.intranda.com
 *          - http://digiverso.com
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package de.intranda.digiverso.m2m.utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.m2m.DataManager;
import de.intranda.digiverso.m2m.oai.RequestHandler;
import de.intranda.digiverso.m2m.oai.enums.Metadata;
import de.intranda.digiverso.m2m.oai.model.LicenseType;
import de.intranda.digiverso.m2m.oai.model.Set;

public class SolrSearchIndex {

    /** Logger for this class. */
    final static Logger logger = LoggerFactory.getLogger(SolrSearchIndex.class);

    private static final int MIN_SCHEMA_VERSION = 20151001;
    private static final String SCHEMA_VERSION_PREFIX = "intranda_viewer-";

    public static final int MAX_HITS = Integer.MAX_VALUE;
    private static final int TIMEOUT_SO = 10000;
    private static final int TIMEOUT_CONNECTION = 10000;

    private SolrServer server;
    private final boolean testMode;

    public SolrSearchIndex(SolrServer server, boolean testMode) {
        this.testMode = testMode;
        if (server == null) {
            if (StringUtils.isEmpty(DataManager.getInstance().getConfiguration().getIndexUrl())) {
                logger.error("Solr URL is not configured. Cannot instantiate the OAI-PMH interface.");
                return;
            }
            this.server = getNewHttpSolrServer(DataManager.getInstance().getConfiguration().getIndexUrl());
        } else {
            this.server = server;
        }
    }

    /**
     * Checks whether the server's configured URL matches that in the config file. If not, a new server instance is created.
     */
    public void checkReloadNeeded() {
        if (!testMode && server != null && server instanceof HttpSolrServer) {
            HttpSolrServer httpSolrServer = (HttpSolrServer) server;
            if (!DataManager.getInstance().getConfiguration().getIndexUrl().equals(httpSolrServer.getBaseURL())) {
                logger.info("Solr URL has changed, re-initializing SolrHelper...");
                httpSolrServer.shutdown();
                server = getNewHttpSolrServer(DataManager.getInstance().getConfiguration().getIndexUrl());
            }
        }
    }

    /**
     * 
     * @param indexUrl
     * @return
     */
    public static HttpSolrServer getNewHttpSolrServer(String indexUrl) {
        if (indexUrl == null) {
            throw new IllegalArgumentException("indexUrl may not be null");
        }
        HttpSolrServer server = new HttpSolrServer(indexUrl);
        server.setSoTimeout(TIMEOUT_SO); // socket read timeout
        server.setConnectionTimeout(TIMEOUT_CONNECTION);
        server.setDefaultMaxConnectionsPerHost(100);
        server.setMaxTotalConnections(100);
        server.setFollowRedirects(false); // defaults to false
        server.setAllowCompression(true);
        server.setMaxRetries(1); // defaults to 0. > 1 not recommended.
        // server.setParser(new XMLResponseParser()); // binary parser is used by default
        server.setRequestWriter(new BinaryRequestWriter());

        return server;
    }

    /**
     * 
     * @param query
     * @return
     * @throws SolrServerException
     */
    public SolrDocumentList search(String query) throws SolrServerException {
        SolrQuery solrQuery = new SolrQuery(query + getAllSuffixes());
        solrQuery.setRows(MAX_HITS);
        QueryResponse resp = server.query(solrQuery);

        return resp.getResults();
    }

    /**
     * Pure Solr search method.
     * 
     * @param query {@link String}
     * @param first {@link Integer}
     * @param rows {@link Integer}
     * @param sortFields
     * @param fieldList If not null, only the fields in the list will be returned.
     * @param params Additional query parameters.
     * @return {@link QueryResponse}
     * @throws SolrServerException
     * @throws PresentationException
     * @should return correct results
     * @should return correct number of rows
     * @should sort results correctly
     */
    public QueryResponse search(String query, int first, int rows, List<String> sortFields, List<String> fieldList, Map<String, String> params)
            throws SolrServerException {
        SolrQuery solrQuery = new SolrQuery(query);
        solrQuery.setStart(first);
        solrQuery.setRows(rows - first);

        if (sortFields != null && !sortFields.isEmpty()) {
            for (String sortField : sortFields) {
                solrQuery.addSort(sortField, ORDER.asc);
                // logger.trace("Added sorting field: {}", sortField);
            }
        }
        if (fieldList != null && !fieldList.isEmpty()) {
            for (String field : fieldList) {
                // logger.trace("adding result field: " + field);
                if (StringUtils.isNotEmpty(field)) {
                    solrQuery.addField(field);
                }
            }
        }
        if (params != null && !params.isEmpty())

        {
            for (String key : params.keySet()) {
                solrQuery.set(key, params.get(key));
                // logger.trace("&{}={}", key, params.get(key));
            }
        }

        // logger.trace("Solr query: {}", solrQuery.toString());
        // logger.debug("range: {} - {}", first, rows);
        // logger.debug("facetFields: " + facetFields);
        // logger.debug("fieldList: " + fieldList);
        QueryResponse resp = server.query(solrQuery);
        // logger.debug("found: " + resp.getResults().getNumFound());
        // logger.debug("fetched: {}", resp.getResults().size());

        return resp;

    }

    /**
     * searches and returns a list of {@link SolrDocument}
     * 
     * @param from startdate
     * @param until enddate
     * @param set which collection
     * @param metadataPrefix
     * @param firstRow
     * @param numRows
     * @param urnOnly
     * @param querySuffix
     * @return list of hits as {@link SolrDocument}
     * @throws IOException
     * @throws SolrServerException
     */
    public SolrDocumentList search(String from, String until, String set, String metadataPrefix, int firstRow, int numRows, boolean urnOnly,
            String querySuffix) throws IOException, SolrServerException {
        StringBuilder sbQuery = new StringBuilder(buildQueryString(from, until, set, metadataPrefix, urnOnly, querySuffix));
        if (urnOnly) {
            sbQuery.append(" AND (").append(SolrConstants.URN).append(":* OR ").append(SolrConstants.IMAGEURN_OAI).append(":*)");
        }
        sbQuery.append(getAllSuffixes());
        logger.debug("OAI query: {}", sbQuery.toString());
        SolrQuery solrQuery = new SolrQuery(sbQuery.toString());
        solrQuery.setStart(firstRow);
        solrQuery.setRows(numRows);
        solrQuery.addSort(SolrConstants.DATECREATED, ORDER.asc);
        QueryResponse resp = server.query(solrQuery);
        logger.debug("Total hits: {}, fetched records {} - {}", resp.getResults().getNumFound(), firstRow, firstRow + resp.getResults().size() - 1);

        return resp.getResults();
    }

    /**
     * there is no difference between oai_dc or mets
     * 
     * @param params
     * @param firstRow
     * @param numRows
     * @param urnOnly
     * @return
     * @throws SolrServerException
     */
    public SolrDocumentList getListIdentifiers(Map<String, String> params, int firstRow, int numRows, boolean urnOnly) throws SolrServerException {
        try {
            return search(params.get("from"), params.get("until"), params.get("set"), params.get("metadataPrefix"), firstRow, numRows, urnOnly, null);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return null;
    }

    /**
     * listRecords display the whole meta-data of an element
     * 
     * @param params
     * @param firstRow
     * @param numRows
     * @param urnOnly
     * @param querySuffix
     * @return
     * @throws SolrServerException
     */
    public SolrDocumentList getListRecords(Map<String, String> params, int firstRow, int numRows, boolean urnOnly, String querySuffix)
            throws SolrServerException {
        try {
            return search(params.get("from"), params.get("until"), params.get("set"), params.get("metadataPrefix"), firstRow, numRows, urnOnly,
                    querySuffix);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return new SolrDocumentList();
    }

    /**
     * Searches for identifier and return {@link SolrDocument} identifier can be PPN or URN (doc or page)
     * 
     * @param identifier Identifier to search
     * @param metadataPrefix
     * @return {@link SolrDocument}
     * @throws IOException
     * @throws SolrServerException
     */
    public SolrDocument getListRecord(final String identifier, String metadataPrefix) throws IOException, SolrServerException {
        SolrDocumentList ret = query(identifier, metadataPrefix, 1);
        if (!ret.isEmpty()) {
            return ret.get(0);
        }

        return null;
    }

    /**
     * 
     * @param identifier
     * @param metadataPrefix
     * @return
     * @throws SolrServerException
     */
    public boolean isRecordExists(final String identifier, final String metadataPrefix) throws SolrServerException {
        return !query(identifier, metadataPrefix, 0).isEmpty();
    }

    /**
     * 
     * @param identifier
     * @param metadataPrefix
     * @param rows
     * @return
     * @throws SolrServerException
     */
    private SolrDocumentList query(final String identifier, String metadataPrefix, int rows) throws SolrServerException {
        String useIdentifier = ClientUtils.escapeQueryChars(identifier);

        StringBuilder sb = new StringBuilder();
        sb.append('(').append(SolrConstants.PI).append(':').append(useIdentifier).append(" OR ").append(SolrConstants.URN).append(':').append(
                useIdentifier).append(" OR ").append(SolrConstants.IMAGEURN).append(':').append(useIdentifier).append(')');
        sb.append(getAllSuffixes());
        logger.debug(sb.toString());
        SolrQuery solrQuery = new SolrQuery(sb.toString());
        solrQuery.setRows(rows);
        QueryResponse resp = server.query(solrQuery);

        //        if (resp.getResults().isEmpty() && Metadata.epicur.name().equals(metadataPrefix)) {
        //            solrQuery = new SolrQuery(new StringBuilder(SolrConstants.IMAGEURN_OAI).append(":").append(identifier).toString());
        //            solrQuery.setRows(rows);
        //            resp = server.query(solrQuery);
        //        }

        return resp.getResults();
    }

    /**
     * build the query String depending on the variables
     * 
     * @param from
     * @param until
     * @param set
     * @param metadataPrefix
     * @param excludeAnchor
     * @param querySuffix
     * @return
     */

    private static String buildQueryString(String from, String until, String set, String metadataPrefix, boolean excludeAnchor, String querySuffix) {
        StringBuilder query = new StringBuilder();
        query.append('(').append(SolrConstants.ISWORK).append(":true");
        if (!excludeAnchor) {
            query.append(" OR ").append(SolrConstants.ISANCHOR).append(":true");
        }
        if (StringUtils.isNotEmpty(querySuffix)) {
            query.append(querySuffix);
        }
        query.append(')');
        // Solr timestamp range is irrelevant for iv_* formats
        if (!Metadata.iv_overviewpage.name().equals(metadataPrefix) && !Metadata.iv_crowdsourcing.name().equals(metadataPrefix) && (from != null
                || until != null)) {
            long fromTimestamp = RequestHandler.getFromTimestamp(from);
            long untilTimestamp = RequestHandler.getUntilTimestamp(until);
            if (fromTimestamp == untilTimestamp) {
                untilTimestamp += 999;
            }
            query.append(" AND ").append(SolrConstants.DATEUPDATED).append(":[").append(normalizeDate(String.valueOf(fromTimestamp))).append(" TO ")
                    .append(normalizeDate(String.valueOf(untilTimestamp))).append(']');

        }
        if (set != null) {
            // check if set is definied in configuration file
            List<Set> additionalSetList = DataManager.getInstance().getConfiguration().getAdditionalSets();
            boolean defaultSet = true;
            for (Set currentSet : additionalSetList) {
                if (currentSet.getSetSpec().equals(set)) {
                    defaultSet = false;
                    set = currentSet.getSetQuery();
                    break;
                }
            }
            if (defaultSet) {
                // if set is not defined, use DC query 
                query.append(" AND ").append(SolrConstants.DC).append(':').append(set);
            } else {
                // if set is configured, use configured query
                if (set.charAt(0) == '-' || set.startsWith("NOT(")) {
                    // do not wrap the conditions in parentheses if it starts with a negation, otherwise it won't work
                    query.append(" AND ").append(set);
                } else {
                    query.append(" AND ").append(set);
                }
            }
        }
        if (metadataPrefix != null) {
            switch (metadataPrefix.toLowerCase()) {
                case "mets":
                case "marcxml":
                    query.append(" AND ").append(SolrConstants.SOURCEDOCFORMAT).append(':').append(SolrConstants._METS);
                    break;
                case "lido":
                    query.append(" AND ").append(SolrConstants.SOURCEDOCFORMAT).append(':').append(SolrConstants._LIDO);
                    break;
            }
        }

        return query.toString();
    }

    /**
     * Creates a list with all available values for the given field (minus any blacklisted values).
     * 
     * @return
     * @throws SolrServerException
     * @should return all values
     */
    public List<String> getSets(String field) throws SolrServerException {
        List<String> ret = new ArrayList<>();

        SolrQuery query = new SolrQuery();
        query.setQuery(field + ":* " + DataManager.getInstance().getConfiguration().getCollectionBlacklistFilterSuffix());
        query.setStart(0);
        query.setRows(0);
        query.addFacetField(field);
        logger.trace("Set query: {}", query.getQuery());
        QueryResponse resp = server.query(query);

        FacetField facetField = resp.getFacetField(field);
        if (facetField != null) {
            for (Count count : facetField.getValues()) {
                if (count.getCount() == 0) {
                    continue;
                }
                ret.add(count.getName());
            }
        }

        Collections.sort(ret);
        logger.trace("{} terms found for {}", ret.size(), field);
        return ret;
    }

    /**
     * 
     * @param params
     * @param urnOnly
     * @param querySuffix
     * @return size of search
     * @throws IOException
     * @throws SolrServerException
     */
    public long getTotalHitNumber(Map<String, String> params, boolean urnOnly, String querySuffix) throws IOException, SolrServerException {
        StringBuilder sbQuery = new StringBuilder(buildQueryString(params.get("from"), params.get("until"), params.get("set"), params.get(
                "metadataPrefix"), urnOnly, querySuffix));
        if (urnOnly) {
            sbQuery.append(" AND (").append(SolrConstants.URN).append(":* OR ").append(SolrConstants.IMAGEURN_OAI).append(":*)");
        }
        sbQuery.append(getAllSuffixes());
        logger.debug("OAI query: {}", sbQuery.toString());
        SolrQuery solrQuery = new SolrQuery(sbQuery.toString());
        solrQuery.setStart(0);
        solrQuery.setRows(0);
        solrQuery.addSort(SolrConstants.DATECREATED, ORDER.asc);
        QueryResponse resp = server.query(solrQuery);

        long num = resp.getResults().getNumFound();
        logger.debug("Total hits: {}", num);
        return num;
    }

    public String getEarliestRecordDatestamp() throws SolrServerException {
        try {
            String searchStr = SolrConstants.ISWORK + ":true" + getAllSuffixes();

            SolrQuery solrQuery = new SolrQuery(searchStr);
            solrQuery.setRows(1);
            solrQuery.addField(SolrConstants.DATECREATED);
            solrQuery.addSort(SolrConstants.DATECREATED, ORDER.asc);
            QueryResponse resp = server.query(solrQuery);

            if (resp.getResults().size() > 0) {
                SolrDocument doc = resp.getResults().get(0);
                if (doc.getFieldValue(SolrConstants.DATECREATED) != null) {
                    Date d = new Date((Long) doc.getFieldValue(SolrConstants.DATECREATED));
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");// ;YYYY-MM-DDThh:mm:ssZ
                    SimpleDateFormat hours = new SimpleDateFormat("HH:mm:ss");
                    format.setTimeZone(TimeZone.getTimeZone("GMT"));
                    hours.setTimeZone(TimeZone.getTimeZone("GMT"));
                    String yearMonthDay = format.format(d);
                    String hourMinuteSeconde = hours.format(d);

                    return yearMonthDay + "T" + hourMinuteSeconde + "Z";
                }
            }
        } catch (NullPointerException e) {
            logger.error(e.getMessage(), e);
        }
        return "";
    }

    /**
     * If the given SolrDocument is an anchor, retrieve the latest DATEUPDATED timestamp value from its volumes.
     * 
     * @param anchorDoc
     * @param untilTimestamp
     * @return
     * @throws SolrServerException
     */
    public long getLatestVolumeTimestamp(SolrDocument anchorDoc, long untilTimestamp) throws SolrServerException {
        if (anchorDoc.getFieldValue(SolrConstants.ISANCHOR) != null && (Boolean) anchorDoc.getFieldValue(SolrConstants.ISANCHOR)) {
            SolrDocumentList volumes = search(SolrConstants.ISWORK + ":true AND " + SolrConstants.IDDOC_PARENT + ":" + (String) anchorDoc
                    .getFieldValue(SolrConstants.IDDOC));
            if (volumes != null) {
                long latest = 0;
                for (SolrDocument volume : volumes) {
                    long volumeTimestamp = getLatestValidDateUpdated(volume, untilTimestamp);
                    if (latest < volumeTimestamp) {
                        latest = volumeTimestamp;
                    }
                }

                if (latest > 0) {
                    return latest;
                }
            }
        }

        return -1;
    }

    /**
     * Returns the blacklist filter suffix (if enabled), followed by the user-agnostic access condition suffix.
     * 
     * @return
     */
    public static String getAllSuffixes() {
        StringBuilder sb = new StringBuilder();
        if (DataManager.getInstance().getConfiguration().isUseCollectionBlacklist()) {
            sb.append(DataManager.getInstance().getConfiguration().getCollectionBlacklistFilterSuffix());
        }
        List<LicenseType> restrictedLicenseTypes = DataManager.getInstance().getConfiguration().getRestrictedAccessConditions();
        if (!restrictedLicenseTypes.isEmpty()) {
            sb.append(" -(");
            boolean moreThanOne = false;
            for (LicenseType licenseType : restrictedLicenseTypes) {
                if (moreThanOne) {
                    sb.append(" OR ");
                }
                if (StringUtils.isNotEmpty(licenseType.getConditions())) {
                    if (licenseType.getConditions().charAt(0) == '-') {
                        // do not wrap the conditions in parentheses if it starts with a negation, otherwise it won't work
                        sb.append('(').append(licenseType.getField()).append(":\"").append(licenseType.getValue()).append("\" AND ").append(
                                licenseType.getProcessedConditions()).append(')');
                    } else {
                        sb.append('(').append(licenseType.getField()).append(":\"").append(licenseType.getValue()).append("\" AND (").append(
                                licenseType.getProcessedConditions()).append("))");
                    }
                } else {
                    sb.append(licenseType.getField()).append(':').append(licenseType.getValue());
                }
                moreThanOne = true;
            }
            sb.append(')');
        }
        String querySuffix = DataManager.getInstance().getConfiguration().getQuerySuffix();
        if (StringUtils.isNotEmpty(querySuffix)) {
            if (querySuffix.charAt(0) != ' ') {
                sb.append(' ');
            }
            sb.append(querySuffix);
        }

        return sb.toString();
    }

    /**
     * 
     * @param date
     * @return
     */
    private static String normalizeDate(String date) {
        if (date != null) {
            if (date.charAt(0) == '-' || date.length() > 12) {
                return date;
            }
            switch (date.length()) {
                case 8:
                    return "00000" + date;
                case 9:
                    return "0000" + date;
                case 10:
                    return "000" + date;
                case 11:
                    return "00" + date;
                case 12:
                    return "0" + date;
                default: // nothing
            }
        }

        return date;
    }

    /**
     * Returns the latest DATEUPDATED value on the given <code>SolrDocument</code> that is no larger than <code>untilTimestamp</code>. Returns 0 if no
     * such value is found.
     * 
     * @param doc
     * @param untilTimestamp
     * @return Latest DATEUPDATED value is less than or equals untilTimestamp on doc; 0 if none found.
     * @should return correct value
     * @should return 0 if no valid value is found
     */
    public static Long getLatestValidDateUpdated(SolrDocument doc, long untilTimestamp) {
        long ret = 0;
        Collection<Object> dateUpdatedValues = doc.getFieldValues(SolrConstants.DATEUPDATED);
        if (dateUpdatedValues != null && !dateUpdatedValues.isEmpty()) {
            // Get latest DATEUPDATED values
            for (Object o : dateUpdatedValues) {
                long dateUpdated = (Long) o;
                if (dateUpdated > ret && dateUpdated <= untilTimestamp) {
                    ret = dateUpdated;
                }
            }
        }

        return ret;
    }

    /**
     * 
     * @param query
     * @param fieldList
     * @return
     * @throws SolrServerException
     * @throws PresentationException
     * @throws IndexUnreachableException
     * @should return correct doc
     */
    public SolrDocument getFirstDoc(String query, List<String> fieldList) throws SolrServerException {
        logger.trace("getFirstDoc: {}", query);
        SolrDocumentList hits = search(query, 0, 1, null, fieldList, null).getResults();
        if (hits.getNumFound() > 0) {
            return hits.get(0);
        }

        return null;
    }

}