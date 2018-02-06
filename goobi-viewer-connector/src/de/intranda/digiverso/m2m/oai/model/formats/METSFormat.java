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
package de.intranda.digiverso.m2m.oai.model.formats;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.intranda.digiverso.m2m.DataManager;
import de.intranda.digiverso.m2m.oai.RequestHandler;
import de.intranda.digiverso.m2m.oai.enums.Metadata;
import de.intranda.digiverso.m2m.oai.model.ErrorCode;
import de.intranda.digiverso.m2m.utils.SolrConstants;
import de.intranda.digiverso.m2m.utils.Utils;

/**
 * METS
 */
public class METSFormat extends AbstractFormat {

    private final static Logger logger = LoggerFactory.getLogger(METSFormat.class);

    /* (non-Javadoc)
     * @see de.intranda.digiverso.m2m.oai.model.formats.AbstractFormat#createListRecords(de.intranda.digiverso.m2m.oai.RequestHandler, int, int)
     */
    @Override
    public Element createListRecords(RequestHandler handler, int firstRow, int numRows) throws IOException, SolrServerException {
        QueryResponse qr = solr.getListRecords(Utils.filterDatestampFromRequest(handler), firstRow, numRows, false,
                " AND " + SolrConstants.SOURCEDOCFORMAT + ":METS", null);
        if (qr.getResults()
                .isEmpty()) {
            return new ErrorCode().getNoRecordsMatch();
        }
        try {
            return generateMets(qr.getResults(), qr.getResults()
                    .getNumFound(), firstRow, numRows, handler, "ListRecords");
        } catch (IOException e) {
            logger.error(e.getMessage());
            return new ErrorCode().getIdDoesNotExist();
        } catch (JDOMException e) {
            return new ErrorCode().getCannotDisseminateFormat();
        }
    }

    /* (non-Javadoc)
     * @see de.intranda.digiverso.m2m.oai.model.formats.AbstractFormat#createGetRecord(de.intranda.digiverso.m2m.oai.RequestHandler)
     */
    @Override
    public Element createGetRecord(RequestHandler handler) {
        if (handler.getIdentifier() == null) {
            return new ErrorCode().getBadArgument();
        }
        try {
            SolrDocument doc = solr.getListRecord(handler.getIdentifier());
            if (doc == null) {
                return new ErrorCode().getCannotDisseminateFormat();
            }
            return generateMets(Collections.singletonList(doc), 1L, 0, 1, handler, "GetRecord");
        } catch (IOException e) {
            return new ErrorCode().getIdDoesNotExist();
        } catch (JDOMException e) {
            return new ErrorCode().getCannotDisseminateFormat();
        } catch (SolrServerException e) {
            return new ErrorCode().getIdDoesNotExist();
        }
    }

    /**
     * Creates a list of METS documents for the given Solr document list.
     * 
     * @param records
     * @param totalHits
     * @param firstRow
     * @param numRows
     * @param handler
     * @param recordType "GetRecord" or "ListRecords"
     * @return
     * @throws IOException
     * @throws JDOMException
     * @throws SolrServerException
     */
    private static Element generateMets(List<SolrDocument> records, long totalHits, int firstRow, int numRows, RequestHandler handler,
            String recordType) throws JDOMException, IOException, SolrServerException {
        Namespace xmlns = DataManager.getInstance()
                .getConfiguration()
                .getStandardNameSpace();
        Element xmlListRecords = new Element(recordType, xmlns);

        Namespace mets = Namespace.getNamespace(Metadata.mets.getMetadataPrefix(), Metadata.mets.getMetadataNamespace());
        Namespace xsi = Namespace.getNamespace("xsi", "http://www.w3.org/2001/XMLSchema-instance");
        Namespace mods = Namespace.getNamespace("mods", "http://www.loc.gov/mods/v3");
        Namespace dv = Namespace.getNamespace("dv", "http://dfg-viewer.de/");
        Namespace xlink = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");

        if (records.size() < numRows) {
            numRows = records.size();
        }
        for (SolrDocument doc : records) {
            String url = new StringBuilder(DataManager.getInstance()
                    .getConfiguration()
                    .getDocumentResolverUrl()).append(doc.getFieldValue(SolrConstants.PI_TOPSTRUCT))
                            .toString();
            String xml = Utils.getWebContent(url);
            if (StringUtils.isEmpty(xml)) {
                xmlListRecords.addContent(new ErrorCode().getCannotDisseminateFormat());
                continue;
            }

            org.jdom2.Document metsFile = Utils.getDocumentFromString(xml, null);
            Element mets_root = metsFile.getRootElement();
            Element newmets = new Element(Metadata.mets.getMetadataPrefix(), mets);
            newmets.addNamespaceDeclaration(xsi);
            newmets.addNamespaceDeclaration(mods);
            newmets.addNamespaceDeclaration(dv);
            newmets.addNamespaceDeclaration(xlink);
            newmets.setAttribute("schemaLocation",
                    "http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-3.xsd http://www.loc.gov/METS/ http://www.loc.gov/standards/mets/version17/mets.v1-7.xsd",
                    xsi);
            newmets.addContent(mets_root.cloneContent());

            Element record = new Element("record", xmlns);
            Element header = getHeader(doc, null, handler);
            record.addContent(header);
            Element metadata = new Element("metadata", xmlns);
            metadata.addContent(newmets);
            record.addContent(metadata);
            xmlListRecords.addContent(record);
        }

        // Create resumption token
        if (totalHits > firstRow + numRows) {
            Element resumption = createResumptionTokenAndElement(totalHits, firstRow + numRows, xmlns, handler);
            xmlListRecords.addContent(resumption);
        }

        return xmlListRecords;
    }

    /* (non-Javadoc)
     * @see de.intranda.digiverso.m2m.oai.model.formats.AbstractFormat#getTotalHits(java.util.Map)
     */
    @Override
    public long getTotalHits(Map<String, String> params) throws IOException, SolrServerException {
        return solr.getTotalHitNumber(params, false, " AND " + SolrConstants.SOURCEDOCFORMAT + ":METS", null);
    }

}