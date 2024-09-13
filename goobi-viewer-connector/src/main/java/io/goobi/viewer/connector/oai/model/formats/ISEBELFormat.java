package io.goobi.viewer.connector.oai.model.formats;

import static io.goobi.viewer.api.rest.v1.ApiUrls.RECORDS_FILES;
import static io.goobi.viewer.api.rest.v1.ApiUrls.RECORDS_FILES_ALTO;
import static io.goobi.viewer.api.rest.v1.ApiUrls.RECORDS_FILES_PLAINTEXT;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.jdom2.Element;
import org.jdom2.Namespace;

import com.ctc.wstx.shaded.msv_core.verifier.ErrorInfo.ElementErrorInfo;

import io.goobi.viewer.connector.DataManager;
import io.goobi.viewer.connector.oai.RequestHandler;
import io.goobi.viewer.connector.oai.enums.Metadata;
import io.goobi.viewer.connector.oai.enums.Verb;
import io.goobi.viewer.connector.oai.model.ErrorCode;
import io.goobi.viewer.connector.oai.model.metadata.MetadataParameter;
import io.goobi.viewer.connector.oai.model.metadata.MetadataParameter.MetadataParameterType;
import io.goobi.viewer.connector.utils.SolrSearchTools;
import io.goobi.viewer.connector.utils.Utils;
import io.goobi.viewer.controller.ALTOTools;
import io.goobi.viewer.controller.FileTools;
import io.goobi.viewer.controller.NetTools;
import io.goobi.viewer.controller.StringTools;
import io.goobi.viewer.exceptions.HTTPException;
import io.goobi.viewer.messages.ViewerResourceBundle;
import io.goobi.viewer.model.translations.language.Language;
import io.goobi.viewer.solr.SolrConstants;

/**
 * ISEBEL
 */
public class ISEBELFormat extends Format {
	
	private static final Logger logger = LogManager.getLogger(ISEBELFormat.class);
	
	protected static Map<String, String> anchorTitles = new HashMap<>();
	
	private List<String> setSpecFields = DataManager.getInstance().getConfiguration().getSetSpecFieldsForMetadataFormat(Metadata.ISEBEL.getMetadataPrefix());
	
	/** {@inheritDoc} */
	@Override
	public Element createListRecords(
		RequestHandler handler,
		int firstVirtualRow,
		int firstRawRow,
		int numRows,
		String versionDiscriminatorField,
		String filterQuerySuffix
	) throws SolrServerException, IOException {
		QueryResponse qr;
		long totalVirtualHits;
		long totalRawHits;
		
		if (StringUtils.isNotEmpty(versionDiscriminatorField)) {
            // One OAI record for each record version
			qr = solr.getListRecords(
            	Utils.filterDatestampFromRequest(handler), 
            	firstRawRow, 
            	numRows, 
            	false,
                SolrSearchTools.getAdditionalDocstructsQuerySuffix(
                    DataManager
                    	.getInstance()
                    	.getConfiguration()
                    	.getAdditionalDocstructTypes()
                ) + " AND " + versionDiscriminatorField + ":*",
                filterQuerySuffix, 
                null, 
                Collections.singletonList(versionDiscriminatorField)
            );
            totalVirtualHits = SolrSearchTools.getFieldCount(
            	qr, 
            	versionDiscriminatorField
            );
            totalRawHits = qr.getResults().getNumFound();
        } else {
            // One OAI record for each record proper
            qr = solr.getListRecords(
            	Utils.filterDatestampFromRequest(handler), 
            	firstRawRow, 
            	numRows, 
            	false,
                SolrSearchTools.getAdditionalDocstructsQuerySuffix(
                	DataManager
                		.getInstance()
                		.getConfiguration()
                		.getAdditionalDocstructTypes()
                ),
                filterQuerySuffix, 
                null, 
                null);
            totalRawHits = qr.getResults().getNumFound();
            totalVirtualHits = totalRawHits;
		}
		
        if (qr.getResults().isEmpty()) {
            logger.trace("Results are empty");
            return new ErrorCode().getNoRecordsMatch();
        }

        return generateDC(
        	qr.getResults(), 
        	totalVirtualHits, 
        	totalRawHits, 
        	firstVirtualRow, 
        	firstRawRow, 
        	numRows, 
        	handler, 
        	"ListRecords",
            versionDiscriminatorField, 
            null, 
            filterQuerySuffix
        );
	}

    /** {@inheritDoc} */
    @Override
    public Element createGetRecord(
    	RequestHandler handler, 
    	String filterQuerySuffix
    ) {
        if (handler.getIdentifier() == null) {
            return new ErrorCode().getBadArgument();
        }
        String versionDiscriminatorField =
            DataManager.getInstance()
            	.getConfiguration()
                .getVersionDisriminatorFieldForMetadataFormat(
                	handler.getMetadataPrefix().getMetadataPrefix()
                );
        if (StringUtils.isNotEmpty(versionDiscriminatorField)) {
            String[] identifierSplit = Utils.splitIdentifierAndLanguageCode(
            	handler.getIdentifier(), 3
            );
            try {
                SolrDocument doc = solr.getListRecord(
                	identifierSplit[0], 
                	null, 
                	filterQuerySuffix
                );
                if (doc == null) {
                    return new ErrorCode().getIdDoesNotExist();
                }
                return generateDC(
                	Collections.singletonList(doc), 
                	1L, 
                	1L, 
                	0, 
                	0, 
                	1, 
                	handler, 
                	"GetRecord", 
                	versionDiscriminatorField,
                    identifierSplit[1], 
                    filterQuerySuffix
                );
            } catch (IOException | SolrServerException e) {
                return new ErrorCode().getNoMetadataFormats();
            }
        }
        try {
            SolrDocument doc = solr.getListRecord(
            	handler.getIdentifier(), 
            	null, 
            	filterQuerySuffix
            );
            if (doc == null) {
                return new ErrorCode().getIdDoesNotExist();
            }
            return generateDC(
            	Collections.singletonList(doc), 
            	1L, 
            	1L, 
            	0, 
            	0, 
            	1, 
            	handler, 
            	"GetRecord", 
            	null, 
            	null, 
            	filterQuerySuffix
            );
        } catch (IOException | SolrServerException e) {
            return new ErrorCode().getNoMetadataFormats();
        }
    }

    /**
     * generates oai_dc records
     * 
     * @param records
     * @param totalVirtualHits
     * @param totalRawHits
     * @param firstVirtualRow
     * @param firstRawRow
     * @param numRows
     * @param handler
     * @param recordType
     * @param versionDiscriminatorField
     * @param requestedVersion
     * @param filterQuerySuffix Filter query suffix for the client's session
     * @return {@link Element}
     * @throws SolrServerException
     * @throws IOException
     */
    Element generateDC(
    	List<SolrDocument> records, 
    	long totalVirtualHits, 
    	long totalRawHits, 
    	int firstVirtualRow, 
    	int firstRawRow,
        final int numRows, 
        RequestHandler handler, 
        String recordType, 
        String versionDiscriminatorField, 
        String requestedVersion,
        String filterQuerySuffix
    ) throws SolrServerException, IOException {
        Namespace nsOaiDoc = Namespace.getNamespace(
        		Metadata.ISEBEL.getMetadataNamespacePrefix(), 
        		Metadata.ISEBEL.getMetadataNamespaceUri());
        Element xmlListRecords = new Element(recordType, OAI_NS);

        int virtualHitCount = 0;
        if (StringUtils.isNotEmpty(versionDiscriminatorField)) {
            List<String> versions = Collections.singletonList(requestedVersion);
            for (SolrDocument doc : records) {
                if (requestedVersion == null) {
                    versions = SolrSearchTools.getMetadataValues(
                    	doc, versionDiscriminatorField
                    );
                }
                for (String version : versions) {
                    virtualHitCount++;
                    String iso3code = version;
                    // Make sure to add the ISO-3 language code
                    if (SolrConstants.LANGUAGE.equals(versionDiscriminatorField) && 
                    		iso3code.length() == 2) {
                        Language lang = DataManager
                        	.getInstance()
                        	.getLanguageHelper()
                        	.getLanguage(version);
                        if (lang != null) {
                            iso3code = lang.getIsoCode();
                        }
                    }
                    xmlListRecords.addContent(
                    	generateSingleDCRecord(
                    		doc, 
                    		handler, 
                    		iso3code, 
                    		OAI_NS, 
                    		nsOaiDoc, 
                    		setSpecFields, 
                    		filterQuerySuffix
                    	)
                    );
                }
            }
        } else {
            for (SolrDocument doc : records) {
                xmlListRecords.addContent(
                	generateSingleDCRecord(
                		doc, 
                		handler, 
                		null, 
                		OAI_NS, 
                		nsOaiDoc, 
                		setSpecFields, 
                		filterQuerySuffix
                	)
                );
                virtualHitCount++;
            }
        }

        // Create resumption token
        int useNumRows = numRows;
        if (records.size() < useNumRows) {
            useNumRows = records.size();
        }
        if (totalRawHits > firstRawRow + useNumRows) {
            Element resumption = createResumptionTokenAndElement(
            	totalVirtualHits, 
            	totalRawHits, 
            	firstVirtualRow + virtualHitCount,
                firstRawRow + useNumRows, 
                firstVirtualRow, 
                handler);
            xmlListRecords.addContent(resumption);
        }

        return xmlListRecords;
    }

    /**
     * 
     * @param doc
     * @param handler
     * @param requestedVersion
     * @param xmlns
     * @param nsOaiDoc
     * @param setSpecFields
     * @param filterQuerySuffix Filter query suffix for the client's session
     * @return {@link ElementErrorInfo}
     * @throws SolrServerException
     * @throws IOException
     * @should generate element correctly
     */
    Element generateSingleDCRecord(
    	SolrDocument doc, 
   		RequestHandler handler, 
    	String requestedVersion, 
    	Namespace xmlns, 
  		Namespace nsOaiDoc,
        List<String> setSpecFields, 
        String filterQuerySuffix
    ) throws SolrServerException, IOException {
        boolean isWork = doc.getFieldValue(SolrConstants.ISWORK) != null 
        	&& (boolean) doc.getFieldValue(SolrConstants.ISWORK);
        boolean isAnchor = doc.getFieldValue(SolrConstants.ISANCHOR) != null 
        	&& (boolean) doc.getFieldValue(SolrConstants.ISANCHOR);
        boolean openAccess = true;
        Set<String> accessConditions = new HashSet<>();
        if (doc.getFieldValues(SolrConstants.ACCESSCONDITION) != null) {
            for (Object o : doc.getFieldValues(SolrConstants.ACCESSCONDITION)) {
                accessConditions.add((String) o);
                if (!SolrConstants.OPEN_ACCESS_VALUE.equals(o)) {
                    openAccess = false;
                }
            }
        }
        SolrDocument topstructDoc = null;
        if (isWork || isAnchor) {
            topstructDoc = doc;
        } else {
            // If child element metadata fields are empty, get certain values from topstruct
            String iddocTopstruct = (String) doc.getFieldValue(SolrConstants.IDDOC_TOPSTRUCT);
            SolrDocumentList docList = solr.search(
            	"+" + SolrConstants.IDDOC + ":" + iddocTopstruct, 
            	filterQuerySuffix
            );
            if (docList != null && !docList.isEmpty()) {
                topstructDoc = docList.get(0);
            }
        }
        if (topstructDoc == null && !doc.containsKey(SolrConstants.DATEDELETED)) {
            logger.warn(
            	"No topstruct found for IDDOC:{} - is this a page document? Please check the base query.",
            	doc.getFieldValue(SolrConstants.IDDOC)
            );
        }
        SolrDocument anchorDoc = null;
        if (!isAnchor) {
            SolrDocument childDoc = topstructDoc != null ? topstructDoc : doc;
            String iddocAnchor = (String) childDoc.getFieldValue(SolrConstants.IDDOC_PARENT);
            if (iddocAnchor != null) {
                SolrDocumentList docList = solr.search(
                	"+" + SolrConstants.IDDOC + ":" + iddocAnchor, 
                	filterQuerySuffix
                );
                if (docList != null && !docList.isEmpty()) {
                    anchorDoc = docList.get(0);
                }
            }
        }
        String docstruct = (String) doc.getFieldValue(SolrConstants.DOCSTRCT);

        Element eleRecord = new Element("record", xmlns);
        Element header = getHeader(
        	doc, 
        	topstructDoc, 
        	handler, 
        	requestedVersion, 
        	setSpecFields, 
        	filterQuerySuffix
        );
        eleRecord.addContent(header);

        if ("deleted".equals(header.getAttributeValue("status"))) {
            return eleRecord;
        }

        // create the metadata element, special for dc
        Element metadata = new Element("metadata", xmlns);

        // creating Element <oai_dc:dc ....> </oai_dc:dc>
        Namespace nsIsebel = nsOaiDoc;
        Namespace nsDc = Namespace.getNamespace(
        	Metadata.DC.getMetadataNamespacePrefix(), 
        	Metadata.DC.getMetadataNamespaceUri()
        );
        Namespace nsOaiDc = Namespace.getNamespace(
        	Metadata.OAI_DC.getMetadataNamespacePrefix(),
        	Metadata.OAI_DC.getMetadataNamespaceUri()
        );
        Namespace nsDataCite = Namespace.getNamespace(
        	"datacite", "http://datacite.org/schema/kernel-4"
        );
        Element eleDoc = new Element("dc", nsOaiDc);
   
        String docID = SolrSearchTools.getMetadataValues(doc, "PI").get(0);
        String idDoc = SolrSearchTools.getMetadataValues(doc, "IDDOC").get(0);
        
        SolrDocumentList folkloreRecords = solr.search(
        	"+PI_TOPSTRUCT:\"" 
        		+ docID
        		+ "\" +DOCTYPE:\"DOCSTRCT\" +DOCSTRCT:\"folklore_record\"", 
        	filterQuerySuffix
        );
        
        for (int i = 0; i < folkloreRecords.size(); i++) {
        	SolrDocument rec = folkloreRecords.get(i);
        	String idRec = SolrSearchTools.getMetadataValues(rec, "IDDOC").get(0);
        	
        	Element eleRec = new Element("story", nsIsebel);
            eleRec.addNamespaceDeclaration(nsDc);
            eleRec.addNamespaceDeclaration(nsDataCite);
            eleRec.addNamespaceDeclaration(XSI_NS);
            eleRec.setAttribute(
            	"schemaLocation", 
            	Metadata.ISEBEL.getMetadataNamespaceUri() + " " + Metadata.ISEBEL.getSchema(), 
            	XSI_NS
            );
            
            List<String> recLang = SolrSearchTools.getMetadataValues(rec, "MD_LANGUAGE");
            if (recLang.size() < 1) {
            	recLang = SolrSearchTools.getMetadataValues(doc, "MD_LANGUAGE");
            }
            if (recLang.size() > 0) {
            	String recLangVal = recLang.get(0); // What if more than one language
            	eleRec.setAttribute("lang", recLangVal, Namespace.XML_NAMESPACE);
            }
            
            String attrRecID = docID + "/" + (i + 1);
            eleRec.setAttribute("id", attrRecID);
        	
        	String recID = "no.samla." + docID + "." + (i + 1);
        	Element eleId = new Element("identifier", nsDc);
        	eleId.addContent(recID);
        	eleRec.addContent(eleId);
        	
        	String purl = DataManager
        		.getInstance()
        		.getConfiguration()
        		.getOaiIdentifier()
        		.get("repositoryIdentifier") + docID;
        	Element elePurl = new Element("purl", nsIsebel);
        	elePurl.addContent(purl);
        	eleRec.addContent(elePurl);
        	
        	List<String> titles = SolrSearchTools.getMetadataValues(rec, "MD_TITLE");
        	for (String title : titles) {
        		Element eleTitle = new Element("title", nsDc);
        		eleTitle.setText(title);
        		eleRec.addContent(eleTitle);
        	}
        	
        	List<String> genres = SolrSearchTools.getMetadataValues(rec, "MD_Genre");
        	for (String genre : genres) {
        		String genreVal = ViewerResourceBundle.getTranslation(genre, Locale.ENGLISH);
        		Element eleGenre = new Element("type", nsDc);
        		eleGenre.setText(genreVal);
        		eleRec.addContent(eleGenre);
        	}
        	
        	SolrDocumentList taleTypesList = solr.search(
        		"+IDDOC_OWNER:\"" 
        			+ idRec
                	+ "\" +LABEL:\"MD_CatalogClassification_display\"", 
                filterQuerySuffix
            );
        	if (taleTypesList.size() < 1) {
        		taleTypesList = solr.search(
        			"+IDDOC_OWNER:\"" 
        				+ idDoc 
        				+ "\" +LABEL:\"MD_CatalogClassification_display\"", 
        			filterQuerySuffix);
        	}
        	if (taleTypesList.size() > 0) {
        		Element eleTaleTypes = new Element("taleTypes", nsIsebel);
        		for (SolrDocument taleType : taleTypesList) {
        			Element eleTaleType = new Element("taleType", nsIsebel);
        			String refCode = SolrSearchTools
        				.getMetadataValues(taleType, "NORM_RefCode")
        				.get(0);
        			eleTaleType.setAttribute("number", refCode);
        			List<String> names = SolrSearchTools
        				.getMetadataValues(taleType, "NORM_Name_LANG_EN");
        			if (names.size() < 1) {
        				names = SolrSearchTools
        					.getMetadataValues(taleType, "NORM_Name");
        			}
        			String name = names.get(0);
        			eleTaleType.setAttribute("title", name);
        			eleTaleTypes.addContent(eleTaleType);
        		}
        		eleRec.addContent(eleTaleTypes);
        	}

        	List<String> places = SolrSearchTools.getMetadataValues(rec, "MD_Place");
        	List<String> keywords = SolrSearchTools.getMetadataValues(rec, "MD_Subject");
        	List<String> taleTypes = SolrSearchTools.getMetadataValues(rec, "MD_CatalogClassification");
        	
        	List<String> refCodes = SolrSearchTools.getMetadataValues(rec, "NORM_RefCode");
        	List<String> normNames = SolrSearchTools.getMetadataValues(rec, "NORM_Name");
        	List<String> latitudes = SolrSearchTools.getMetadataValues(rec, "NORM_LATITUDE");
        	List<String> longitudes = SolrSearchTools.getMetadataValues(rec, "NORM_LONGITUDE");
        	
        	int refCodeOffsetTaleTypes = refCodes.size() - taleTypes.size();
        	int refCodeOffsetKeywords = refCodeOffsetTaleTypes - keywords.size();
        	int nameOffsetPlaces = 0;
        	int nameOffsetKeywords = nameOffsetPlaces + places.size();
        	int nameOffsetTaleTypes = nameOffsetKeywords + keywords.size();
        	
        	/*
        	Element eleTaleTypes = new Element("taleTypes", nsIsebel);
        	for (int j = 0; j < taleTypes.size(); j++) {
        		Element eleTaleType = new Element("taleType", nsIsebel);
        		eleTaleType.setAttribute("number", refCodes.get(j + refCodeOffsetTaleTypes));
        		eleTaleType.setAttribute("title", normNames.get(j + nameOffsetTaleTypes));
        		eleTaleTypes.addContent(eleTaleType);
        	}
        	*/
        	
        	Element elePlaces = new Element("places", nsIsebel);
        	
        	for (int j = 0; j < places.size(); j++) {
    			Element elePlace = new Element("place", nsIsebel);
    			
    			Element placeTitle = new Element("title", nsDc);
    			placeTitle.setText(normNames.get(j + nameOffsetPlaces));
    			elePlace.addContent(placeTitle);
    			
    			Element point = new Element("point", nsIsebel);
    			Element latitude = new Element("pointLatitude", nsDataCite);
    			latitude.setText(latitudes.get(j));
    			point.addContent(latitude);
    			Element longitude = new Element("pointLongitude", nsDataCite);
    			longitude.setText(longitudes.get(j));
    			point.addContent(longitude);
    			elePlace.addContent(point);
    			
    			Element placeRole = new Element("role", nsIsebel);
    			placeRole.setText("recording"); // this is almost always the case
    			elePlace.addContent(placeRole);
    			
    			elePlaces.addContent(elePlace);
        	}
        	
        	Element eleKeywords = new Element("keywords", nsIsebel);
        	for (int j = 0; j < keywords.size(); j++) {
        		Element eleKeyword = new Element("keyword", nsIsebel);
        		eleKeyword.setAttribute("id", refCodes.get(j + refCodeOffsetKeywords));
        		eleKeyword.setText(normNames.get(j + nameOffsetKeywords));
        		eleKeywords.addContent(eleKeyword);
        	}

        	List<String> placesDoc = SolrSearchTools.getMetadataValues(doc, "MD_Place");
        	List<String> keywordsDoc = SolrSearchTools.getMetadataValues(doc, "MD_Subject");
        	List<String> taleTypesDoc = SolrSearchTools.getMetadataValues(doc, "MD_CatalogClassification");
        	
        	List<String> refCodesDoc = SolrSearchTools.getMetadataValues(doc, "NORM_RefCode");
        	List<String> normNamesDoc = SolrSearchTools.getMetadataValues(doc, "NORM_Name");
        	List<String> latitudesDoc = SolrSearchTools.getMetadataValues(doc, "NORM_LATITUDE");
        	List<String> longitudesDoc = SolrSearchTools.getMetadataValues(doc, "NORM_LONGITUDE");
        	
        	refCodeOffsetTaleTypes = refCodesDoc.size() - taleTypesDoc.size();
        	refCodeOffsetKeywords = refCodeOffsetTaleTypes - keywordsDoc.size();
        	nameOffsetPlaces = 0;
        	nameOffsetKeywords = nameOffsetPlaces + placesDoc.size();
        	nameOffsetTaleTypes = nameOffsetKeywords + keywordsDoc.size();
        	
        	if (places.size() < 1) {
        		for (int j = 0; j < placesDoc.size(); j++) {
        			Element elePlace = new Element("place", nsIsebel);
        			
        			Element placeTitle = new Element("title", nsDc);
        			placeTitle.setText(normNamesDoc.get(j + nameOffsetPlaces));
        			elePlace.addContent(placeTitle);
        			
        			Element point = new Element("point", nsIsebel);
        			Element latitude = new Element("pointLatitude", nsDataCite);
        			latitude.setText(latitudesDoc.get(j));
        			point.addContent(latitude);
        			Element longitude = new Element("pointLongitude", nsDataCite);
        			longitude.setText(longitudesDoc.get(j));
        			point.addContent(longitude);
        			elePlace.addContent(point);
        			
        			Element placeRole = new Element("role", nsIsebel);
        			placeRole.setText("recording"); // this is almost always the case
        			elePlace.addContent(placeRole);
        			
        			elePlaces.addContent(elePlace);
        		}
        	}
        	
        	/*
	        if (taleTypes.size() > 0) {
	        	eleRec.addContent(eleTaleTypes);
	        }
	        */
	        
	        String fulltext = getFulltext(idRec, filterQuerySuffix);
	        if (fulltext.length() > 0) {
		        Element contents = new Element("contents", nsIsebel);
		        Element content = new Element("content", nsIsebel);
		        content.setAttribute("lang", recLang.get(0), Namespace.XML_NAMESPACE);
		        content.addContent(fulltext);
		        contents.addContent(content);
		        eleRec.addContent(contents);
	        }
	        
	        if (places.size() > 0 || placesDoc.size() > 0) {
	        	eleRec.addContent(elePlaces);
	        }
        	
	        List<Element> people = new ArrayList<>();
	        people.addAll(getPeopleElements(
	        	"collector", 
	        	"MD_Collector", 
	        	idRec, 
	       		idDoc, 
	        	filterQuerySuffix, 
	        	nsIsebel
	        ));
	        people.addAll(getPeopleElements(
	        	"informant",
	        	"MD_Informant",
	        	idRec,
	        	idDoc,
	        	filterQuerySuffix,
	        	nsIsebel
	        ));
        	
	        if (people.size() > 0) {
	        	Element elePersons = new Element("persons", nsIsebel);
	        	for (Element person : people) {
	        		elePersons.addContent(person);
	        	}
	        	eleRec.addContent(elePersons);
	        }
	        
	        List<String> dates = SolrSearchTools.getMetadataValues(rec, "MD_Date");
	        if (dates.size() < 1) {
	        	dates = SolrSearchTools.getMetadataValues(doc, "MD_Date");
	        }
	        if (dates.size() > 0) {
	        	Element eleEvents = new Element("events", nsIsebel);
	        	for (String date : dates) {
	        		Element eleEvent = new Element("event", nsIsebel);
	        		Element eleDate = new Element("date", nsIsebel);
	        		eleDate.setText(date);
	        		eleEvent.addContent(eleDate);
	        		Element eleRole = new Element("role", nsIsebel);
	        		eleRole.setText("recording");
	        		eleEvent.addContent(eleRole);
	        		eleEvents.addContent(eleEvent);
	        	}
	        	eleRec.addContent(eleEvents);
	        }
	        
	        if (keywords.size() > 0) {
	        	eleRec.addContent(eleKeywords);
	        }
            
        	metadata.addContent(eleRec);
        }

        // Configured fields
        List<io.goobi.viewer.connector.oai.model.metadata.Metadata> metadataList = DataManager
        	.getInstance()
        	.getConfiguration()
        	.getMetadataConfiguration(Metadata.ISEBEL.getMetadataPrefix(), docstruct);
        if (metadataList != null && !metadataList.isEmpty()) {
            for (io.goobi.viewer.connector.oai.model.metadata.Metadata md : metadataList) {
                boolean restrictedContent = false;
                List<String> finishedValues = new ArrayList<>();

                // Alternative 1: get value from source
                if ("#AUTO#".equals(md.getMasterValue())) {
                    // #AUTO# means a hardcoded value is added
                    String val = "";
                    switch (md.getLabel()) {
                        case "identifier":
                            if (StringUtils.isNotEmpty((String) doc.getFieldValue(SolrConstants.URN))) {
                                val = DataManager.getInstance().getConfiguration().getUrnResolverUrl()
                                        + (String) doc.getFieldValue(SolrConstants.URN);
                            } else if (StringUtils.isNotEmpty((String) doc.getFieldValue(SolrConstants.PI))) {
                                val = DataManager.getInstance().getConfiguration().getPiResolverUrl() 
                                		+ (String) doc.getFieldValue(SolrConstants.PI);
                            } else if (StringUtils.isNotEmpty((String) doc.getFieldValue(SolrConstants.PI_TOPSTRUCT))) {
                                val = DataManager.getInstance().getConfiguration().getPiResolverUrl()
                                        + (String) doc.getFieldValue(SolrConstants.PI_TOPSTRUCT);
                            }
                            finishedValues.add(val);
                            break;
                        case "rights":
                            if (openAccess) {
                                val = ACCESSCONDITION_OPENACCESS;
                            } else {
                                for (String accessCondition : accessConditions) {
                                    val = DataManager.getInstance()
                                    	.getConfiguration()
                                        .getAccessConditionMappingForMetadataFormat(
                                        	Metadata.ISEBEL.getMetadataPrefix(), 
                                        	accessCondition
                                        );
                                }
                                if (StringUtils.isEmpty(val)) {
                                    val = ACCESSCONDITION_CLOSEDACCESS;
                                }
                            }
                            finishedValues.add(val);
                            break;
                        case "source":
                            if (topstructDoc == null) {
                                logger.warn(
                                	"No topstruct found for IDDOC:{} - is this a page document? Please check the base query.",
                                    doc.getFieldValue(SolrConstants.IDDOC)
                                );
                                continue;
                            }
                            eleDoc.addContent(generateDcSource(doc, topstructDoc, anchorDoc, nsDc));
                            break;
                        case "fulltext":
                            if (topstructDoc == null) {
                                logger.warn(
                                	"No topstruct found for IDDOC:{} - is this a page document? Please check the base query.",
                                    doc.getFieldValue(SolrConstants.IDDOC)
                                );
                                continue;
                            }
                            for (Element eleOaiFullText : generateFulltextUrls(
                            	(String) topstructDoc.getFieldValue(SolrConstants.PI_TOPSTRUCT),
                            	nsDc
                            )) {
                                eleDoc.addContent(eleOaiFullText);
                            }
                            break;
                        default:
                            val = "No automatic configuration possible for field: " + md.getLabel();
                            finishedValues.add(val);
                            break;
                    }
                } else if ("#TOC#".equals(md.getMasterValue())) {
                    // Generated TOC as plain text
                    String url = DataManager.getInstance().getConfiguration().getRestApiUrl() 
                    	+ "records/"
                        + (String) doc.getFieldValue(SolrConstants.PI) 
                        + "/toc/";
                    try {
                        String val = null;
                        try {
                            val = NetTools.getWebContentGET(url);
                        } catch (HTTPException e) {
                            /* If the API end point was not found, 
                             * try the fallback, 
                             * otherwise re-throw the exception
                             */
                            if (e.getCode() != 404) {
                                throw e;
                            }
                        }
                        if (StringUtils.isEmpty(val)) {
                            // Old API fallback
                            url = DataManager.getInstance().getConfiguration().getRestApiUrl() 
                            	+ "records/toc/"
                                + (String) doc.getFieldValue(SolrConstants.PI) 
                                + "/";
                            val = NetTools.getWebContentGET(url);
                        }

                        if (StringUtils.isNotEmpty(val)) {
                            finishedValues.add(url);
                        }
                    } catch (IOException e) {
                        logger.error(
                        	"Could not retrieve TOC for '{}': {}", 
                        	doc.getFieldValue(SolrConstants.PI), 
                        	e.getMessage()
                        );
                    } catch (HTTPException e) {
                        logger.error(
                        	"Could not retrieve TOC for '{}' (code {}) {}", 
                        	doc.getFieldValue(SolrConstants.PI), 
                        	e.getCode(), 
                        	url
                        );
                    }
                } else if (!md.getParams().isEmpty()) {
                    // Parameter configuration
                	// Her skjer konfigurasjon for ikke #AUTO# eller #TOC#
                    String firstField = md.getParams().get(0).getKey();
                    int numValues = SolrSearchTools.getMetadataValues(doc, firstField).size();
                    // for each instance of the first field value
                    for (int i = 0; i < numValues; ++i) {
                        String val = md.getMasterValue();
                        int paramIndex = 0;
                        // for each parameter
                        for (MetadataParameter param : md.getParams()) {
                            if (SolrConstants.THUMBNAIL.equals(param.getKey())) {
                                restrictedContent = true;
                            }
                            String paramVal = "";
                            List<String> values = SolrSearchTools.getMetadataValues(doc, param.getKey());
                            if (values.isEmpty() && !param.isDontUseTopstructValue()) {
                                values = SolrSearchTools.getMetadataValues(topstructDoc, param.getKey());
                            }
                            if (!values.isEmpty()) {
                                paramVal = values.size() > i ? values.get(i) : "";
                                if (StringUtils.isNotEmpty(paramVal)) {
                                    if (MetadataParameterType.TRANSLATEDFIELD.equals(param.getType())) {
                                        paramVal = ViewerResourceBundle.getTranslation(paramVal, null);
                                    }
                                    if (StringUtils.isNotEmpty(param.getPrefix())) {
                                        String prefix = ViewerResourceBundle.getTranslation(
                                        	param.getPrefix(), 
                                        	null
                                        );
                                        paramVal = prefix + paramVal;
                                    }
                                    if (StringUtils.isNotEmpty(param.getSuffix())) {
                                        String suffix = ViewerResourceBundle.getTranslation(
                                        	param.getSuffix(), 
                                        	null
                                        );
                                        paramVal += suffix;
                                    }
                                }
                            }
                            val = val.replace("{" + paramIndex + '}', paramVal);
                            paramIndex++;
                        }
                        if (openAccess || !restrictedContent) {
                            finishedValues.add(val);
                        }
                    }
                } else if (StringUtils.isNotEmpty(md.getMasterValue())) {
                    // Default value
                    String val = md.getMasterValue();
                    if ("title".equals(md.getLabel()) 
                    	&& isWork 
                    	&& doc.getFieldValue(SolrConstants.IDDOC_PARENT) != null) {
                        // If this is a volume, add anchor title in front
                        String iddocParent = (String) doc.getFieldValue(SolrConstants.IDDOC_PARENT);
                        String anchorTitle = anchorTitles.get(iddocParent);
                        if (anchorTitle == null) {
                            anchorTitle = getAnchorTitle(iddocParent, filterQuerySuffix);
                            if (anchorTitle != null) {
                                val = anchorTitle + "; " + val;
                                anchorTitles.put(iddocParent, anchorTitle);
                            }
                        }
                    }
                    finishedValues.add(val);
                }

                // Add all constructed values for the current field to XML
                for (String val : finishedValues) {
                    Element eleField = new Element(md.getLabel(), nsDc);
                    eleField.setText(val);
                    eleDoc.addContent(eleField);
                }
            }
        }

        metadata.addContent(eleDoc);
        eleRecord.addContent(metadata);

        return eleRecord;

    }
    
    private List<Element> getPeopleElements(
    	String role, 
    	String label, 
    	String recID, 
    	String docID,
    	String filterQuerySuffix,
    	Namespace ns
    ) throws IOException, SolrServerException {
    	List<Element> result = new ArrayList<>();
    	SolrDocumentList people = solr.search(
            "+IDDOC_OWNER:\"" + recID + "\" +LABEL:\"" + label + "\"", 
            filterQuerySuffix
        );
    	if (people.size() < 1) {
    		people = solr.search(
    			"+IDDOC_OWNER:\"" + docID + "\" +LABEL:\"" + label + "\"", 
    			filterQuerySuffix
    		);
    	}
    	for (SolrDocument person : people) {
    		Element elePerson = new Element("person", ns);
    		
    		Element personName = new Element("name", ns);
    		personName.setText(
    			SolrSearchTools.getMetadataValues(person, "MD_DISPLAYFORM").get(0)
    		);
    		elePerson.addContent(personName);
    		
    		Element personRole = new Element("role", ns);
    		personRole.setText(role);
    		elePerson.addContent(personRole);
    		
    		result.add(elePerson);
    	}
    	
    	return result;
    }

    /**
     * <p>
     * getAnchorTitle.
     * </p>
     *
     * @param iddocParent
     * @param filterQuerySuffix Filter query suffix for the client's session
     * @return a {@link java.lang.String} object.
     */
    protected String getAnchorTitle(String iddocParent, String filterQuerySuffix) {
        try {
            logger.trace("anchor title query: {}:{}", SolrConstants.IDDOC, iddocParent);
            SolrDocumentList hits = solr.search(
            	"+" + SolrConstants.IDDOC + ":" + iddocParent, 
            	filterQuerySuffix
            );
            if (hits != null && !hits.isEmpty()) {
                return (String) hits.get(0).getFirstValue(SolrConstants.TITLE);
            }
        } catch (IOException | SolrServerException e) {
            logger.error(e.getMessage(), e);
        }

        return null;
    }

    /**
     * <p>
     * generateDcSource.
     * </p>
     *
     * @param doc a {@link org.apache.solr.common.SolrDocument} object.
     * @param topstructDoc a {@link org.apache.solr.common.SolrDocument} object.
     * @param anchorDoc a {@link org.apache.solr.common.SolrDocument} object.
     * @param namespace a {@link org.jdom2.Namespace} object.
     * @return a {@link org.jdom2.Element} object.
     * @should throw IllegalArgumentException if topstructDoc null
     * @should create element correctly
     */
    static Element generateDcSource(
    	SolrDocument doc, 
    	SolrDocument topstructDoc, 
    	SolrDocument anchorDoc, 
    	Namespace namespace
    ) {
        if (topstructDoc == null) {
            throw new IllegalArgumentException("topstructDoc may not be null");
        }

        StringBuilder sbSourceCreators = new StringBuilder();
        if (doc != null && doc.getFieldValues(MD_CREATOR) != null) {
            for (Object fieldValue : doc.getFieldValues(MD_CREATOR)) {
                if (sbSourceCreators.length() > 0) {
                    sbSourceCreators.append(", ");
                }
                sbSourceCreators.append((String) fieldValue);
            }
        } else if (topstructDoc.getFieldValues(MD_CREATOR) != null) {
            for (Object fieldValue : topstructDoc.getFieldValues(MD_CREATOR)) {
                if (sbSourceCreators.length() > 0) {
                    sbSourceCreators.append(", ");
                }
                sbSourceCreators.append((String) fieldValue);
            }
        }
        if (sbSourceCreators.length() == 0) {
            sbSourceCreators.append('-');
        }

        StringBuilder sbSourceTitle = new StringBuilder();
        if (doc != null && doc.getFirstValue(SolrConstants.TITLE) != null) {
            sbSourceTitle.append((String) doc.getFirstValue(SolrConstants.TITLE));
        }
        if (anchorDoc != null && anchorDoc.getFirstValue(SolrConstants.TITLE) != null) {
            if (sbSourceTitle.length() > 0) {
                sbSourceTitle.append("; ");
            }
            sbSourceTitle.append((String) anchorDoc.getFirstValue(SolrConstants.TITLE));
        }
        if (sbSourceTitle.length() == 0) {
            sbSourceTitle.append('-');
        }
        if (topstructDoc != doc && topstructDoc.getFirstValue(SolrConstants.TITLE) != null) {
            if (sbSourceTitle.length() > 0) {
                sbSourceTitle.append("; ");
            }
            sbSourceTitle.append((String) topstructDoc.getFirstValue(SolrConstants.TITLE));
        }

        // Publisher info
        String sourceYearpublish = (String) topstructDoc.getFirstValue(MD_YEARPUBLISH);
        if (sourceYearpublish == null) {
            sourceYearpublish = "-";
        }
        String sourcePlacepublish = (String) topstructDoc.getFirstValue("MD_PLACEPUBLISH");
        if (sourcePlacepublish == null) {
            sourcePlacepublish = "-";
        }
        String sourcePublisher = (String) topstructDoc.getFirstValue(MD_PUBLISHER);
        if (sourcePublisher == null) {
            sourcePublisher = "-";
        }

        // Page range
        String orderLabelFirst = null;
        String orderLabelLast = null;
        if (doc != null) {
            orderLabelFirst = (String) doc.getFirstValue("ORDERLABELFIRST");
            orderLabelLast = (String) doc.getFirstValue("ORDERLABELLAST");
        }

        StringBuilder sbSourceString = new StringBuilder();
        sbSourceString
        	.append(sbSourceCreators.toString())
        	.append(": ")
        	.append(sbSourceTitle.toString());

        if (doc == topstructDoc || doc == anchorDoc) {
            // Only top level docs should display publisher information
            sbSourceString
            	.append(", ")
            	.append(sourcePlacepublish)
            	.append(": ")
            	.append(sourcePublisher)
            	.append(' ')
            	.append(sourceYearpublish);
        } else if (
        	orderLabelFirst != null 
        	&& orderLabelLast != null 
        	&& !"-".equals(orderLabelFirst.trim()) 
        	&& !"-".equals(orderLabelLast.trim())
        ) {
            // Add page range for lower level docstructs, if available
            sbSourceString
            	.append(", P ")
            	.append(orderLabelFirst)
            	.append(" - ")
            	.append(orderLabelLast);
        }

        sbSourceString.append('.');

        Element eleDcSource = new Element("source", namespace);
        eleDcSource.setText(sbSourceString.toString());

        return eleDcSource;
    }

    /**
     * <p>
     * generateFulltextUrls.
     * </p>
     *
     * @param namespace a {@link org.jdom2.Namespace} object.
     * @throws org.apache.solr.client.solrj.SolrServerException
     * @param pi a {@link java.lang.String} object.
     * @return a {@link java.util.List} object.
     * @throws IOException
     */
    protected static List<Element> generateFulltextUrls(String pi, Namespace namespace)
            throws SolrServerException, IOException {
        if (pi == null) {
            throw new IllegalArgumentException("pi may not be null");
        }

        Map<Integer, String> fulltextFilePaths = 
        	DataManager.getInstance().getSearchIndex().getFulltextFileNames(pi);
        if (fulltextFilePaths.isEmpty()) {
            return Collections.emptyList();
        }

        List<Integer> orderedPageList = new ArrayList<>(fulltextFilePaths.keySet());
        Collections.sort(orderedPageList);

        List<Element> ret = new ArrayList<>(orderedPageList.size());
        for (int i : orderedPageList) {
            String filePath = fulltextFilePaths.get(i); 
            // file path relative to the data repository (Goobi viewer 3.2 and later)
            
            if (StringUtils.isEmpty(filePath)) {
                continue;
            }
            String fileName = FileTools.getFilenameFromPathString(filePath); // pure file name
            String url = null;
            switch (FilenameUtils.getExtension(fileName).toLowerCase()) {
                case "xml":
                    url = io.goobi.viewer.controller.DataManager.getInstance()
                    	.getRestApiManager()
                        .getContentApiManager()
                        .map(urls -> urls.path(RECORDS_FILES, RECORDS_FILES_ALTO)
                        	.params(pi, fileName)
                            .build())
                        .orElse("");
                    break;
                case "txt":
                    url = io.goobi.viewer.controller.DataManager.getInstance()
                            .getRestApiManager()
                            .getContentApiManager()
                            .map(urls -> urls.path(RECORDS_FILES, RECORDS_FILES_PLAINTEXT)
                            	.params(pi, fileName)
                                .build())
                            .orElse("");
                    break;
                default:
                    break;

            }

            logger.trace(url);

            if (url != null) {
                Element eleDcFulltext = new Element("source", namespace);
                eleDcFulltext.setText(url);
                ret.add(eleDcFulltext);
            }
        }

        return ret;
    }

    /* (non-Javadoc)
     * @see io.goobi.viewer.connector.oai.model.formats.AbstractFormat#getTotalHits(java.util.Map, java.util.String, java.util.String)
     */
    /** {@inheritDoc} */
    @Override
    public long getTotalHits(
    	Map<String, String> params, 
    	String versionDiscriminatorField, 
    	String filterQuerySuffix
    ) throws IOException, SolrServerException {
        String additionalQuery = "";
        if (!Verb.LISTIDENTIFIERS.getTitle().equals(params.get("verb"))) {
            additionalQuery += SolrSearchTools.getAdditionalDocstructsQuerySuffix(
            	DataManager.getInstance().getConfiguration().getAdditionalDocstructTypes()
            );
        }
        // Query Solr index for the total hits number

        if (StringUtils.isNotEmpty(versionDiscriminatorField)) {
            // Query Solr index for the count of the discriminator field
            QueryResponse qr = solr.search(
            	params.get("from"), 
            	params.get("until"), 
            	params.get("set"), 
            	params.get("metadataPrefix"), 
            	0, 
            	0, 
            	false,
                additionalQuery + " AND " + versionDiscriminatorField + ":*", 
                filterQuerySuffix, 
                null,
                Collections.singletonList(versionDiscriminatorField)
            );
            return SolrSearchTools.getFieldCount(qr, versionDiscriminatorField);
        }
        return solr.getTotalHitNumber(params, false, additionalQuery, null, filterQuerySuffix);
    }

    public String getFulltext(
    	String idRec, 
    	String filterQuerySuffix
    ) throws IOException, SolrServerException {
    	String text = "";
    	
    	SolrDocumentList pages = solr.search(
        	"+IDDOC_OWNER:\"" + idRec + "\" +DOCTYPE:\"PAGE\"", 
            filterQuerySuffix
        );
    	
    	for (SolrDocument page : pages) {
    		List<String> filePaths = SolrSearchTools.getMetadataValues(page, "FILENAME_ALTO");
    		if (filePaths.size() < 1) {
    			continue;
    		} else {
        		Path path = Paths.get("/opt/digiverso/viewer/" + filePaths.get(0));
        		text += "\n" + ALTOTools.getFulltext(path, StringTools.DEFAULT_ENCODING) + "\n";
    		}
    	}
    	
    	return text;
    }
}