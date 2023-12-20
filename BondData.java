package entity;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.support.SimpleFunction;
import com.googlecode.cqengine.query.QueryFactory;
import org.apache.commons.lang3.StringUtils;

import static com.googlecode.cqengine.query.QueryFactory.attribute;
import static com.googlecode.cqengine.query.QueryFactory.nullableAttribute;


public class BondData {

    private String contributorID = StringUtils.EMPTY;
    private String bondName = StringUtils.EMPTY;
    private Float bidPx = 0f;
    private Float offerPx = 0f;
    private String multiBidVolume = StringUtils.EMPTY;
    private Float askVolume = 0f;
    private String securityID = StringUtils.EMPTY;
    private String displayListedMarket = StringUtils.EMPTY;
    private String marketDataTime =StringUtils.EMPTY;
    private Integer relatedID;

    public BondData() {
    }

    public BondData(Float bidPx) {
        this.bidPx = bidPx;
    }

    public BondData(String contributorID, String bondName, Float bidPx, Float offerPx, String multiBidVolume,
                             Float askVolume, String securityID, String displayListedMarket, String marketDataTime) {
        this.contributorID = contributorID;
        this.bondName = bondName;
        this.bidPx = bidPx;
        this.offerPx = offerPx;
        this.multiBidVolume = multiBidVolume;
        this.askVolume = askVolume;
        this.securityID = securityID;
        this.displayListedMarket = displayListedMarket;
        this.marketDataTime = marketDataTime;
    }

    public BondData(String contributorID, String bondName, Float bidPx, Float offerPx, String multiBidVolume, Float askVolume, String securityID, String displayListedMarket, String marketDataTime, Integer relatedID) {
        this.contributorID = contributorID;
        this.bondName = bondName;
        this.bidPx = bidPx;
        this.offerPx = offerPx;
        this.multiBidVolume = multiBidVolume;
        this.askVolume = askVolume;
        this.securityID = securityID;
        this.displayListedMarket = displayListedMarket;
        this.marketDataTime = marketDataTime;
        this.relatedID = relatedID;
    }

    public static final Attribute<BondData, String> CONTRIBUTOR_ID = nullableAttribute("contributorID", BondData::getContributorID);
    public static final Attribute<BondData, String> BOND_NAME = nullableAttribute("bondName", BondData::getBondName);
    public static final Attribute<BondData, Float> BID_PX = nullableAttribute("bidPx", BondData::getBidPx);
    public static final Attribute<BondData, Float> OFFER_PX = nullableAttribute("offerPx", BondData::getOfferPx);
    public static final Attribute<BondData, String> MULTIBID_VOLUMN = nullableAttribute("multiBidVolume", BondData::getMultiBidVolume);
    public static final Attribute<BondData, Float> ASK_VOLUME = nullableAttribute("askVolume", BondData::getAskVolume);
    public static final Attribute<BondData, String> SECURITY_ID = nullableAttribute("securityID", BondData::getSecurityID);
    public static final Attribute<BondData, String> DISPLAYLISTED_MARKET = nullableAttribute("displayListedMarket", BondData::getDisplayListedMarket);
    public static final Attribute<BondData, String> MARKETDATA_TIME = nullableAttribute("marketDataTime", BondData::getMarketDataTime);
    public static final Attribute<BondData, String> MARKETDATA_TIME_SORT = attribute("marketDataTime", BondData::getMarketDataTime);
    public static final Attribute<BondData, Integer> RELATED_ID = nullableAttribute("relatedID", BondData::getRelatedID);

    public String getMarketDataTime() {
        return marketDataTime;
    }

    public void setMarketDataTime(String marketDataTime) {
        this.marketDataTime = marketDataTime;
    }

    public String getContributorID() {
        return contributorID;
    }

    public void setContributorID(String contributorID) {
        this.contributorID = contributorID;
    }

    public String getBondName() {
        return bondName;
    }

    public void setBondName(String bondName) {
        this.bondName = bondName;
    }

    public Float getBidPx() {
        return bidPx;
    }

    public void setBidPx(Float bidPx) {
        this.bidPx = bidPx;
    }

    public Float getOfferPx() {
        return offerPx;
    }

    public void setOfferPx(Float offerPx) {
        this.offerPx = offerPx;
    }

    public String getMultiBidVolume() {
        return multiBidVolume;
    }

    public void setMultiBidVolume(String multiBidVolume) {
        this.multiBidVolume = multiBidVolume;
    }

    public Float getAskVolume() {
        return askVolume;
    }

    public void setAskVolume(Float askVolume) {
        this.askVolume = askVolume;
    }

    public String getSecurityID() {
        return securityID;
    }

    public void setSecurityID(String securityID) {
        this.securityID = securityID;
    }

    public String getDisplayListedMarket() {
        return displayListedMarket;
    }

    public void setDisplayListedMarket(String displayListedMarket) {
        this.displayListedMarket = displayListedMarket;
    }

    public Integer getRelatedID() {
        return relatedID;
    }

    public void setRelatedID(Integer relatedID) {
        this.relatedID = relatedID;
    }

    @Override
    public String toString() {
        return "SumscopeDataModel{" +
                "contributorID='" + contributorID + '\'' +
                ", bondName='" + bondName + '\'' +
                ", bidPx=" + bidPx +
                ", offerPx=" + offerPx +
                ", multiBidVolume=" + multiBidVolume +
                ", askVolume=" + askVolume +
                ", securityID='" + securityID + '\'' +
                ", displayListedMarket='" + displayListedMarket + '\'' +
                ", marketDataTime='" + marketDataTime + '\'' +
                '}';
    }
}
