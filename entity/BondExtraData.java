package entity;

import com.googlecode.cqengine.attribute.Attribute;
import org.apache.commons.lang3.StringUtils;

import static com.googlecode.cqengine.query.QueryFactory.nullableAttribute;

public class BondExtraData {

    private Integer relatedID;

    private String testName = StringUtils.EMPTY;

    private String testIntro = StringUtils.EMPTY;

    private String testCreateTime = StringUtils.EMPTY;

    private String testUpdateTime = StringUtils.EMPTY;

    public BondExtraData() {
    }

    public BondExtraData(Integer relatedID, String testName, String testIntro, String testCreateTime, String testUpdateTime) {
        this.relatedID = relatedID;
        this.testName = testName;
        this.testIntro = testIntro;
        this.testCreateTime = testCreateTime;
        this.testUpdateTime = testUpdateTime;
    }

    public static final Attribute<BondExtraData, Integer> RELATED_ID = nullableAttribute("relatedID", BondExtraData::getRelatedID);
    public static final Attribute<BondExtraData, String> TEST_NAME = nullableAttribute("testName", BondExtraData::getTestName);
    public static final Attribute<BondExtraData, String> TEST_INTRO = nullableAttribute("testIntro", BondExtraData::getTestIntro);
    public static final Attribute<BondExtraData, String> TEST_CREATE_TIME = nullableAttribute("testCreateTime", BondExtraData::getTestCreateTime);
    public static final Attribute<BondExtraData, String> TEST_UPDATE_TIME = nullableAttribute("testUpdateTime", BondExtraData::getTestUpdateTime);


    public Integer getRelatedID() {
        return relatedID;
    }

    public void setRelatedID(Integer relatedID) {
        this.relatedID = relatedID;
    }

    public String getTestName() {
        return testName;
    }

    public void setTestName(String testName) {
        this.testName = testName;
    }

    public String getTestIntro() {
        return testIntro;
    }

    public void setTestIntro(String testIntro) {
        this.testIntro = testIntro;
    }

    public String getTestCreateTime() {
        return testCreateTime;
    }

    public void setTestCreateTime(String testCreateTime) {
        this.testCreateTime = testCreateTime;
    }

    public String getTestUpdateTime() {
        return testUpdateTime;
    }

    public void setTestUpdateTime(String testUpdateTime) {
        this.testUpdateTime = testUpdateTime;
    }
}
