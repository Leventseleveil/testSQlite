package entity;

import org.apache.commons.lang3.StringUtils;

public class BondMixedData {

    private String bondName = StringUtils.EMPTY;
    private Integer relatedID;
    private String testName = StringUtils.EMPTY;

    public BondMixedData() {
    }

    public BondMixedData(String bondName, Integer relatedID, String testName) {
        this.bondName = bondName;
        this.relatedID = relatedID;
        this.testName = testName;
    }

    public String getBondName() {
        return bondName;
    }

    public void setBondName(String bondName) {
        this.bondName = bondName;
    }

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
}
