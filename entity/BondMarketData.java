package entity;

import java.util.List;

public class BondMarketData {

    private String bestOfr;
    private List<String> latestOfrs;
    private List<String> latestBids;

    public String getBestOfr() {
        return bestOfr;
    }

    public void setBestOfr(String bestOfr) {
        this.bestOfr = bestOfr;
    }

    public List<String> getLatestOfrs() {
        return latestOfrs;
    }

    public void setLatestOfrs(List<String> latestOfrs) {
        this.latestOfrs = latestOfrs;
    }

    public List<String> getLatestBids() {
        return latestBids;
    }

    public void setLatestBids(List<String> latestBids) {
        this.latestBids = latestBids;
    }
}
