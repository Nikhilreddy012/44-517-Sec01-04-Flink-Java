package edu.nwmissouri.deccanchargers.pannala;

import java.util.ArrayList;

public class RankedPage {
    String name;
    Double rank;
    Integer vote;

    public RankedPage(String name, Double rank, Integer vote) {
        this.name = name;s
        this.rank = rank;
        this.vote = vote;
    }

    public RankedPage(String key, ArrayList<VotingPage> voters) {
    }
    
}
