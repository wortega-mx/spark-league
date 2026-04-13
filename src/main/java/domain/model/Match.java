package main.java.domain.model;

public class Match 
{

    private String teamA;
    private int scoreA;
    private String teamB;
    private int scoreB;

    public Match() {}

    public Match(String teamA, int scoreA, String teamB, int scoreB) 
    {
        this.teamA = teamA;
        this.scoreA = scoreA;
        this.teamB = teamB;
        this.scoreB = scoreB;
    }

    public String getTeamA() { return teamA; }
    public int getScoreA() { return scoreA; }
    public String getTeamB() { return teamB; }
    public int getScoreB() { return scoreB; }
}