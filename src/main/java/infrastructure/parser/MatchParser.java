package main.java.infrastructure.parser;

import main.java.domain.model.Match;

public class MatchParser 
{
    public static Match parse(String line) 
    {
        int commaIdx = line.indexOf(',');

        String left = line.substring(0, commaIdx).trim();
        String right = line.substring(commaIdx + 1).trim();

        int lastSpaceA = left.lastIndexOf(' ');
        int lastSpaceB = right.lastIndexOf(' ');

        String teamA = left.substring(0, lastSpaceA).trim();
        int scoreA = Integer.parseInt(left.substring(lastSpaceA + 1));

        String teamB = right.substring(0, lastSpaceB).trim();
        int scoreB = Integer.parseInt(right.substring(lastSpaceB + 1));

        return new Match(teamA, scoreA, teamB, scoreB);
    }
}