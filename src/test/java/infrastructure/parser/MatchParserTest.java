package test.java.infrastructure.parser;

import main.java.domain.model.Match;
import main.java.infrastructure.parser.MatchParser;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MatchParserTest 
{

    @Test
    void shouldParseValidMatchLine() 
    {
        String input = "Team A 3, Team B 2";

        Match result = MatchParser.parse(input);

        assertEquals("Team A", result.getTeamA());
        assertEquals(3, result.getScoreA());
        assertEquals("Team B", result.getTeamB());
        assertEquals(2, result.getScoreB());
    }

    @Test
    void shouldHandleMultiWordTeamNames() 
    {
        String input = "Real Madrid 5, FC Barcelona 4";

        Match result = MatchParser.parse(input);

        assertEquals("Real Madrid", result.getTeamA());
        assertEquals(5, result.getScoreA());
        assertEquals("FC Barcelona", result.getTeamB());
        assertEquals(4, result.getScoreB());
    }

    @Test
    void shouldTrimExtraSpaces() 
    {
        String input = "  Team A   1,   Team B   0  ";

        Match result = MatchParser.parse(input);

        assertEquals("Team A", result.getTeamA());
        assertEquals(1, result.getScoreA());
        assertEquals("Team B", result.getTeamB());
        assertEquals(0, result.getScoreB());
    }

    @Test
    void shouldHandleZeroScores() 
    {
        String input = "Team A 0, Team B 0";

        Match result = MatchParser.parse(input);

        assertEquals(0, result.getScoreA());
        assertEquals(0, result.getScoreB());
    }

    @Test
    void shouldThrowExceptionWhenNoComma() 
    {
        String input = "Team A 1 Team B 2";

        assertThrows(StringIndexOutOfBoundsException.class, () -> {
            MatchParser.parse(input);
        });
    }

    @Test
    void shouldThrowExceptionWhenScoreIsNotNumber() 
    {
        String input = "Team A X, Team B 2";

        assertThrows(NumberFormatException.class, () -> {
            MatchParser.parse(input);
        });
    }

    @Test
    void shouldThrowExceptionWhenMissingScore() 
    {
        String input = "Team A, Team B 2";

        assertThrows(Exception.class, () -> {
            MatchParser.parse(input);
        });
    }

    @Test
    void shouldThrowExceptionWithEmptyString() 
    {
        String input = "";

        assertThrows(Exception.class, () -> {
            MatchParser.parse(input);
        });
    }
}