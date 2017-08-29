public class ScoreCount implements java.io.Serializable{

    private final double score;
    private final double count;
    private final double drawCount;
    private final double whiteWinCount;
    private final double blackWinCount;
    private final double sumWhiteElo;
    private final double sumBlackElo;

    public ScoreCount(double score, double count, double sumWhiteElo, double sumBlackElo) {
        if(score == 1) {
            whiteWinCount = 1;
            drawCount = 0;
            blackWinCount = 0;
        }
        else if(score == 0.5) {
            whiteWinCount = 0;
            drawCount = 1;
            blackWinCount = 0;
        }
        else {
            whiteWinCount = 0;
            drawCount = 0;
            blackWinCount = 1;
        }

        this.score = score;
        this.count = count;
        this.sumBlackElo = sumBlackElo;
        this.sumWhiteElo = sumWhiteElo;
    }

    public ScoreCount(double score, double count, double drawCount, double whiteWinCount, double blackWinCount, double sumWhiteElo, double sumBlackElo) {
        this.score = score;
        this.count = count;
        this.drawCount = drawCount;
        this.whiteWinCount = whiteWinCount;
        this.blackWinCount = blackWinCount;
        this.sumBlackElo = sumBlackElo;
        this.sumWhiteElo = sumWhiteElo;
    }

    public double getDrawCount() {
        return drawCount;
    }

    public double getWhiteWinCount() {
        return whiteWinCount;
    }

    public double getBlackWinCount() {
        return blackWinCount;
    }

    public double getAverageScore() {
        return score/count;
    }

    public double getScore() {
        return score;
    }

    public double getCount() {
        return count;
    }

    public ScoreCount add(ScoreCount a){
        return new ScoreCount(a.score + score,
                count + a.count,
                drawCount + a.drawCount,
                whiteWinCount + a.whiteWinCount,
                blackWinCount + a.blackWinCount,
                sumWhiteElo + a.sumWhiteElo,
                sumBlackElo +a.sumBlackElo);
    }

    public double getAverageWhiteElo() {
        return sumWhiteElo/count;
    }

    public double getAverageBlackElo() {
        return sumBlackElo/count;
    }

    @Override
    public String toString() {
        return getAverageScore() + "|" + score
                + "|" + count
                + "|" + drawCount
                + "|" + whiteWinCount
                + "|" + blackWinCount
                + "|" + getAverageWhiteElo()
                + "|" + getAverageBlackElo()
                + "|" + sumWhiteElo
                + "|" + sumBlackElo;
    }

    public static String getFileHeader() {
        return "averageScore|score|count|drawCount|whiteWinCount|blackWinCount|averageWhiteElo|averageBlackElo|sumWhiteElo|sumBlackElo";
    }
}