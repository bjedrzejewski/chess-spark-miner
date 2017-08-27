public class ScoreCount implements java.io.Serializable{

    private final double score;
    private final double count;
    private final double drawCount;
    private final double whiteWinCount;
    private final double blackWinCount;

    public ScoreCount(double score, double count) {
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
    }

    public ScoreCount(double score, double count, double drawCount, double whiteWinCount, double blackWinCount) {
        this.score = score;
        this.count = count;
        this.drawCount = drawCount;
        this.whiteWinCount = whiteWinCount;
        this.blackWinCount = blackWinCount;
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
                blackWinCount + a.blackWinCount);
    }
}