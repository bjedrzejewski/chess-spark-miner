import java.io.Serializable;

public class GameKey implements Serializable{
    
    private final String opening;
    private final String eco;
    private final String tempo;
    private final String avgEloClass;
    private final String ratingDiffClass;

    public GameKey(String opening, String eco, String tempo, String avgEloClass, String ratingDiffClass) {
        this.opening = opening;
        this.eco = eco;
        this.tempo = tempo;
        this.avgEloClass = avgEloClass;
        this.ratingDiffClass = ratingDiffClass;
    }

    public String getOpening() {
        return opening;
    }

    public String getEco() {
        return eco;
    }

    public String getTempo() {
        return tempo;
    }

    public String getAvgEloClass() {
        return avgEloClass;
    }

    public String getRatingDiffClass() {
        return ratingDiffClass;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GameKey gameKey = (GameKey) o;

        if (!opening.equals(gameKey.opening)) return false;
        if (!eco.equals(gameKey.eco)) return false;
        if (!tempo.equals(gameKey.tempo)) return false;
        if (!avgEloClass.equals(gameKey.avgEloClass)) return false;
        return ratingDiffClass.equals(gameKey.ratingDiffClass);
    }

    @Override
    public int hashCode() {
        int result = opening.hashCode();
        result = 31 * result + eco.hashCode();
        result = 31 * result + tempo.hashCode();
        result = 31 * result + avgEloClass.hashCode();
        result = 31 * result + ratingDiffClass.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return opening + "|" + eco + "|" + tempo + "|" + avgEloClass + "|" + ratingDiffClass;
    }
}
