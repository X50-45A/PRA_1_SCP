import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

public class MapConc extends Map {
    long wordsEmitted = 0;
    public MapConc(MapReduce mapr, Split split) {
        super(mapr,split);
    }
    public MapConc(MapReduce mapr) {
        super(mapr);
    }

    public long getWordsEmitted() {
        return wordsEmitted;
    }
    @Override
    public void EmitResult(String key, int value) {
        GetOutput()
                .computeIfAbsent(key, k -> new ArrayList<>())
                .add(value);

        wordsEmitted++;
    }
}
