/* ---------------------------------------------------------------
Práctica 1.
Código fuente: MapConc.java
Grau Informàtica
NIF [pendiente] y nombre completo autor1: Augustin-Dragos Belu.
NIF [pendiente] y nombre completo autor2: Abraham Ruíz Monsteirin.
--------------------------------------------------------------- */

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class MapConc extends Map {
    private final MapReduceConc mapReduceConc;
    private final SplitConc split;
    private final List<BlockingQueue<ShuffleTuple>> shuffleInputs;
    private long wordsEmitted = 0;

    public MapConc(MapReduceConc mapReduceConc, SplitConc split,
                   List<BlockingQueue<ShuffleTuple>> shuffleInputs) {
        super(mapReduceConc);
        this.mapReduceConc = mapReduceConc;
        this.split = split;
        this.shuffleInputs = shuffleInputs;
    }

    public long getWordsEmitted() {
        return wordsEmitted;
    }

    // Consume tuplas a medida que SplitConc las va leyendo. take() espera si
    // el productor aún no ha publicado la siguiente tupla.
    @Override
    public Error Run() {
        try {
            while (true) {
                MapInputTuple tuple = split.TakeInput();
                if (tuple.getKey() < 0) {
                    if (MapReduceConc.DEBUG) PrintOutputs();
                    // Shuffle ya recibió cada resultado directamente por su cola.
                    GetOutput().clear();
                    return Error.COk;
                }

                Error result = mapReduceConc.Map(this, tuple);
                if (result != Error.COk) {
                    return result;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Error.CError;
        }
    }

    @Override
    public void EmitResult(String key, int value) {

        GetOutput().computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        int reducerIndex = Math.floorMod(key.hashCode(), shuffleInputs.size());
        shuffleInputs.get(reducerIndex).add(new ShuffleTuple(key, value));
        wordsEmitted++;
    }
}
