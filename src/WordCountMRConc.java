import java.util.Collection;


public class WordCountMRConc extends MapReduceConc
{
    public WordCountMRConc(String input,String output, int nreducers, int nthreads)
    {
        SetInputPath(input);
        SetOutputFiles(output);
        SetReducers(nreducers);
        SetThreads(nthreads);
    }

    // Word Count Map.
    @Override
    public Error Map(Map map, MapInputTuple tuple)
    {
        String value = tuple.getValue();

        if (MapReduce.DEBUG) System.err.println("DEBUG::MapWordCount procesing tuple " + tuple.getKey() + " -> "+ tuple.getValue());

        // Emit map result (word,'1').
        for (String word : value.toLowerCase().split("[^\\p{L}]+")) {
            if (!word.isEmpty())
                map.EmitResult(word, 1);
        }

        return(Error.COk);
    }


    // Word Count Reduce.
    @Override
    public Error Reduce(Reduce reduce, String key, Collection<Integer> values)
    {
        int totalCount=0;

        if (MapReduce.DEBUG) System.err.print("DEBUG::ReduceWordCount key " + key +"s -> ");

        // Procesar todos los valores para esta clave.
        for(int number : values)
        {
            if (MapReduce.DEBUG) System.err.print(" " + number);
            totalCount +=  number;
        }

        if (MapReduce.DEBUG) System.err.println(" ==> " + totalCount);

        reduce.EmitResult(key, totalCount);

        return(Error.COk);
    }

}