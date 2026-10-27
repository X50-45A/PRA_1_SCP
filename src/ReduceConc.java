import java.util.Collection;

// Reducer de la versión concurrente. Cada instancia pertenece a un único hilo.
class ReduceConc extends Reduce
{
    private long wordsEmitted = 0;

    public ReduceConc(MapReduce mapr, String outputPath)
    {
        super(mapr, outputPath);
    }

    public long getWordsEmitted()
    {
        return wordsEmitted;
    }

    @Override
    public void EmitResult(String key, int value)
    {
        OutputFile.write(key + " " + value + "\n");
        wordsEmitted++;
    }
}
