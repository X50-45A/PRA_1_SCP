import java.io.*;
import java.util.Vector;

public class Split
{
    private Vector<MapInputTuple> Input = new Vector<MapInputTuple>();

    public Vector<MapInputTuple> getInput()
    {
        return(Input);
    }

    // Lee fichero de entrada (split) línea a línea y lo guarda en un vector en forma de tuplas (key,value).
    public Error ReadFileTuples(String fileName)
    {
        long Offset=0;
        FileInputStream fis;

        try {
            fis = new FileInputStream(fileName);
        } catch (FileNotFoundException e) {
            System.err.println("Map::ERROR File "+fileName+" not found.");
            e.printStackTrace();
            return(Error.CErrorOpenInputFile);
        }

        //Construct BufferedReader from InputStreamReader
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));

        String line = null;
        try {
            while ((line = br.readLine())!=null)
            {
                if (MapReduce.DEBUG) System.err.println("DEBUG::Map input " + Offset + " -> " + line);
                AddInput(new MapInputTuple(Offset, line));
                Offset+=line.length();
            }
        } catch (IOException e) {
            System.err.println("Map::ERROR Reading file "+fileName+".");
            e.printStackTrace();
            return(Error.CErrorReadingFile);
        }
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
            return(Error.CErrorReadingFile);
        }

        return(Error.COk);
    }

    public void AddInput(MapInputTuple tuple) { Input.add(tuple); }

}
