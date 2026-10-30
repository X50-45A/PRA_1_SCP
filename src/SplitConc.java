/* ---------------------------------------------------------------
Práctica 1.
Código fuente: SplitConc.java
Grau Informàtica
NIF [pendiente] y nombre completo autor1: Augustin-Dragos Belu.
NIF [pendiente] y nombre completo autor2: Abraham Ruíz Monsteirin.
--------------------------------------------------------------- */

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

// Split productor: publica cada línea en cuanto la lee.
class SplitConc {
    private final BlockingQueue<MapInputTuple> input = new LinkedBlockingQueue<>();

    public MapInputTuple TakeInput() throws InterruptedException {
        return input.take();
    }

    public Error ReadFileTuples(String fileName) {
        long offset = 0;
        Error result = Error.COk;

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(fileName)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (MapReduce.DEBUG) {
                    System.err.println("DEBUG::Map input " + offset + " -> " + line);
                }
                input.add(new MapInputTuple(offset, line));
                offset += line.length();
            }
        } catch (FileNotFoundException e) {
            System.err.println("SplitConc::ERROR File " + fileName + " not found.");
            result = Error.CErrorOpenInputFile;
        } catch (IOException e) {
            System.err.println("SplitConc::ERROR Reading file " + fileName + ".");
            e.printStackTrace();
            result = Error.CErrorReadingFile;
        } finally {
            // Incluso si falla la lectura, el mapper debe poder salir de take().
            input.add(new MapInputTuple(-1, ""));
        }

        return result;
    }
}
