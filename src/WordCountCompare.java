/* ---------------------------------------------------------------
Práctica 1.
Código fuente: WordCountCompare.java
Grau Informàtica
NIF [pendiente] y nombre completo autor1: Augustin-Dragos Belu.
NIF [pendiente] y nombre completo autor2: Abraham Ruíz Monsteirin.
--------------------------------------------------------------- */

import java.text.DecimalFormat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class WordCountCompare {
    private static final int DEFAULT_REDUCERS = 1;
    private static final int DEFAULT_THREADS = 1;
    private static final String DEFAULT_OUTPUT = "./result";
    private static final double NANOS_PER_SECOND = 1_000_000_000.0;

    public static void main(String[] args) {
        if (args.length < 1 || args.length > 4) {
            System.out.println("Uso: java WordCountCompare <input dir/file> [<output base>] [<num_reducers>] [<num_hilos>]");
            return;
        }

        String input = args[0];
        String outputBase = args.length > 1 ? args[1] : DEFAULT_OUTPUT;
        int reducers = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_REDUCERS;
        int threads = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_THREADS;
        String sequentialOutput = outputBase + "_sequential";
        String concurrentOutput = outputBase + "_concurrent";

        WordCountMR sequential = new WordCountMR(input, sequentialOutput, reducers, 1);
        resetCounters();
        long sequentialStart = System.nanoTime();
        Error sequentialStatus = sequential.Run();
        long sequentialNanos = System.nanoTime() - sequentialStart;

        WordCountMRConc concurrent = new WordCountMRConc(input, concurrentOutput, reducers, threads);
        resetCounters();
        long concurrentStart = System.nanoTime();
        Error concurrentStatus = concurrent.Run();
        long concurrentNanos = System.nanoTime() - concurrentStart;

        DecimalFormat format = new DecimalFormat("0.000");
        double sequentialSeconds = sequentialNanos / NANOS_PER_SECOND;
        double concurrentSeconds = concurrentNanos / NANOS_PER_SECOND;

        System.out.println("\n=== Comparación de ejecuciones ===");
        System.out.println("Secuencial:  " + format.format(sequentialSeconds) + " s (" + sequential.getProcessedFiles() + " ficheros)");
        System.out.println("Concurrente: " + format.format(concurrentSeconds) + " s (" + concurrent.getProcessedFiles() + " ficheros, " + threads + " hilos)");
        if (concurrentSeconds > 0) {
            System.out.println("Aceleración (secuencial / concurrente): " + format.format(sequentialSeconds / concurrentSeconds) + "x");
        }
        System.out.println("Salidas secuencial:  " + sequentialOutput + ".r1 ... .r" + reducers);
        System.out.println("Salidas concurrente: " + concurrentOutput + ".r1 ... .r" + reducers);

        if (sequentialStatus == Error.COk && concurrentStatus == Error.COk) {
            try {
                boolean sameResults = readCounts(sequentialOutput, reducers).equals(readCounts(concurrentOutput, reducers));
                System.out.println("Los resultados por palabra " + (sameResults ? "coinciden." : "NO coinciden."));
            } catch (IOException e) {
                System.err.println("No se pudieron comparar los ficheros de salida: " + e.getMessage());
            }
        } else {
            System.err.println("Una de las ejecuciones terminó con error: secuencial="
                    + sequentialStatus + ", concurrente=" + concurrentStatus);
        }
    }

    private static void resetCounters() {
        MapReduce.TotalWords = 0;
        MapReduce.DifferentWords = 0;
        MapReduce.MappersRunning.set(0);
    }

    /** Lee y combina las palabras de todos los ficheros reducer, sin depender de su orden. */
    private static HashMap<String, Long> readCounts(String outputBase, int reducers) throws IOException {
        HashMap<String, Long> counts = new HashMap<>();
        for (int i = 1; i <= reducers; i++) {
            Path outputFile = Path.of(outputBase + ".r" + i);
            for (String line : Files.readAllLines(outputFile)) {
                String[] fields = line.trim().split("\\s+");
                if (fields.length == 2) {
                    counts.merge(fields[0], Long.parseLong(fields[1]), Long::sum);
                }
            }
        }
        return counts;
    }
}
