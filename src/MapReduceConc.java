/* ---------------------------------------------------------------
Práctica 1.
Código fuente: MapReduceConc.java
Grau Informàtica
NIF [pendiente] y nombre completo autor1: Augustin-Dragos Belu.
NIF [pendiente] y nombre completo autor2: Abraham Ruíz Monsteirin.
--------------------------------------------------------------- */

import java.io.File;
import java.util.Collection;
import java.util.Vector;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;


abstract class MapReduceConc extends MapReduce
{
    public static final boolean DEBUG = false;
    public static final int DTopN = 5;

    private String 	InputPath;
    private String OutputFiles;
    private int NReducers;
    private int NThreads;

    private int ProcessedFiles =0;

    public int getProcessedFiles() { return ProcessedFiles; }
    public void incProcessedFiles() { ProcessedFiles++; }

    private Vector<MapConc> Mappers =  new Vector<MapConc>();
    private Vector<ReduceConc> Reducers =  new Vector<ReduceConc>();
    private List<BlockingQueue<ShuffleTuple>> ShuffleInputs = new ArrayList<>();
    private List<Future<Error>> SplitTasks = new ArrayList<>();
    private List<Future<Error>> MapTasks = new ArrayList<>();
    private List<Future<Error>> ShuffleTasks = new ArrayList<>();
    private ExecutorService SplitPool;
    private ExecutorService MapPool;
    private ExecutorService ShufflePool;
    private boolean ShuffleEndSent;


    // Set and Gets
    private void AddMapper(MapConc mapper) { Mappers.add(mapper); }
    private void AddReduce(ReduceConc reducer) { Reducers.add(reducer); }
    public void SetInputPath(String path) {
        InputPath = path;
    }
    public void SetOutputFiles(String path) {
        OutputFiles = path;
    }

    public void SetThreads(int nthreads) { NThreads=nthreads; }
    public void SetReducers(int nReducers) { NReducers = nReducers; }

    //  Constructores
    public MapReduceConc()
    {
        SetInputPath("");
        SetOutputFiles("");
        SetReducers(1);
        SetThreads(1);
    }

    // Constructor MapReduce: número de reducers a utilizar. Los parámetros de directorio/fichero entrada y salida,
    // número de reducers y threads se inicilizan mediante Set* y las funciones Map y reduce sobreescribiendo los
    // métodos abstractos.
    public MapReduceConc(String input, String output, int nReducers, int ntreads)
    {
        SetInputPath(input);
        SetOutputFiles(output);
        SetReducers(nReducers);
        SetThreads(ntreads);
    }

    public Error Init()
    {
        // Delete output reducer files
        DeleteOutputFiles();

        Mappers.clear();
        Reducers.clear();
        ShuffleInputs.clear();
        SplitTasks.clear();
        MapTasks.clear();
        ShuffleTasks.clear();
        for(int x=0;x<NReducers;x++)
        {
            AddReduce(new ReduceConc(this, OutputFiles +".r"+(x+1)));
            ShuffleInputs.add(new LinkedBlockingQueue<>());
        }

        return Error.COk;
    }


    // Orden de coordinación: iniciar Shuffle, programar Split, recoger Map,
    // esperar Shuffle y ejecutar Reduce.
    @Override
    public Error Run()
    {
        Error initResult = Init();
        if (initResult != Error.COk) return initResult;

        SplitPool = Executors.newVirtualThreadPerTaskExecutor();
        MapPool = Executors.newFixedThreadPool(NThreads);
        ShufflePool = Executors.newFixedThreadPool(Reducers.size());
        ShuffleEndSent = false;
        Error firstError = Error.COk;

        try {
            firstError = FirstError(firstError, StartShuffle());
            firstError = FirstError(firstError, Split(InputPath));
            firstError = FirstError(firstError, Maps());
            if (Thread.currentThread().isInterrupted()) return Error.CError;
            firstError = FirstError(firstError, Suffle());
            firstError = FirstError(firstError, Reduces());
            return firstError;
        } finally {
            // No dejar consumidores Shuffle esperando si una fase falla antes.
            if (!ShuffleEndSent && !Thread.currentThread().isInterrupted()) {
                for (BlockingQueue<ShuffleTuple> queue : ShuffleInputs) {
                    queue.add(ShuffleTuple.endOfStream());
                }
            }
            SplitPool.shutdownNow();
            MapPool.shutdownNow();
            ShufflePool.shutdownNow();
        }
    }

    // ---------- Inicio de la fase Shuffle ----------
    // Arranca sus consumidores antes de que Maps publique resultados.
    private Error StartShuffle() {
        try {
            for (int i = 0; i < Reducers.size(); i++) {
                final int reducerIndex = i;
                ShuffleTasks.add(ShufflePool.submit(() -> RunShuffle(reducerIndex)));
            }
            return Error.COk;
        } catch (RuntimeException e) {
            e.printStackTrace();
            return Error.CError;
        }
    }

    // ---------- Fase Split ----------
    // Programa las lecturas y sus Maps; vuelve sin esperar a que acaben.
    private Error Split(String inputDir) {
        return ProcessInputFiles(inputDir, SplitPool, MapPool, SplitTasks, MapTasks);
    }

    // Ayudante de Split: recorre directorios y programa un productor/mapper por fichero.
    private Error ProcessInputFiles(String input, ExecutorService splitExecutor, ExecutorService mapExecutor, List<Future<Error>> splitTasks, List<Future<Error>> mapTasks) {
        File folder = new File(input);
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files == null) return Error.CErrorOpenInputDir;

            Error firstError = Error.COk;
            for (File file : files) {
                Error result;
                if (file.isFile()) {
                    result = ScheduleFile(file, splitExecutor, mapExecutor, splitTasks, mapTasks);
                } else if (file.isDirectory()) {
                    System.out.println("Directory " + file.getName());
                    result = ProcessInputFiles(file.getAbsolutePath(), splitExecutor, mapExecutor, splitTasks, mapTasks);
                } else {
                    continue;
                }
                firstError = FirstError(firstError, result);
            }
            return firstError;
        }
        if (folder.isFile()) {
            return ScheduleFile(folder, splitExecutor, mapExecutor, splitTasks, mapTasks);
        }
        return Error.CErrorOpenInputFile;
    }

    // Programa el lector virtual y el Map limitado para el mismo SplitConc.
    private Error ScheduleFile(File file, ExecutorService splitExecutor, ExecutorService mapExecutor, List<Future<Error>> splitTasks, List<Future<Error>> mapTasks) {
        SplitConc split = new SplitConc();
        MapConc mapper = new MapConc(this, split, ShuffleInputs);
        AddMapper(mapper);
        System.out.println("Processing input file " + file.getName() + ".");

        // Executors distintos: un Map esperando datos no bloquea al lector.
        splitTasks.add(splitExecutor.submit(() -> split.ReadFileTuples(file.getAbsolutePath())));
        mapTasks.add(mapExecutor.submit(() -> {
            if (DEBUG) System.err.println("DEBUG::Running Map " + mapper);
            return mapper.Run();
        }));
        return Error.COk;
    }

    // ---------- Fase Map ----------
    // Espera a Split y Map; después, el hilo principal cierra las entradas Shuffle.
    private Error Maps() {
        Error firstError = Error.COk;
        for (Future<Error> task : SplitTasks) {
            try {
                Error result = task.get();
                if (result == Error.COk) incProcessedFiles();
                firstError = FirstError(firstError, result);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Error.CError;
            } catch (ExecutionException e) {
                e.getCause().printStackTrace();
                firstError = FirstError(firstError, Error.CError);
            }
        }

        long totalWords = 0;
        for (int i = 0; i < MapTasks.size(); i++) {
            try {
                firstError = FirstError(firstError, MapTasks.get(i).get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Error.CError;
            } catch (ExecutionException e) {
                e.getCause().printStackTrace();
                firstError = FirstError(firstError, Error.CError);
            }
            totalWords += Mappers.get(i).getWordsEmitted();
        }
        TotalWords = totalWords;
        System.out.println("\nMappers han procesado un total de " + totalWords + " palabras.");

        for (BlockingQueue<ShuffleTuple> queue : ShuffleInputs) {
            queue.add(ShuffleTuple.endOfStream());
        }
        ShuffleEndSent = true;
        return firstError;
    }

    // ---------- Fase Shuffle ----------
    // Espera resultados: los consumidores ya han trabajado junto con Map.
    private Error Suffle() {
        Error firstError = Error.COk;
        for (Future<Error> task : ShuffleTasks) {
            try {
                firstError = FirstError(firstError, task.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Error.CError;
            } catch (ExecutionException e) {
                e.getCause().printStackTrace();
                firstError = FirstError(firstError, Error.CError);
            }
        }
        return firstError;
    }

    // Consumidor de una cola: agrega valores solo al reducer que le pertenece.
    private Error RunShuffle(int reducerIndex) {
        BlockingQueue<ShuffleTuple> queue = ShuffleInputs.get(reducerIndex);
        ReduceConc reducer = Reducers.get(reducerIndex);
        try {
            while (true) {
                ShuffleTuple tuple = queue.take();
                if (tuple.end()) return Error.COk;
                if (DEBUG) {
                    System.err.println("DEBUG::Suffle merge key " + tuple.key()
                            + " to reduce " + reducerIndex);
                }
                reducer.AddInputKeys(tuple.key(), List.of(tuple.value()));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Error.CError;
        } catch (RuntimeException e) {
            e.printStackTrace();
            return Error.CError;
        }
    }

    // Conserva el primer error encontrado al recoger las tareas.
    private static Error FirstError(Error current, Error candidate) {
        return current == Error.COk ? candidate : current;
    }


    public Error Map(Map map, MapInputTuple tuple)
    {
        System.err.println("MapReduce::Map -> ERROR map must be override.");
        return(Error.CError);
    }



    // ---------- Fase Reduce ----------
    // Ejecuta cada reducer en un hilo propio y espera a todos con join().
    private Error Reduces()
    {
        Thread[] threads = new Thread[Reducers.size()];
        Error[] results = new Error[Reducers.size()];

        for (int i = 0; i < Reducers.size(); i++) {
            int reducerIndex = i;
            ReduceConc reduce = Reducers.get(i);

            threads[i] = new Thread(() -> {
                try {
                    results[reducerIndex] = reduce.Run();
                } catch (RuntimeException e) {
                    e.printStackTrace();
                    results[reducerIndex] = Error.CError;
                }
            }, "Reduce-" + (i + 1));
            threads[i].start();
        }

        long totalDifferentWords = 0;
        Error firstError = Error.COk;
        boolean interrupted = false;

        for (int i = 0; i < threads.length; i++) {
            while (true) {
                try {
                    threads[i].join();
                    break;
                } catch (InterruptedException e) {
                    // Esperamos a que todos los reducers terminen antes de devolver.
                    interrupted = true;
                }
            }

            totalDifferentWords += (Reducers.get(i)).getWordsEmitted();
            if (results[i] != Error.COk && firstError == Error.COk) {
                firstError = results[i];
            }
        }

        DifferentWords = totalDifferentWords;
        System.out.println("Reduccers han procesado " + totalDifferentWords + " palabras diferentes.");
        if (interrupted) {
            Thread.currentThread().interrupt();
            return Error.CError;
        }
        return firstError;
    }


    public Error Reduce(Reduce reduce, String key, Collection<Integer> values)
    {
        System.err.println("MapReduce::Reduce  -> ERROR Reduce must be override.");
        return(Error.CError);
    }

    public void DeleteOutputFiles()
    {
        Path outputPath = Paths.get(OutputFiles);
        Path directorio = outputPath.getParent();
        String nombreResultado = outputPath.getFileName().toString();

        try (DirectoryStream<Path> stream =  Files.newDirectoryStream(directorio, nombreResultado +".r*"))
        {
            for (Path file : stream) {
                Files.delete(file);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public void ShowResults()
    {
        Path outputPath = Paths.get(OutputFiles);
        Path directorio = outputPath.getParent();
        String nombreResultado = outputPath.getFileName().toString();

        try (DirectoryStream<Path> stream =  Files.newDirectoryStream(directorio, nombreResultado +".r*"))
        {
            for (Path file : stream) {
                System.out.println("----------------------------------------------------------------------");
                System.out.print("Fichero: " + file.getFileName());
                processResultsFile(file, DTopN);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    record WC(String word, long count) {}

    private void processResultsFile(Path file, int topN) throws IOException
    {
        long sizeBytes = Files.size(file);

        List<WC> entries = 	Files.lines(file)
                .map(this::parseLine)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        System.out.println(" \tTamaño: " + sizeBytes + " bytes.  \tNúmero palabras: " + entries.size());
        System.out.println("Top " + topN + " palabras:");
        entries.stream()
                .sorted( Comparator.comparingLong(WC::count).reversed())
                .limit(topN)
                .forEach(wc -> System.out.printf("\t%-20s %d%n", wc.word(), wc.count()));
    }

    private WC parseLine(String line)
    {
        String[] parts = line.trim().split("\\s+");

        if (parts.length != 2) {
            return null;
        }

        try {
            return new WC(parts[0], Long.parseLong(parts[1]));
        }
        catch (NumberFormatException e) {
            return null;
        }
    }

}


