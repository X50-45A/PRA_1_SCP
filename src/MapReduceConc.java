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
import java.util.concurrent.atomic.AtomicInteger;
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

    private Vector<Split> Splits =  new Vector<Split>();
    private Vector<MapConc> Mappers =  new Vector<MapConc>();
    private Vector<Reduce> Reducers =  new Vector<Reduce>();


    // Set and Gets
    private void AddSplit(Split split) { Splits.add(split); }
    private void AddMapper(MapConc mapper) { Mappers.add(mapper); }
    private void AddReduce(Reduce reducer) { Reducers.add(reducer); }
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

        for(int x=0;x<NReducers;x++)
            AddReduce(new Reduce(this, OutputFiles +".r"+(x+1)));

        return Error.COk;
    }


    // Procesa diferentes fases del framework mapreduc: split, map, suffle/merge, reduce.
    //PIPELINE TODO: Que se pueda solapar.
    @Override
    public Error Run()
    {
        if (Init()!=Error.COk)
            Error.showError("MapReduce::Run-Error Init");

        if (Split(InputPath)!=Error.COk)
            Error.showError("MapReduce::Run-Error Split");

        if (Maps()!=Error.COk)
            Error.showError("MapReduce::Run-Error Map");

        if (Suffle()!=Error.COk)
            Error.showError("MapReduce::Run-Error Merge");

        if (Reduces()!=Error.COk)
            Error.showError("MapReduce::Run-Error Reduce");

        return(Error.COk);
    }

    // Procesa los ficheros de entrada para generar los diferentes split y posteriormente crea un mapper para procesarlos
    // Versión Conc: asume que cada Map va a procesar un split y cada split tiene un fichero.
    private Error Split(String inputDir)
    {
        List<Future<Error>> pending = new ArrayList<>();
        Error firstError;

        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor())
        {
            // Se recorre los directorios y se envia las lecturas el executador
            firstError = ProcessInputFiles(inputDir, pool, pending);

            // Espera a que terminen todas las lecturas.
            for (Future<Error> future : pending)
            {
                try {
                    Error result = future.get();

                    if (result == Error.COk) {
                        incProcessedFiles();
                    }
                    else if (firstError == Error.COk) {
                        firstError = result;
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    pool.shutdownNow();
                    return Error.CError;
                }
                catch (ExecutionException e) {
                    e.getCause().printStackTrace();

                    if (firstError == Error.COk) {
                        firstError = Error.CError;
                    }
                }
            }
        }

        if (firstError != Error.COk) {
            return firstError;
        }

        // Un mapper por cada split ya leído.
        for (Split split : Splits) {
            MapConc mapper = new MapConc(this, split);
            AddMapper(mapper);
        }

        return Error.COk;
    }

    // Genera y lee diferentes splits: 1 split por fichero. Genera un vector con todos los splits generados.
    //Cambios a la version Concurrente: --> Se hace que cada Split tenga su arhivo, en la version secuencial se va añadiendo to en el mismo SPLIT.
    private Error ProcessInputFiles(String input, ExecutorService executor, List<Future<Error>> pending) {
        File folder = new File(input);
        if (folder.isDirectory()) {
            File[] listOfFiles = folder.listFiles();
            if (listOfFiles == null) {
                return Error.CErrorOpenInputDir;
            }
            for (File file : listOfFiles) {
                if (file.isFile()) {
                    Split split = new Split();
                    AddSplit(split);
                    System.out.println("Processing input file " + file.getName() + ".");

                    // La lectura del fichero ocurre en una tarea virtual.
                    pending.add(executor.submit(() -> {
                        Error result = split.ReadFileTuples(file.getAbsolutePath());
                        if (result == Error.COk) {
                            split.AddInput(new MapInputTuple(-1, ""));
                        }
                        return result;
                    }));
                }
                else if (file.isDirectory()) {
                    System.out.println("Directory " + file.getName());
                    Error result = ProcessInputFiles(file.getAbsolutePath(), executor, pending);
                    if (result != Error.COk) {
                        return result;
                    }
                }
            }
        }
        else if (folder.isFile()) {
            Split split = new Split();
            AddSplit(split);
            System.out.println("Processing input file " + folder.getName() + ".");
            pending.add(executor.submit(() -> {
                Error result = split.ReadFileTuples(folder.getAbsolutePath());
                if (result == Error.COk) {
                    split.AddInput(new MapInputTuple(-1, ""));
                }
                return result;
            }));
        }
        else {
            return Error.CErrorOpenInputFile;
        }
        return Error.COk;
    }


    // Ejecuta cada uno de los Maps.
    private Error Maps()
    {
        long totalWords = 0;
        Error firstError = Error.COk;

        ExecutorService pool = Executors.newFixedThreadPool(NThreads);
        List<Future<Error>> pendiente = new ArrayList<>();
        try {
            for (MapConc map : Mappers) {
                pendiente.add(pool.submit(() -> {
                    if (MapReduce.DEBUG) {
                        System.err.println("DEBUG::Running Map " + map);
                    }

                    MappersRunning.incrementAndGet();
                    try {
                        return map.Run();
                    } finally {
                        MappersRunning.decrementAndGet();
                    }
                }));
            }

            for (int i = 0; i < pendiente.size(); i++) {
                try {
                    Error result = pendiente.get(i).get(); // espera a que termine este Map

                    totalWords += Mappers.get(i).getWordsEmitted();

                    if (result != Error.COk && firstError == Error.COk) {
                        firstError = result;
                    }
                } catch (ExecutionException e) {
                    e.getCause().printStackTrace();
                    if (firstError == Error.COk) {
                        firstError = Error.CError;
                    }
                    // Sigue recogiendo las demás tareas antes de salir.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    pool.shutdownNow();
                    return Error.CError;
                }
            }

            if (firstError != Error.COk) {
                return firstError;
            }

            System.out.println("\nMappers han procesado un total de "
                    + totalWords + " palabras.");

            return Error.COk;

        } finally {
            pool.shutdown();
        }
    }


    public Error Map(MapConc map, MapInputTuple tuple)
    {
        System.err.println("MapReduce::Map -> ERROR map must be override.");
        return(Error.CError);
    }



    // Ordena y junta todas las tuplas de salida de los maps. Utiliza una función de hash como
    // función de partición, para distribuir las claves entre los posibles reducers.
    // Utiliza un multimap para realizar la ordenación/unión.
    private Error Suffle()
    {
        int numberOfReducers = Reducers.size();
        ExecutorService pool = Executors.newFixedThreadPool(numberOfReducers);
        List<Future<Error>> pending = new ArrayList<>();
        Error firstError = Error.COk;
        try {
            for (int i = 0; i < numberOfReducers; i++) {
                final int myReducerIndex = i;
                Reduce myReducer = Reducers.get(i);
                pending.add(pool.submit(() -> {
                    for (MapConc map : Mappers) {
                        for (String key : map.GetOutput().keySet()) {
                            int assignedReducer = Math.floorMod(key.hashCode(), numberOfReducers);
                            if (assignedReducer == myReducerIndex) {
                                if (MapReduceConc.DEBUG) {
                                    System.err.println("DEBUG::Suffle merge key " + key + " to reduce " + assignedReducer);
                                }
                                myReducer.AddInputKeys(
                                        key, map.GetOutput().get(key));
                            }
                        }
                    }
                    return Error.COk;
                }));
            }
            for (Future<Error> future : pending) {
                try {
                    Error result = future.get();
                    if (result != Error.COk && firstError == Error.COk) {
                        firstError = result;
                    }
                }
                catch (ExecutionException e) {
                    e.getCause().printStackTrace();
                    if (firstError == Error.COk) {
                        firstError = Error.CError;
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    pool.shutdownNow();
                    return Error.CError;
                }
            }
        }
        finally {
            pool.shutdown();
        }
        if (firstError != Error.COk) {
            return firstError;
        }
        // Limpia las salidas solo después de que todos los reducers las hayan leído.
        for (MapConc map : Mappers) {
            map.GetOutput().clear();
        }

        return Error.COk;
    }

    // Ejecuta cada uno de los Reducers.
    private Error Reduces()
    {
        for(Reduce reduce : Reducers)
        {
            if (reduce.Run()!=Error.COk)
                Error.showError("MapReduce::Reduce Run error.\n");
        }
        System.out.println("Reduccers han procesado "+DifferentWords+" palabras diferentes.");
        return(Error.COk);
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


