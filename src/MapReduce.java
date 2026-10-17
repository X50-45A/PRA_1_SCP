import java.io.File;
import java.util.Collection;
import java.util.Vector;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

 
abstract class MapReduce 
{
	public static final boolean DEBUG = false;
	public static final int DTopN = 5;

	private String 	InputPath;
	private String OutputFiles;
	private int NReducers;
	private int NThreads;

	private int ProcessedFiles =0;
	public static long TotalWords = 0;
	public static long DifferentWords = 0;
	public static AtomicInteger MappersRunning = new AtomicInteger(0);

	public int getProcessedFiles() { return ProcessedFiles; }
	public void incProcessedFiles() { ProcessedFiles++; }

	private Vector<Split> Splits =  new Vector<Split>();
	private Vector<Map> Mappers =  new Vector<Map>();
	private Vector<Reduce> Reducers =  new Vector<Reduce>();


	// Set and Gets
	private void AddSplit(Split split) { Splits.add(split); }
	private void AddMapper(Map mapper) { Mappers.add(mapper); }
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
	public MapReduce()
	{
		SetInputPath("");
		SetOutputFiles("");
		SetReducers(1);
		SetThreads(1);
	}
		
	// Constructor MapReduce: número de reducers a utilizar. Los parámetros de directorio/fichero entrada y salida,
	// número de reducers y threads se inicilizan mediante Set* y las funciones Map y reduce sobreescribiendo los
	// métodos abstractos.
	public MapReduce(String input, String output, int nReducers, int ntreads)
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
	// Versión secuencial: asume que un único Map va a procesar todos los splits.
	private Error Split(String inputDir)
	{
		Error cod;

		// Process input files
		cod = ProcessInputFiles(inputDir);

		// Create only one Mapper and assign all input splits
		if (cod==Error.COk)
		{
			// Create new mapper
			Map mapper = new Map(this);
			AddMapper(mapper);

			// Assigna all splits to the mapper
			for(Split split : Splits)
				mapper.addInputSplit(split.getInput());
		}
		return cod;
	}

	// Genera y lee diferentes splits: 1 split por fichero. Genera un vector con todos los splits generados.
	private Error ProcessInputFiles(String input)
	{
		File folder = new File(input);

		Split split = new Split();
		AddSplit(split);
		
		if (folder.isDirectory()) 
		{
			File[] listOfFiles = folder.listFiles();
		
			/* Read all the files and directories within directory */
		    for (int i = 0; i < listOfFiles.length; i++) 
		    {
		    	if (listOfFiles[i].isFile()) 
		    	{
		    		System.out.println("Processing input file " + listOfFiles[i].getName() + ".");
					incProcessedFiles();
					split.ReadFileTuples(listOfFiles[i].getAbsolutePath());
		    	}
		    	else if (listOfFiles[i].isDirectory()) {
		    		System.out.println("Directory " + listOfFiles[i].getName());
					return ProcessInputFiles(listOfFiles[i].getName());
		    	}
		    }
		}
		else 
		{
			System.out.println("Processing input file " + folder.getName() + ".");
			incProcessedFiles();
			split.ReadFileTuples(folder.getAbsolutePath());
		}

		split.AddInput(new MapInputTuple(-1, ""));
		
		return(Error.COk);
	}
	
	
	// Ejecuta cada uno de los Maps.
	private Error Maps()
	{	
		for(Map map : Mappers)
		{
			if (MapReduce.DEBUG) System.err.println("DEBUG::Running Map "+ map);
			MappersRunning.incrementAndGet();
			if (map.Run()!=Error.COk)
				Error.showError("MapReduce::Map Run error.\n");
			MappersRunning.decrementAndGet();
		}
		System.out.println("\nMappers han procesado un total de "+TotalWords+" palabras.");
		return(Error.COk);
	}
	
	
	public Error Map(Map map, MapInputTuple tuple)
	{
		System.err.println("MapReduce::Map -> ERROR map must be override.");
		return(Error.CError);
	}
	
	
	
	// Ordena y junta todas las tuplas de salida de los maps. Utiliza una función de hash como 
	// función de partición, para distribuir las claves entre los posibles reducers.
	// Utiliza un multimap para realizar la ordenación/unión.
	private Error Suffle()
	{
		for(Map map : Mappers)
		{
			if (MapReduce.DEBUG) map.PrintOutputs();

			for (String key : map.GetOutput().keySet())
			{		    
			    // Calcular a que reducer le corresponde está clave:
				int r = Math.abs(key.hashCode())%Reducers.size();
	
				if (MapReduce.DEBUG) System.err.println("DEBUG::MapReduce::Suffle merge key " + key +" to reduce " + r);
	
				// Añadir todas las tuplas de la clave al reducer correspondiente.
				Reducers.get(r).AddInputKeys(key, map.GetOutput().get(key));
			}
			
			// Eliminar todas las salidas.
			map.GetOutput().clear();
		}
		
		return(Error.COk);
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


