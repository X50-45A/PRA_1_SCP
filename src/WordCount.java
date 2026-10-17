import java.text.DecimalFormat;

public class WordCount
{
	private static final int DNumReducers = 1;
	private static final String DOutputDirFiles = "./result";
	public static final double NANOSECONDS_IN_ONE_SECOND = 1000 * 1000 * 1000;

	public static void main(String[] args) 
	{
		String inputDir, outputFiles=DOutputDirFiles;
		int NRedurcers=DNumReducers, NThreads=1;

		if (args.length <1 || args.length > 4) {
			System.out.println("Uso: java WordCount <input dir> [<ouput dir>] [<num_reducers>] [<num_hilos>]");
			return;
		}

		if (args.length>1)
			outputFiles = args[1];
		if (args.length>2)
			NRedurcers = Integer.parseInt(args[2]);
		if (args.length>3)
			NThreads = Integer.parseInt(args[3]);

		long startTime = System.nanoTime();
	
		WordCountMR wc = new WordCountMR(args[0], outputFiles, NRedurcers, NThreads);

		wc.Run();

		long endTime = System.nanoTime();

		// Mostrar primos encontrados
		DecimalFormat df = new DecimalFormat("0.000");
		System.out.println("\n" + wc.getProcessedFiles() + " fichero/s procesado/s "+ df.format((endTime-startTime)/NANOSECONDS_IN_ONE_SECOND) + " segs:");
		wc.ShowResults();

		System.exit(0);
	}
}
