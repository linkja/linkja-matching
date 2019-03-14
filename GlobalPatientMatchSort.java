package externalSortPackage;

//classes used
//import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.FileUtils;
//import org.json.JSONArray;
//import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
//import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
//import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class GlobalPatientMatchSort {

	private final static DateTimeFormatter dateTimeformat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	private final static DateTimeFormatter dateTimeHL7format = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
	private final static String directorySeparator = "/";
	private final static String aZero = "0";
	private final static String delimPipe = "|";
	private final static String delimComma = ",";
	//private final static String newLine = System.getProperty("line.separator");
	
	private static AtomicInteger globalId;
	private static int atomicIntegerSeed;
	private static String atomicIntegerPath;
	private static final String atomicIntegerFile = "global-match-globalId.txt";

	private static String configRootPath;
	private static String configFilePath;
	private static final String configFileName = "global-match.properties";
	private static String inputDir;
	private static String outputDir;
	private static String processedDir;
	private static String logFile;
	private static String logFileDir;
	private static final String logFileName = "match-log-";
	private static String outputFile1 = null;
	private static String tempMessage = null;

	private static int processStep = 0;
	private static String masterFileMatch1;
	private static String masterFileMatch2;
	private static String masterFileMatch3;
	private static String masterFileMatch4;
	private static String masterFileMatch5;
	private static String inputFileNamePrefix;
	private static String inputFileNameSuffix;
	
	private static int siteid_Idx = 0;				// index into columns in input hash files
	private static int projectid_Idx = 1;
	private static int PIDHASH_Idx = 2;
	private static int fnamelnamedobssn1_Idx = 3;
	private static int lnamefnamedobssn2_Idx = 4;
	private static int fnamelnamedob3_Idx = 5;
	private static int lnamefnamedob4_Idx = 6;
	private static int fnamelnameTdobssn5_Idx = 7;
	private static int fnamelnameTdob6_Idx = 8;
	private static int fname3lnamedobssn7_Idx = 9;
	private static int fname3lnamedob8_Idx = 10;
	private static int fnamelnamedobDssn9_Idx = 11;
	private static int fnamelnamedobYssn10_Idx = 12;
	private static int exception_flg11_Idx = 13;
	private static int recordsToSkip = 1;
	
	private static List<String> lineArr = new ArrayList<String>(10000);		// lines read from hash files

	//private static String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();

	// step 1 process hash files 
	private static void processGlobalPatientMatchIntake(int step) {

		tempMessage = null;

		String fullName = null;			// read hash files from input directory
		tempMessage = "Step " + step + ": reading input files from: " + inputDir;
		System.out.println( tempMessage );
		writeLog( tempMessage );

		ArrayList<String> inputFiles = getFileNames1Dir(inputDir, inputFileNamePrefix, inputFileNameSuffix);	// go read files in directory
		if (inputFiles == null || inputFiles.size() == 0) {
			tempMessage = "no input files found in directory " + inputDir;
			System.out.println( tempMessage );
			writeLog( tempMessage );
			return;
		}
		
		for (int i=0; i< inputFiles.size(); i++) {

			fullName = inputFiles.get(i);
			fullName = changeDirectorySeparator(fullName);	// change file separator if Windows
			//fileName = getFileName(fullName); 			// get file name without extension

			readFileIn(fullName);		// go read file
			
			/*
			// move file to processed dir
			String fileToMoveName = changeFileExtension(fullName, "processed");
			File fileToMove = FileUtils.getFile(fileToMoveName);
			if (fileToMove.exists()) {
				boolean fileMoved = false;
				try {
					fileMoved = moveFileToDir(processedDir, fileToMoveName);
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (!fileMoved) {
					System.out.println("***file not moved: " + fileToMoveName);
				}
			}
			*/
		}
	}

	public static void readFileIn(String inputFile1) {

		outputFile1 = masterFileMatch1;		// put all data in masterFileMatch1 file
		String invalidDataFile = changeFileExtension(inputFile1, "bad");

		int recordsRead = 0;
		tempMessage = "processing hash file: " + inputFile1;
		System.out.println( tempMessage );
		writeLog( tempMessage );

		File fileIn = new File(inputFile1);
		try ( BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileIn), "UTF-8"))) {

			String line = null;
			while ((line = br.readLine()) != null) {

				recordsRead = recordsRead + 1;
				if (recordsRead <= recordsToSkip) {
					System.out.println("skipping record " + recordsRead);
					continue;
				}

				String lineIn = line.trim().replaceAll("\\s+", " ");	// reduce multiple spaces to single space
				String[] splitLine = lineIn.split(delimComma);			// split incoming text

				// check that primary data is valid
				boolean validData = true;
				if (splitLine[siteid_Idx] == null || splitLine[siteid_Idx].isEmpty()) {
					validData = false;
				}
				if (splitLine[exception_flg11_Idx] == null || splitLine[exception_flg11_Idx].isEmpty()) {
					validData = false;
				}
				if (splitLine[fnamelnamedobssn1_Idx] == null || splitLine[fnamelnamedobssn1_Idx].isEmpty() || splitLine[fnamelnamedobssn1_Idx].equalsIgnoreCase("NULL")) {
					validData = false;
				}
				if (splitLine[lnamefnamedobssn2_Idx] == null || splitLine[lnamefnamedobssn2_Idx].isEmpty() || splitLine[lnamefnamedobssn2_Idx].equalsIgnoreCase("NULL")) {
					validData = false;
				}
				if (splitLine[fnamelnameTdobssn5_Idx] == null || splitLine[fnamelnameTdobssn5_Idx].isEmpty() || splitLine[fnamelnameTdobssn5_Idx].equalsIgnoreCase("NULL")) {
					validData = false;
				}
				if (splitLine[fnamelnamedobDssn9_Idx] == null || splitLine[fnamelnamedobDssn9_Idx].isEmpty() || splitLine[fnamelnamedobDssn9_Idx].equalsIgnoreCase("NULL")) {
					validData = false;
				}
				if (splitLine[fnamelnamedobYssn10_Idx] == null || splitLine[fnamelnamedobYssn10_Idx].isEmpty() || splitLine[fnamelnamedobYssn10_Idx].equalsIgnoreCase("NULL")) {
					validData = false;
				}
				if (!validData) {
					writeInvalidData( invalidDataFile, lineIn );	// write out data for later
					continue;			// skip to next record
				}
				
				// make up record for match 1 (like database index)
				String lineNew =splitLine[fnamelnamedobssn1_Idx] + delimComma +
								splitLine[lnamefnamedobssn2_Idx] + delimComma +
								splitLine[fnamelnameTdobssn5_Idx] + delimComma +
								splitLine[fnamelnamedobDssn9_Idx] + delimComma +
								splitLine[fnamelnamedobYssn10_Idx] + delimComma +
								splitLine[siteid_Idx] + delimComma +
								splitLine[projectid_Idx] + delimComma +
								splitLine[PIDHASH_Idx] + delimComma +
								splitLine[fnamelnamedob3_Idx] + delimComma +
								splitLine[lnamefnamedob4_Idx] + delimComma +
								splitLine[fnamelnameTdob6_Idx] + delimComma +
								splitLine[fname3lnamedobssn7_Idx] + delimComma +
								splitLine[fname3lnamedob8_Idx] + delimComma +
								splitLine[exception_flg11_Idx] + delimComma +
								"0";		// default to global id of 0, always in last column

				lineArr.add( lineNew );		// save this new rearranged line 
				if (lineArr.size() > 25000) {		// collect many lines before storing to disk
					System.out.println("lineArr has elements: " + lineArr.size());
					boolean success = saveCurrentLineArray(outputFile1, lineArr);		// go save off this array
					if (success) {
						lineArr.clear();	// remove all elements, clear faster than removeAll
					} else {
						System.out.println("error occurred, exiting");
						System.exit(-1);
					}
				}
			}

		} catch (UnsupportedEncodingException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		if (lineArr.size() > 0) {
			System.out.println("lineArr still has elements: " + lineArr.size());
			boolean success = saveCurrentLineArray(outputFile1, lineArr);		// go save off this array
		}

		tempMessage = recordsRead + " lines read from: " + inputFile1;
		System.out.println( tempMessage );
		writeLog( tempMessage );
	}

	public static boolean saveCurrentLineArray(String outFileName, List<String> lines) {
		
		boolean success = true;
	
		tempMessage = "sorting " + lineArr.size() + " lines before storing";
		System.out.println( tempMessage );
		writeLog( tempMessage );
		Collections.sort( lineArr );		// sort array before storing it

		tempMessage = "saving " + lineArr.size() + " lines to " + outFileName;
		System.out.println( tempMessage );
		writeLog( tempMessage );

		Path path = Paths.get( outFileName );
		if (Files.notExists(path)) {
			try { Files.createFile(path);		// create file if doesn't exist
			} catch (IOException e) { e.printStackTrace(); }
		}
		try(    FileWriter fw = new FileWriter( outFileName, true);  //try-with-resources --> autoclose
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw)) {
			
			for (String entry : lines) {
				out.println( entry );		// write out text to output file
			}
		}  
		catch( IOException e ){
			// File writing/opening failed at some stage.
			System.out.println("**Unable to write to log file " + outFileName);
			success = false;
		}
		return success;
	}
	
	// step 2 sort masterfile1
	private static void sortMasterFile1(int step) {
		
		String masterFile;
		String masterFileOut;
		String tempFileDir;
		switch (step) {
		case 1:
			System.out.println("***Invalid process step " + step + " found in sortMasterFile1");
			return;
		case 2:
			masterFile = masterFileMatch1;
			masterFileOut = masterFileMatch2;
			tempFileDir = getFileDir(masterFileMatch2);
			break;
		default :
			System.out.println("***Invalid process step " + step + " found in sortMasterFile1");
			return;
		}
		
		tempMessage = "Step " + step + ": sorting masterfile " + masterFile;
		System.out.println( tempMessage );
		writeLog( tempMessage );
		
		doExternalSortFile(masterFile, masterFileOut, tempFileDir);		// go do external sort
	}
	
	private static void doExternalSortFile(String masterFile, String masterFileOut, String tempFileDir) {
		
        File f1 = null;
        File f2 = null;
		try {
			f1 = new File( masterFile );
			f2 = new File( masterFileOut );
			//f2 = File.createTempFile( masterFileOut, "tmp");
	        //f2.deleteOnExit();								
			//call sort method "-t","2048" is max temp files ,"-H","1" to ignore header lines
	        ExternalSort.main(new String[]{"-v","-t","2048","-s",tempFileDir,f1.toString(),f2.toString()});
		
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		tempMessage = "finished sort: " + masterFile;
		System.out.println( tempMessage );
		writeLog( tempMessage );
		tempMessage = "created file:  " + masterFileOut;
		System.out.println( tempMessage );
		writeLog( tempMessage );
	}
	
	// step 4 sort masterfile3
	private static void sortMasterFile3(int step) {
		
		String masterFile;
		String masterFileOut;
		String tempFileDir;
		switch (step) {
		case 1:
		case 2:
		case 3:
			System.out.println("***Invalid process step " + step + " found in sortMasterFile3");
			return;
		case 4:
			masterFile = masterFileMatch3;
			masterFileOut = masterFileMatch4;
			tempFileDir = getFileDir(masterFileMatch3);
			break;
		default :
			System.out.println("***Invalid process step " + step + " found in sortMasterFile3");
			return;
		}
		
		tempMessage = "Step " + step + ": sorting masterfile " + masterFile;
		System.out.println( tempMessage );
		writeLog( tempMessage );
		
		doExternalSortFile(masterFile, masterFileOut, tempFileDir);		// go do external sort
	}
	
	// step 3 assign global ids to matching patients in masterfile2
	private static void assignGlobalIds2(int step) {

		String masterFile;
		String masterFileOut;

		switch (step) {
		case 1:
		case 2:
			System.out.println("***Invalid process step " + step + " found in assignGlobalIds2");
			return;
		case 3:
			masterFile = masterFileMatch2;
			masterFileOut = masterFileMatch3;
			break;
		default :
			System.out.println("***Invalid process step " + step + " found in assignGlobalIds2");
			return;
		}

		tempMessage = "Step " + step + ": assigning global Ids in " + masterFile;
		System.out.println( tempMessage );
		writeLog( tempMessage );

		int recordsRead = 0;
		int recordsWritten = 0;

		File fileIn = new File(masterFile);
		File fileOut = new File(masterFileOut);
		//BufferedReader br = null;
		//BufferedWriter bw = null;
		try (	BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileIn), "UTF-8"));
				BufferedWriter bw = new BufferedWriter(new FileWriter(fileOut)) ) {

			Integer nextGlobalId = 0;
			boolean currMatch = false;
			boolean lastMatch = false;
			String currKey = "";
			String lastKey = "";
			String currLine = "";
			String lastLine = "";
			String line = "";
			while ((line = br.readLine()) != null) {

				if (line.isEmpty()) {
					System.out.println("skip empty line");
					continue;			// go to next record if blank line
				}
				recordsRead++;

				//System.out.println("get key gblId 4-5: " + line.substring(nthIndexOf(line, delimComma, 4)+1,nthIndexOf(line, delimComma, 5)));
				currKey = line.substring(0, nthIndexOf(line, delimComma, 4));	// get key of this record up to site id
				if (!currKey.equals(lastKey)) {				// check if match with last record
					currMatch = false;
					if (!lastMatch) { 		// if didn't match last record set global id to 0
						nextGlobalId = 0;	// so current global id will not be given to last record 
					}
				} else {
					currMatch = true;
					if (!lastMatch) {	 	// have match, if didn't have previous match, need new global id, else will use previous global id
						nextGlobalId = globalId.incrementAndGet();	// get next global Id	
						//System.out.println("updating patient with globalId: " + nextGlobalId);
					}
				}

				if (recordsRead > 1) {
					// write last line with new global id to file.  skip storing 1st record
					String lastLineNew = updateGlobalIdInRecord( lastLine, nextGlobalId );
					bw.write( lastLineNew );
					bw.newLine();
					recordsWritten++;
				}

				// prepare current line for storage
				String[] splitLine = line.split(delimComma);			// split incoming text
				//elements: 8,9,10,5,6,7,0,1,2,11,12,3,4,13,8(gid) order for masterFileMatch2
				currLine = splitLine[8] + delimComma +		// rearrange line for match 2 indexing
						splitLine[9] + delimComma +
						splitLine[10] + delimComma +
						splitLine[5] + delimComma +
						splitLine[6] + delimComma +
						splitLine[7] + delimComma +
						splitLine[0] + delimComma +
						splitLine[1] + delimComma +
						splitLine[2] + delimComma +
						splitLine[11] + delimComma +
						splitLine[12] + delimComma +
						splitLine[3] + delimComma +
						splitLine[4] + delimComma +
						splitLine[13] + delimComma +
						Integer.toString(nextGlobalId);

				//displayLineColumns( currLine );

				lastKey = currKey;		// save current data to compare for next record
				lastLine = currLine;
				lastMatch = currMatch;
			}
			
			// exits read loop with last record read not stored in output file, so store this record
			String lastLineNew = updateGlobalIdInRecord( lastLine, nextGlobalId );
			bw.write( lastLineNew );
			bw.newLine();
			recordsWritten++;

		} catch (UnsupportedEncodingException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		writeAtomicIntegerSeed();		// save current global id, will be used during next run

		tempMessage = recordsRead + " records read from:  " + masterFile;
		System.out.println( tempMessage );
		writeLog( tempMessage );
		tempMessage = recordsWritten + " records written to: " + masterFileOut;
		System.out.println( tempMessage );
		writeLog( tempMessage );
	}
	
	private static String updateGlobalIdInRecord(String line, int globalId) {
		String lineOut = null;
		int commaIndex = line.lastIndexOf(delimComma);		// global Id is always last column in record		
		lineOut = line.substring(0, commaIndex + 1) + Integer.toString(globalId);	// add global id to end of record
		return lineOut;
	}
	
	private static String getGlobalIdInRecord(String line) {
		int commaIndex = line.lastIndexOf(delimComma);			// global Id is always last column in record		
		String globalId = line.substring(commaIndex + 1);		// get global id at end of record
		return globalId;
	}
	
	// step 5 assign global ids to matching patients in masterfile4
	private static void assignGlobalIds4(int step) {

		String masterFile;
		String masterFileOut;
		String currGlobalId;
		String lastGlobalId;

		switch (step) {
		case 1:
		case 2:
		case 3:
		case 4:
			System.out.println("***Invalid process step " + step + " found in assignGlobalIds4");
			return;
		case 5:
			masterFile = masterFileMatch4;
			masterFileOut = masterFileMatch5;
			break;
		default :
			System.out.println("***Invalid process step " + step + " found in assignGlobalIds4");
			return;
		}

		tempMessage = "Step " + step + ": assigning global Ids in " + masterFile;
		System.out.println( tempMessage );
		writeLog( tempMessage );

		int recordsRead = 0;
		int recordsWritten = 0;

		File fileIn = new File(masterFile);
		File fileOut = new File(masterFileOut);
		//BufferedReader br = null;
		//BufferedWriter bw = null;
		try (	BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileIn), "UTF-8"));
				BufferedWriter bw = new BufferedWriter(new FileWriter(fileOut)) ) {

			Integer nextGlobalId = 0;
			boolean currMatch = false;
			boolean lastMatch = false;
			String currKey = "";
			String lastKey = "";
			String currLine = "";
			String lastLine = "";
			String line = "";
			while ((line = br.readLine()) != null) {

				if (line.isEmpty()) {
					System.out.println("skip empty line");
					continue;			// go to next record if blank line
				}
				recordsRead++;

				currKey = line.substring(0, nthIndexOf(line, delimComma, 2));	// get key of this record up to site id
				
				if (recordsRead > 1) {				//bypass check on 1st record since current and last is meaningless

					lastGlobalId = getGlobalIdInRecord( lastLine );	// get gblId of last record
					if (!currKey.equals(lastKey)) {					// check if this record matches with last record
						currMatch = false;
						if (lastGlobalId.equals( aZero )) {		// if last record has no gblId, get next gblId, update record
							nextGlobalId = globalId.incrementAndGet();			// get next global Id	
							//System.out.println("updating patient with globalId: " + nextGlobalId);
							lastLine = updateGlobalIdInRecord( lastLine, nextGlobalId );		// update record
						}
					} else {
						currMatch = true;		// else records match. Use gblId that is not 0 for both records 
						currGlobalId = getGlobalIdInRecord( line );		// get gblId of curr record
						int compareInt = lastGlobalId.compareToIgnoreCase( currGlobalId );		// compare last gblId and curr gblId
						if (compareInt < 0) {		// if last gblId is less than curr gblId, use curr gblId
							lastLine = updateGlobalIdInRecord( lastLine, Integer.parseInt(currGlobalId) );	// update record
						} else if (compareInt > 0) {	// if last gblId is greater than curr gblId, use last gblId
							line = updateGlobalIdInRecord( line, Integer.parseInt(lastGlobalId) );	// update record
						} else {		// else means both gblIds equal, so update both lastline and curr line with new gblId if gblId is 0
							if (lastGlobalId.equals( aZero )) {		// if last line has no gblId, means both need gblId
								nextGlobalId = globalId.incrementAndGet();		// get next global Id	
								//System.out.println("updating patient with globalId: " + nextGlobalId);
								lastLine = updateGlobalIdInRecord( lastLine, nextGlobalId );		// update record
								line = updateGlobalIdInRecord( line, nextGlobalId );				// update record
							}
						}
					}

					bw.write( lastLine );	// write last line with new global id to file
					bw.newLine();
					recordsWritten++;
				}

				// prepare current line for storage
				String[] splitLine = line.split(delimComma);			// split incoming text
				//elements: 6,7,0,1,8,2,9,10,11,12,3,4,5,13,14(gid) order for masterFileMatch4
				currLine = splitLine[6] + delimComma +		// rearrange line for final matching
						splitLine[7] + delimComma +
						splitLine[0] + delimComma +
						splitLine[1] + delimComma +
						splitLine[8] + delimComma +
						splitLine[2] + delimComma +
						splitLine[9] + delimComma +
						splitLine[10] + delimComma +
						splitLine[11] + delimComma +
						splitLine[12] + delimComma +
						splitLine[3] + delimComma +
						splitLine[4] + delimComma +
						splitLine[5] + delimComma +
						splitLine[13] + delimComma +
						splitLine[14];

				//displayLineColumns( currLine );

				lastKey = currKey;		// save current data to compare for next record
				lastLine = currLine;
				lastMatch = currMatch;
			}
			
			// exits read loop with last record read not stored in output file, so store this record
			lastGlobalId = getGlobalIdInRecord( lastLine );		// get gblId of last record
			if (lastGlobalId.equals( aZero )) {		// if last line has no gblId, means didn't match previous record
				nextGlobalId = globalId.incrementAndGet();						// get next global Id
				lastLine = updateGlobalIdInRecord( lastLine, nextGlobalId );	// add to record
			}
			bw.write( lastLine );			// write to output file
			bw.newLine();
			recordsWritten++;

		} catch (UnsupportedEncodingException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		writeAtomicIntegerSeed();		// save current global id, will be used during next run

		tempMessage = recordsRead + " records read from:  " + masterFile;
		System.out.println( tempMessage );
		writeLog( tempMessage );
		tempMessage = recordsWritten + " records written to: " + masterFileOut;
		System.out.println( tempMessage );
		writeLog( tempMessage );
	}


	private static void displayLineColumns(String line) {

		String[] splitLine = line.split(delimComma);			// split incoming text
		System.out.println("-------------------");
		System.out.println("elem 0: " + splitLine[0]);
		System.out.println("elem 1: " + splitLine[1]);
		System.out.println("elem 2: " + splitLine[2]);
		System.out.println("elem 3: " + splitLine[3]);
		System.out.println("elem 4: " + splitLine[4]);
		System.out.println("elem 5: " + splitLine[5]);
		System.out.println("elem 6: " + splitLine[6]);
		System.out.println("elem 7: " + splitLine[7]);
		System.out.println("elem 8: " + splitLine[8]);
		System.out.println("elem 9: " + splitLine[9]);
		System.out.println("elem 10: " + splitLine[10]);
		System.out.println("elem 11: " + splitLine[11]);
		System.out.println("elem 12: " + splitLine[12]);
		System.out.println("elem 13: " + splitLine[13]);
		System.out.println("elem 14: " + splitLine[14]);
	}

	//compares two Strings returns -1 if c1 comes before c2, +1 if c2 comes first, otherwise 0
	public static int compareToIgnoreCase(final String c1, final String c2) {
		if (c1 == null) {
			return c2 == null ? 0 : -c2.compareToIgnoreCase(c1);
		}
		return c1.compareToIgnoreCase(c2);
	} 

	public static boolean pastTimeToExit() {
		int currentHour = LocalTime.now().getHour();	// check if time to exit
		return (currentHour >= 22);	
	}

	public static String timeNowFormatted() {
		LocalDateTime now = LocalDateTime.now();		//Get current date time
		return now.format( dateTimeformat );
	}

	public static String dateNowFormatted() {
		LocalDate now = LocalDate.now();				//Get current date
		return now.format(DateTimeFormatter.ISO_DATE);
	}

	public static String dateTimeNowFormatted() {
		LocalDateTime now = LocalDateTime.now();		//Get current date time
		return now.format( dateTimeformat );
	}

	public static String dateTimeNowHL7() {
		LocalDateTime now = LocalDateTime.now();		//Get current date time in HL7 format
		return now.format( dateTimeHL7format );
	}

	public static Date dateNow() {
		LocalDate now = LocalDate.now();			//  get local date now
		Instant instant = now.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant();
		Date date = Date.from(instant);				// convert to date
		return date;
	}

	private static long millisToNextHour() {
		LocalDateTime nextHour = LocalDateTime.now().plusHours(1).truncatedTo(ChronoUnit.HOURS);
		long delay = LocalDateTime.now().until(nextHour, ChronoUnit.MILLIS);
		return delay;
	}

	private static long secondsToNextHour() {
		long seconds = TimeUnit.MILLISECONDS.toSeconds(millisToNextHour());
		return seconds;
	}
	
	//	Retrieves index of nth occurrence of substring in the source (0 being the first)
	public final static int nthIndexOf(String src, String sub, int n) {
		int i = -1, c = 0;
		for (c = 0; c < n; c++) {
			i = src.indexOf(sub, i + 1);
			if (i == -1) {
				return i;
			}
		}
		return src.indexOf(sub, i + 1);
	}

	public static void writeInvalidData( String fileOut, String invalidData) {

		if (invalidData == null || invalidData.isEmpty()) {		// check if anything in invalidData
			return;
		}
		System.out.println("skip invalid data record");

		Path path = Paths.get( fileOut );
		if (Files.notExists(path)) {
			try {
				Files.createFile(path);		// create file if doesn't exist
			} catch (IOException e) { e.printStackTrace(); }
		}
		try(    FileWriter fw = new FileWriter( fileOut, true);  //try-with-resources --> autoclose, true=append
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw)) {

			out.println( invalidData );				// write out data
		}  
		catch( IOException e ){
			// File writing/opening failed at some stage.
			System.out.println("**Unable to write to file: " + fileOut);
		}
	}

	public static void writeLog(String textMessage) {

		String logFile1 = logFile + dateNowFormatted() + ".txt";
		String text = timeNowFormatted() + " " + textMessage;

		Path path = Paths.get( logFile1 );
		if (Files.notExists(path)) {
			try { Files.createFile(path);		// create file if doesn't exist
			} catch (IOException e) { e.printStackTrace(); }
		}
		try(    FileWriter fw = new FileWriter( logFile1, true);  //try-with-resources --> autoclose
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw)) {
			out.println( text );		// write out text
		}  
		catch( IOException e ){
			// File writing/opening failed at some stage.
			System.out.println("**Unable to write to log file " + logFile1);
		}
	}

	private static String changeDirectorySeparator(String filePath) {
		return filePath.replaceAll("\\\\", directorySeparator);	// change dir separator if Windows
	}

	private static String makeFilePath(String filePath, String fileName) {
		filePath = changeDirectorySeparator(filePath);
		if (filePath.endsWith(directorySeparator)) {
			return filePath + fileName;
		} else {
			return filePath + directorySeparator + fileName;
		}
	}

	public static String changeFileExtension(String fileToRename, String newExtension) {
		int index = fileToRename.lastIndexOf('.');
		String name = fileToRename.substring(0, index + 1);
		return name + newExtension;
	}

	private static String getFileDir(String fullFilePath) {
		String fullName = changeDirectorySeparator(fullFilePath);	// check if Windows separator
		String fileDir = fullName.substring(0, fullName.lastIndexOf("/")+1);
		return fileDir;
	}
	
	private static String getFileName(String fullFilePath) {
		String fileName = null;
		String fullName = changeDirectorySeparator(fullFilePath);	// check if Windows separator
		int dotIndex = fullName.lastIndexOf(".");					// see if file type present
		if (dotIndex >= 0) {
			fileName = fullName.substring(fullName.lastIndexOf("/")+1, dotIndex);
		} else {
			fileName = fullName.substring(fullName.lastIndexOf("/")+1);
		}
		return fileName;
	}

	private static String getFileExtension(String fullFilePath) {
		String fullName = changeDirectorySeparator(fullFilePath);	// check if Windows separator
		String fileName = fullName.substring(fullName.lastIndexOf("/")+1);
		return fileName;
	}

	public static boolean moveFileToDir(String processedFilesDirectory, String fileToMoveName) throws IOException {
		boolean success = false;
		String tempMessage = null;
		String fileToMoveNameOnly = null;
		String fileNewLocName = null;

		fileToMoveName = changeDirectorySeparator(fileToMoveName);
		fileToMoveNameOnly = getFileExtension(fileToMoveName);
		fileNewLocName = makeFilePath(processedFilesDirectory, fileToMoveNameOnly);	// indicate new location	
		File fileToMove = FileUtils.getFile(fileToMoveName);
		File fileNewLoc = FileUtils.getFile(fileNewLocName);
		if (!fileToMove.exists()) {
			tempMessage = "***file to move not found: " + fileToMoveName;	// check if file exists
			writeLog( tempMessage );
			System.out.println( tempMessage);
			return false;
		}

		try {
			FileUtils.copyFile(fileToMove, fileNewLoc, true);	// copy to new location
			if (fileNewLoc.exists()) {
				FileUtils.deleteQuietly(fileToMove);			// delete orig if copy successful
			}
			tempMessage = fileToMove.getAbsolutePath() + "\n moved to " + fileNewLoc.getAbsolutePath();
			writeLog( tempMessage );
			System.out.println( tempMessage );
			success = true;
		} catch (IOException e) {
			e.printStackTrace();
			tempMessage = "***file not moved: " + fileToMoveName;
			writeLog( tempMessage );
			//System.out.println( tempMessage );
		}
		return success;
	}

	private static void readConfig(String configFile) {
		System.out.println("reading config file: " + configFile);

		/*  // to read config file from a class path directory 
			String resourceName = "myconf.properties"; // could also be a constant
			ClassLoader loader = Thread.currentThread().getContextClassLoader();
			Properties props = new Properties();
			try(InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
			props.load(resourceStream);
			}
		 */

		// read config data from properties file using try with resources
		Properties prop = new Properties();
		try ( InputStream input = new FileInputStream( configFile )) {
			prop.load( input );
			//prop.load(new FileInputStream(configFile));

			//configFileDir = prop.getProperty("ConfigFilesDirectory");
			inputDir = prop.getProperty("InputFilesDirectory");
			inputDir = changeDirectorySeparator(inputDir);				// change file separator if Windows
			outputDir = prop.getProperty("OutputFilesDirectory");
			outputDir = changeDirectorySeparator(outputDir);			// change file separator if Windows
			processedDir = prop.getProperty("ProcessedFilesDirectory");
			processedDir = changeDirectorySeparator(processedDir);		// change file separator if Windows
			logFileDir = prop.getProperty("ProcessedFilesDirectory");
			logFile = makeFilePath(logFileDir, logFileName);
			
			inputFileNamePrefix = prop.getProperty("InputFileNamePrefix");
			inputFileNameSuffix = prop.getProperty("InputFileNameSuffix");
			masterFileMatch1 = prop.getProperty("MasterFileMatch1");
			masterFileMatch1 = changeDirectorySeparator(masterFileMatch1);	// change file separator if Windows
			masterFileMatch2 = prop.getProperty("MasterFileMatch2");
			masterFileMatch2 = changeDirectorySeparator(masterFileMatch2);	// change file separator if Windows
			masterFileMatch3 = prop.getProperty("MasterFileMatch3");
			masterFileMatch3 = changeDirectorySeparator(masterFileMatch3);	// change file separator if Windows
			masterFileMatch4 = prop.getProperty("MasterFileMatch4");
			masterFileMatch4 = changeDirectorySeparator(masterFileMatch4);	// change file separator if Windows
			masterFileMatch5 = prop.getProperty("MasterFileMatch5");
			masterFileMatch5 = changeDirectorySeparator(masterFileMatch5);	// change file separator if Windows
		
			writeLog("*****Starting Global Patient Match*****"); 
			writeLog("reading configuration file " + configFile);

			System.out.println("MasterFileMatch1: " + masterFileMatch1);
			System.out.println("MasterFileMatch2: " + masterFileMatch2);
			System.out.println("MasterFileMatch3: " + masterFileMatch3);
			System.out.println("MasterFileMatch4: " + masterFileMatch4);
			System.out.println("MasterFileMatch5: " + masterFileMatch5);
			System.out.println("log file:     " + logFile);

		} catch (Exception e) {
			//e.printStackTrace();
			System.out.println("**read config error message: "+e.getMessage());
			System.exit(1);
		}
		
		readAtomicIntegerSeed();		// read starting number to seed global Id generator
	}
	
	// read files from 1 input directory
	public static ArrayList<String> getFileNames1Dir(String directory, final String prefix, final String suffix) {

		ArrayList<String> matchingFiles = new ArrayList<String>();

		File dir = new File(directory);
		if (!dir.exists()) {
			System.out.println("***" + directory + " directory does not exist");
			return matchingFiles;
		}
		FilenameFilter fileFilter = new FilenameFilter() {		// create a dir filter
			public boolean accept (File dir, String name) {
				if (prefix.isEmpty()) {
					return name.endsWith( suffix );		// if can have only suffix
				} else {
					return name.startsWith( prefix ) && name.endsWith( suffix );  // if can have prefix + suffix
				}
			} 
		};

		//String[] files = dir.list(fileFilter);
		File[] paths = dir.listFiles(fileFilter);			// get files in dir returned by filter
		for (File path : paths) {
			matchingFiles.add(path.getAbsolutePath());		// get full file and directory path
		}
		return matchingFiles;
	}
	
	public static void readAtomicIntegerSeed() {

		File fileIn = new File(atomicIntegerPath);
		try ( BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileIn))) ) {

			String line = null;
			while ((line = br.readLine()) != null) {
				atomicIntegerSeed = Integer.valueOf( line.substring(0, line.indexOf(delimPipe)).trim());
				globalId = new AtomicInteger(atomicIntegerSeed);
				break;		// only 1 line in file so exit loop
			}

			String tempMessage = "Global Id starting seed: " + atomicIntegerSeed;
			writeLog( tempMessage );
			System.out.println( tempMessage );
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		// try with resources --> autoclose

	}

	public static void writeAtomicIntegerSeed() {
		
		File fileOut = new File(atomicIntegerPath);
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileOut)) ) {
		
			Integer currGlobalId = globalId.get();		// get (but don't increment) current global Id
			String saveRecord = Integer.toString(currGlobalId) +" | "+ dateNowFormatted();
			bw.write(saveRecord);  		// write out record to be read in next restart
			
			String tempMessage = "Global Id ending seed: " + currGlobalId;
			writeLog( tempMessage );
			System.out.println( tempMessage );
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void displayUsage() {
		System.out.println("This Global Patient Match application supports these process steps:");
		System.out.println("Step 1 Process all input files creating MasterFile1");
		System.out.println("Step 2 Sort MasterFile1 creating MasterFile2");
		System.out.println("Step 3 Assign global ids to MasterFile2 creating MasterFile3");
		System.out.println("Step 4 Sort MasterFile3 creating MasterFile4");
		System.out.println("Step 5 Assign global ids to MasterFile4 creating MasterFile5");
	}
	
	public static void main(String[] args) throws IOException {
		String argumentOne = "noargs";
		processStep = 0;
		try {
			argumentOne = args[0];
			processStep = Integer.valueOf(argumentOne);
		}
		catch (ArrayIndexOutOfBoundsException e){
			System.out.println("no args[] found on startup");
		}
		finally {
		}
		
		//processStep = 1;	//****** testing
		//processStep = 2;
		//processStep = 3;
		//processStep = 4;
		//processStep = 5;
		
		if (processStep <= 0 || processStep > 5) {
			System.out.println("Please provide process step number to execute.");
			displayUsage();
			return;
		}
		
		System.out.println("Starting Global Patient Match process step " + processStep);
		
		// read configuration properties file from config subdirectory
		configRootPath = System.getenv("GLOBAL_MATCH_BASE");		// read root dir from System environment variable
		System.out.println("System environment variable GLOBAL_MATCH_BASE: " + configRootPath);
		configRootPath = changeDirectorySeparator(configRootPath);		// change file separator if Windows
		configFilePath = makeFilePath(makeFilePath(configRootPath, "config"), configFileName);  // get config path
		atomicIntegerPath = makeFilePath(makeFilePath(configRootPath, "config"), atomicIntegerFile);  // get path
		readConfig(configFilePath);			// read config file

		switch (processStep) {
		case 1:
			processGlobalPatientMatchIntake(processStep);	// global match intake --> MasterFile1
			break;
		case 2:
			sortMasterFile1(processStep);		// sort MasterFile1 --> MasterFile2
			break;
		case 3:
			assignGlobalIds2(processStep);		// assign global ids to MasterFile2 --> MasterFile3
			break;
		case 4:
			sortMasterFile3(processStep);		// sort MasterFile3 --> MasterFile4
			break;
		case 5:
			assignGlobalIds4(processStep);		// assign global ids to MasterFile4 --> MasterFile5
			break;
		default :
			System.out.println("***Invalid process step found");
			System.exit(0); 	// exit if not right processStep
		}
	}
}