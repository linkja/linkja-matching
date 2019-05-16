package org.linkja.matching;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

public class GlobalMatchSqlite {

	private static final String SW_NAME    = "Global Patient Match";
	private static final String SW_VERSION = " v1.0.beta";
	private static final String SQL_Exception1 = "SQLITE_BUSY";
	//private static final String File_Extension_CSV = ".csv";
	private static final String File_Extension_TXT = ".txt";
	private static String dbDirectory;
	private static String dbName;
	private static Connection db;
	
	private static final String PROJECT_ROOT = "%ProjectRoot%";
	private static String configRootPath;
	private static String configFilePath;
	private static final String configFileName = "global-match.properties";
	private static String inputFileNamePrefix;
	private static String inputFileNameSuffix;
	private static String inputDir;
	private static String outputDir;
	private static String processedDir;
	private static String logFile;
	private static String logFileDir;
	private static final String logFileName = "match-log-";
	private static final String reportFileName = "report1-";
	private static final String directorySeparator = "/";
	private static final String delimPipe = "|";
	private static final String delimComma = ",";
	private static final DateTimeFormatter dateTimeformat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	private static final DateTimeFormatter dateTimeHL7format = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

	private static int siteidIdx = 0;				// index into columns in input hash files
	private static int projectidIdx = 1;
	private static int pidhashIdx = 2;
	private static int hash1Idx = 3;
	private static int hash2Idx = 4;
	private static int hash3Idx = 5;
	private static int hash4Idx = 6;
	private static int hash5Idx = 7;
	private static int hash6Idx = 8;
	private static int hash7Idx = 9;
	private static int hash8Idx = 10;
	private static int hash9Idx = 11;
	private static int hash10Idx = 12;
	private static int exceptFlagIdx = 13;
	private static int recordsToSkip = 1;
	private static AtomicInteger globalId;
	private static int atomicIntegerSeed;
	private static String atomicIntegerPath;
	private static final String atomicIntegerFile = "global-match-globalId.txt";
	private static Integer patAliasGlobalIdCutoff = 0;
	private static final int logInfo = 0;		// normal log message
	private static final int logWarning = 1;	// warning log message
	private static final int logSevere = 2;		// severe log message
	private static final int logTask = 3;		// begin task log message
	private static final int logBegin = 4;		// begin section log message
	private static final int logSection = 5;	// begin subsection log message
	private static final String TempTableIdKey = "TempTableId";
	private static final String TempTableAllKey = "TempTableAllIndex";
	private static final String nullEntry = "NULL";
	private static boolean tempTableAllKeyCreated = false;
	private static String tempMessage;
	
	private Integer patGlobalIdCount = 0;
	private Map<Integer, Set<Integer>> patGlobalIdMap;	// holds patient matches
	private Map<Integer, Integer> patGlobalIdMapLink;	// holds link to patient matches
	private List<String> exclusionPats;					// holds pats with exclusion flag
	private static List<Integer> matchSequence;			// holds match rules to run

	protected final static Map<Integer, String> matchRule = new HashMap<Integer, String>();
	static {
		matchRule.put(0, "1,2,3,4,5,6,7,8,9,10 matchto 1,2,3,4,5,6,7,8,9,10");
		matchRule.put(1, "1,2,5,9,10 matchto 1,2,5,9,10");
		matchRule.put(2, "3,4,6 matchto 3,4,6");
		matchRule.put(3, "1 matchto 1");
		matchRule.put(4, "1 matchto 2");
		matchRule.put(5, "1 matchto 5");
		matchRule.put(6, "1 matchto 9");
		matchRule.put(7, "1 matchto 10");
		matchRule.put(8, "3 matchto 3");
		matchRule.put(9, "3 matchto 4");
		matchRule.put(10, "3 matchto 6");
		matchRule.put(11, "7 matchto 7");
		matchRule.put(12, "8 matchto 8");
	}
	
	class GlobalIdGroup {
		Integer groupGlobalId;
		Set<Integer> groupSet;
	}
	
	// constructor
	public GlobalMatchSqlite(String projectRoot, Integer idSeed) {
		System.out.println("Project Root path passed to constructor: " + projectRoot);
		matchSequence = new ArrayList<Integer>();	// holds match rules to run
		
		// read configuration properties file from config subdirectory
		configRootPath = changeDirectorySeparator(projectRoot);		// change file separator if Windows
		if (configRootPath.endsWith(directorySeparator)) {
			configRootPath = configRootPath.substring(0, configRootPath.length() - 1 ); // remove last / 
		}
		inputFileNamePrefix = "";
		inputFileNameSuffix = "";
		readConfig(configRootPath);		// read config file and assign local variables
		
		if (idSeed > 0) {				// if user value passed in, override config global id seed 
			atomicIntegerSeed = idSeed;
			globalId = new AtomicInteger(atomicIntegerSeed);	// set base Global id
		}
	}

	private String changeDirectorySeparator(String filePath) {
		return filePath.replaceAll("\\\\", directorySeparator);	// change dir separator if Windows
	}
	public String timeNowFormatted() {
		LocalDateTime now = LocalDateTime.now();		//Get current date time
		return now.format( dateTimeformat );
	}
	public String dateNowFormatted() {
		LocalDate now = LocalDate.now();				//Get current date
		return now.format(DateTimeFormatter.ISO_DATE);
	}
	public String dateTimeNowHL7() {
		LocalDateTime now = LocalDateTime.now();		//Get current date time in HL7 format
		return now.format( dateTimeHL7format );
	}
	/**
	 * Connect to SQLite database
	 */
	public void connectDb() {
		db = null;
		try {
			String url = "jdbc:sqlite:" + dbDirectory + dbName;		// create url to database
			writeLog(logInfo, "connecting to url: " + url, true);
			db = DriverManager.getConnection(url);					// create connection to database
			System.out.println("Connection to SQLite has been established.");
			
			SQLiteConfig config = new SQLiteConfig();
			config.setCacheSize(12000);					// set some helpful config values
			//config.setPageSize(8192);
			config.setPageSize(16384);
			config.setJournalMode(SQLiteConfig.JournalMode.OFF);
			config.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
			config.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
			config.setTransactionMode(SQLiteConfig.TransactionMode.EXCLUSIVE);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
		} finally {
		}
	}
	
	public void doAutoCommit(boolean autoCommitFlag) {
		try {
			db.setAutoCommit(autoCommitFlag);	// turn off AutoCommit to make storing faster
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	}

	public void processInputFiles(int step, String prefix, String suffix) {
		exclusionPats = new ArrayList<String>();		// holds list of exclusion pats
		String fullName = null;
		tempMessage = "Step " + step + ": reading input files from: " + inputDir;
		writeLog( logBegin, tempMessage, true);
		
		if (!prefix.isEmpty() || !suffix.isEmpty()) {
			inputFileNamePrefix = prefix;	// if prefix, suffix passed in, override config
			inputFileNameSuffix = suffix;
		}
		// read hash files from input directory
		ArrayList<String> inputFiles = getFileNames1Dir(inputDir, inputFileNamePrefix, inputFileNameSuffix); // go read files in directory
		if (inputFiles == null || inputFiles.size() == 0) {
			tempMessage = "no input files found in directory " + inputDir;
			writeLog(logWarning, tempMessage, true);
		}
		tempTableAllKeyCreated = false;		// set flag so temp file has to be recreated
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		
		for (int i=0; i< inputFiles.size(); i++) {

			fullName = inputFiles.get(i);					// get next file in list				
			fullName = changeDirectorySeparator(fullName);	// change file separator if Windows
			String inputFile1 = fullName;
			String invalidDataFile = changeFileExtension(inputFile1, "bad");
			int recordsRead = 0;
			int commitCount = 0;
			tempMessage = "processing hash file: " + inputFile1;	// indicate input file
			writeLog(logSection, tempMessage, true);
			
			boolean useCommaDelim = true;
			if (inputFile1.endsWith(File_Extension_TXT)) {
				useCommaDelim = false;			// check file delimiter to use
			}
			doAutoCommit(false);	// turn off AutoCommit to make storing faster
			
			File fileIn = new File(inputFile1);
			try ( BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileIn), "UTF-8"))) {
				final int batchSize = 1000;		// number of records stored before commits	
				recordsRead = 0;
				
				String line = null;
				while ((line = br.readLine()) != null) {		// read until end of file
					recordsRead++;
					if (recordsRead <= recordsToSkip) {
						System.out.println("skipping record " + recordsRead);
						continue;
					}
					String[] splitLine;
					String lineIn = line.trim().replaceAll("\\s+", " "); // reduce multiple spaces to single space
					if (useCommaDelim) {
						splitLine = lineIn.split(delimComma);	//split incoming text by , delimiter
					} else {
						splitLine = lineIn.split("\\|");		//split incoming text by | delimiter
					}

					// check that primary data is valid  - site id, project id and pidhash must be filled in
					boolean validData = true;
					if (splitLine[siteidIdx] == null || splitLine[siteidIdx].isEmpty() || splitLine[siteidIdx].equals("NULL")) {
						validData = false;
					}
					if (splitLine[projectidIdx] == null || splitLine[projectidIdx].isEmpty() || splitLine[projectidIdx].equals("NULL")) {
						validData = false;
					}
					if (splitLine[pidhashIdx] == null || splitLine[pidhashIdx].isEmpty() || splitLine[pidhashIdx].equals("NULL")) {
						validData = false;
					}
					if (splitLine.length <= exceptFlagIdx || splitLine[exceptFlagIdx] == null || splitLine[exceptFlagIdx].isEmpty()) {
						validData = false;
					} else if (!splitLine[exceptFlagIdx].equals("0")) {
						exclusionPats.add(lineIn);		// add this record to exclusionPats
						continue;						// skip to next record
					}
					/* columns hash1 and up can be null or empty */
					if (!validData) {
						writeInvalidData( invalidDataFile, lineIn );	// write out bad data for later
						continue;			// skip to next record
					}

					String sql = "INSERT INTO InclusionPatients ("
							+"siteId,projectId,pidhash,hash1,hash2,hash3,hash4,hash5,hash6,hash7,hash8,hash9,hash10,exclusion,globalId) "
							+ "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

					try ( PreparedStatement pstmt = db.prepareStatement(sql)) {
						pstmt.setString(1, splitLine[siteidIdx]);
						pstmt.setString(2, splitLine[projectidIdx]);
						pstmt.setString(3, splitLine[pidhashIdx]);
						pstmt.setString(4, splitLine[hash1Idx]);
						pstmt.setString(5, splitLine[hash2Idx]);
						pstmt.setString(6, splitLine[hash3Idx]);
						pstmt.setString(7, splitLine[hash4Idx]);
						pstmt.setString(8, splitLine[hash5Idx]);
						pstmt.setString(9, splitLine[hash6Idx]);
						pstmt.setString(10, splitLine[hash7Idx]);
						pstmt.setString(11, splitLine[hash8Idx]);
						pstmt.setString(12, splitLine[hash9Idx]);
						pstmt.setString(13, splitLine[hash10Idx]);
						pstmt.setString(14, splitLine[exceptFlagIdx]);
						pstmt.setInt(15, 0);

						pstmt.executeUpdate();			// store data to database
						commitCount++;
						if (commitCount % batchSize == 0) {
							db.commit();	// if at batchSize then do commit since autocommit is off
						}
					} catch (SQLException e) {
						System.out.println(e.getMessage());
						if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
					}
					if ((recordsRead % 10000) == 0) {
						writeLog(logInfo, recordsRead+" records read", true);
					}
				}
				if (commitCount > 0) {
					try {
						db.commit();
						doAutoCommit(true);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			} catch (UnsupportedEncodingException e2) {
				e2.printStackTrace();
			} catch (FileNotFoundException e2) {
				e2.printStackTrace();
			} catch (IOException e2) {
				e2.printStackTrace();
			}
			tempMessage = "read " + recordsRead + " lines from: " + inputFile1;
			writeLog(logInfo, tempMessage, true);
			// end read input file

			File fileToMove = FileUtils.getFile(fullName);	// move file to processed dir
			if (fileToMove.exists()) {
				boolean fileMoved = false;
				try {
					fileMoved = moveFileToDir(processedDir, fullName);	// copy file to processed dir
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (!fileMoved) {
					tempMessage = "file not moved to processed dir: " + fullName;
					writeLog(logSevere, tempMessage, true);
				}
			}
		}
		doAutoCommit(true);		// turn AutoCommit back on

		saveExclusionPatients(exclusionPats);	// go save exclusion patients
		resetGlobalMatchTable();		// clear GlobalMatch Table and import from InclusionPatients
		indexGlobalMatchTable();		// go index GlobalMatch
	}
	
	public void saveExclusionPatients(List<String> exclusionPats) {
		tempMessage = "storing " + exclusionPats.size() + " exclusion patient(s)";
		writeLog(logInfo, tempMessage, true);
		
		for (String entry : exclusionPats) {
			String[] splitLine = null;
			if (entry.contains(delimComma)) {
				splitLine = entry.split(delimComma);	//split incoming text by , delimiter
			} else {
				splitLine = entry.split("\\|");			//split incoming text by | delimiter
			}
			String sql = "INSERT INTO ExclusionPatients ("
				+"globalId,siteId,projectId,pidhash,hash1,hash2,hash3,hash4,hash5,hash6,hash7,hash8,hash9,hash10,exclusion) "
				+ "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

			try ( PreparedStatement pstmt = db.prepareStatement(sql)) {
				pstmt.setInt(1, 0);
				pstmt.setString(2, splitLine[siteidIdx]);
				pstmt.setString(3, splitLine[projectidIdx]);
				pstmt.setString(4, splitLine[pidhashIdx]);
				pstmt.setString(5, splitLine[hash1Idx]);
				pstmt.setString(6, splitLine[hash2Idx]);
				pstmt.setString(7, splitLine[hash3Idx]);
				pstmt.setString(8, splitLine[hash4Idx]);
				pstmt.setString(9, splitLine[hash5Idx]);
				pstmt.setString(10, splitLine[hash6Idx]);
				pstmt.setString(11, splitLine[hash7Idx]);
				pstmt.setString(12, splitLine[hash8Idx]);
				pstmt.setString(13, splitLine[hash9Idx]);
				pstmt.setString(14, splitLine[hash10Idx]);
				pstmt.setString(15, splitLine[exceptFlagIdx]);
				
				pstmt.executeUpdate();			// store to database
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
			}
		}
	}
	
	public void indexGlobalMatchTable() {
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		String[] indexSql = {
			"CREATE INDEX match0 ON GlobalMatch (hash1,hash2,hash3,hash4,hash5,hash6,hash7,hash8,hash9,hash10,id);",
			"CREATE INDEX match1 ON GlobalMatch (hash1,hash2,hash5,hash9,hash10,id);",
			"CREATE INDEX match2 ON GlobalMatch (hash3,hash4,hash6,id);",
			"CREATE INDEX match3 ON GlobalMatch (hash1,id);",
			"CREATE INDEX match4 ON GlobalMatch (hash1,hash2,id);",
			"CREATE INDEX match5 ON GlobalMatch (hash1,hash5,id);",
			"CREATE INDEX match6 ON GlobalMatch (hash1,hash9,id);",
			"CREATE INDEX match7 ON GlobalMatch (hash1,hash10,id);",
			"CREATE INDEX match8 ON GlobalMatch (hash3,id);",
			"CREATE INDEX match9 ON GlobalMatch (hash3,hash4,id);",
			"CREATE INDEX match10 ON GlobalMatch (hash3,hash6,id);",
			"CREATE INDEX match11 ON GlobalMatch (hash7,id);",
			"CREATE INDEX match12 ON GlobalMatch (hash8,id);",
			"CREATE INDEX pidindex ON GlobalMatch (pidhash,siteId,projectId,id);",
			"CREATE VIEW report1 AS SELECT siteId, projectId, pidhash, globalId FROM GlobalMatch;" };

		for (int i = 0; i < indexSql.length; i++) {
			tempMessage = "Indexing GlobalMatch Table Index " + i;
			writeLog(logTask, tempMessage, true);
			try ( PreparedStatement pstmt1 = db.prepareStatement(indexSql[i]) ) {
				pstmt1.executeUpdate();				// update this record
				try { Thread.sleep(1000); } catch (InterruptedException e) {}  // pause to let catch up
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
			}
		}
		//try { db.commit(); } catch (SQLException e) { e.printStackTrace(); }
		tempMessage = "finished indexing GlobalMatch";
		writeLog(logTask, tempMessage, true);
	}
	
	public void dropIndexGlobalMatchTable() {
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		String[] indexSql = {
			"DROP INDEX IF EXISTS match0;",
			"DROP INDEX IF EXISTS match1;",
			"DROP INDEX IF EXISTS match2;",
			"DROP INDEX IF EXISTS match3;",
			"DROP INDEX IF EXISTS match4;",
			"DROP INDEX IF EXISTS match5;",
			"DROP INDEX IF EXISTS match6;",
			"DROP INDEX IF EXISTS match7;",
			"DROP INDEX IF EXISTS match8;",
			"DROP INDEX IF EXISTS match9;",
			"DROP INDEX IF EXISTS match10;",
			"DROP INDEX IF EXISTS match11;",
			"DROP INDEX IF EXISTS match12;",
			"DROP INDEX IF EXISTS pidindex;",
			"DROP VIEW IF EXISTS report1;" };

		for (int i = 0; i < indexSql.length; i++) {
			tempMessage = "dropping GlobalMatch Table Index " + i;
			writeLog(logTask, tempMessage, true);
			try ( PreparedStatement pstmt1 = db.prepareStatement(indexSql[i]) ) {
				pstmt1.executeUpdate();				// execute sql
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
			}
		}
		tempMessage = "finished dropping indexes on GlobalMatch";
		writeLog(logTask, tempMessage, true);
	}

	public void resetGlobalMatchTable() {
		tempMessage = "Clearing GlobalMatch Table";
		writeLog(logTask, tempMessage, true);
		int count = 0;
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		
		String sqlDelete = "DELETE FROM GlobalMatch";	// delete existing records
		try ( PreparedStatement pstmt1 = db.prepareStatement(sqlDelete) ) {
			count = pstmt1.executeUpdate();				// update this record
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
		}
		tempMessage = count + " rows deleted from GlobalMatch";
		writeLog(logTask, tempMessage, true);
		
		transferInclusionPatients();	// go load GlobalMatch table from InclusionPatients
	}

	public void transferInclusionPatients() {
		tempMessage = "transferring Inclusion Patient(s) to GlobalMatch table";
		writeLog(logInfo, tempMessage, true);
		int recordsRead = 0;
		int commitCount = 0;
		final int batchSize = 1000;	// number of records stored before commits
		if (db == null) {
			connectDb();			// connect to database if no connection yet
		}
		dropIndexGlobalMatchTable();	// drop indexes for faster insert
		doAutoCommit(false);			// turn off AutoCommit to make storing faster

		String sql1 = "SELECT globalId,siteId,projectId,pidhash,hash1,hash2,hash3,hash4,hash5,hash6,hash7,hash8,hash9,hash10,exclusion FROM InclusionPatients";
		String sql2 = "INSERT INTO GlobalMatch ("
						+ "globalId,siteId,projectId,pidhash,hash1,hash2,hash3,hash4,hash5,hash6,hash7,hash8,hash9,hash10,exclusion) "
						+ "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		try (	PreparedStatement pstmt1 = db.prepareStatement(sql1);
				PreparedStatement pstmt2 = db.prepareStatement(sql2) ) {
			pstmt1.setFetchSize(1000);				//number of rows to be fetched when needed
			ResultSet resultSet1 = pstmt1.executeQuery();	// do query
			while (resultSet1.next()) {

				recordsRead++;
				pstmt2.setInt(1, resultSet1.getInt("globalId"));
				pstmt2.setString(2, resultSet1.getString("siteId"));
				pstmt2.setString(3, resultSet1.getString("projectId"));
				pstmt2.setString(4, resultSet1.getString("pidhash"));
				pstmt2.setString(5, resultSet1.getString("hash1"));
				pstmt2.setString(6, resultSet1.getString("hash2"));
				pstmt2.setString(7, resultSet1.getString("hash3"));
				pstmt2.setString(8, resultSet1.getString("hash4"));
				pstmt2.setString(9, resultSet1.getString("hash5"));
				pstmt2.setString(10, resultSet1.getString("hash6"));
				pstmt2.setString(11, resultSet1.getString("hash7"));
				pstmt2.setString(12, resultSet1.getString("hash8"));
				pstmt2.setString(13, resultSet1.getString("hash9"));
				pstmt2.setString(14, resultSet1.getString("hash10"));
				pstmt2.setString(15, resultSet1.getString("exclusion"));

				pstmt2.executeUpdate();			// store data to database
				commitCount++;
				if (commitCount % batchSize == 0) {
					db.commit();				// if at batchSize then do commit
				}
				if ((recordsRead % 10000) == 0) {
					writeLog(logInfo, recordsRead+" records transferred to GlobalMatch", true);
				}
			}
			if (resultSet1 != null) {
				resultSet1.close();
			}
			db.commit();
			db.setAutoCommit(true);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
		}
		writeLog(logInfo, recordsRead+" records transferred to GlobalMatch", true);
	}

	/*  Exclusion patients not transferred to avoid complications in matching
	public void transferExclusionPatients() {
		int count = 0;
		String sql1 = 		
		"INSERT INTO GlobalMatch (globalId,siteId,projectId,pidhash,hash1,hash2,hash3,hash4,hash5,hash6,hash7,hash8,hash9,hash10,exclusion) "+
		"SELECT globalId,siteId,projectId,pidhash,hash1,hash2,hash3,hash4,hash5,hash6,hash7,hash8,hash9,hash10,exclusion FROM ExclusionPatients";
		try ( PreparedStatement pstmt1 = db.prepareStatement(sql1)) {
			count = pstmt1.executeUpdate();			// store to database
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
		tempMessage = "transferring " + count + " Exclusion Patient(s) to GlobalMatch table";
		writeLog(logInfo, tempMessage, true);
	}
	*/

	// step 2 run match rules as indicated
	public void runMatchRules(int step, List<Integer> ruleList) {
		tempMessage = "Step " + step + ": Run patient match rule sequence " + ruleList;
		writeLog(logBegin, tempMessage, true);
		Integer currGlobalId = globalId.get();		// get (but don't increment) current global Id
		tempMessage = "Global Id starting seed: " + currGlobalId;
		writeLog(logInfo, tempMessage, true);
		if (db == null) {
			connectDb();				// connect to database if no connection yet
		}
		patGlobalIdMap = new HashMap<Integer, Set<Integer>>(750000, 0.90f);	//declare initial capacity and load factor
		patGlobalIdMapLink = new HashMap<Integer, Integer>(750000, 0.90f);	//declare initial capacity and load factor
		

		resetGlobalIds();				// go reset globalIds in GlobalMatch to 0
		assignPatientAliasGlobalIds();	// assign Global Ids to derived patients (aliases) first
		
		for (Integer currentRule : ruleList) {		// run match rules requested

			String ruleTextFull = matchRule.get( currentRule );
			int matchToIndex = ruleTextFull.indexOf(" matchto ");
			String key1Part = ruleTextFull.substring(0, matchToIndex);
			String key2Part = ruleTextFull.substring(matchToIndex + 9);
			boolean keySame = false;		// indicates if key1 = key2
			if (key1Part.equals(key2Part)) {						
				keySame = true;
			}
			tempMessage = "Processing Match Rule " + currentRule;
			writeLog(logInfo, tempMessage, true);
			
			switch (currentRule) {		
			case 0 :
				keySame = true;
				runGlobalMatchRule0( keySame );	// go run match rule indicated
				break;
			case 1 :
				keySame = true;
				runGlobalMatchRule1( keySame );
				break;
			case 2 :
				keySame = true;
				runGlobalMatchRule2( keySame );
				break;
			case 3 :
				keySame = true;
				runGlobalMatchRule3( keySame );
				break;
			case 4 :
				keySame = false;
				runGlobalMatchRule4( keySame );
				break;
			case 5 :
				keySame = false;
				runGlobalMatchRule5( keySame );
				break;
			case 6 :
				keySame = false;
				runGlobalMatchRule6( keySame );
				break;
			case 7 :
				keySame = false;
				runGlobalMatchRule7( keySame );
				break;
			case 8 :
				keySame = true;
				runGlobalMatchRule8( keySame );
				break;
			case 9 :
				keySame = false;
				runGlobalMatchRule9( keySame );
				break;
			case 10 :
				keySame = false;
				runGlobalMatchRule10( keySame );
				break;
			case 11 :
				keySame = true;
				runGlobalMatchRule11( keySame );
				break;
			case 12 :
				keySame = true;
				runGlobalMatchRule12( keySame );
				break;
			default :
			}
			writeLog(logInfo, "finished Match Rule " + currentRule, true);
			//System.out.println("matching pats:");
			//patGlobalIdMap.forEach((k, v) -> System.out.println((k + ":" + v)));	// print out - Java 8
		}

		assignMatchedGlobalIds();		// go assign global ids for matched patients
		assignUnMatchedGlobalIds();		// go assign global ids for unmatched patients
		writeAtomicIntegerSeed();		// go save current value of next globalId
		
		//if (db != null) {				// don't close to allow multiple runs of match rules
		//	try {db.close();} catch (SQLException e) { /* ignored */} }
	}
	
	public void resetGlobalIds() {
		tempMessage = "Resetting all GlobalMatch global ids to 0";
		writeLog(logTask, tempMessage, true);
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		String sqlUpdate = "UPDATE GlobalMatch SET globalId = 0 WHERE globalId > 0";
		try ( PreparedStatement pstmt = db.prepareStatement(sqlUpdate)) {
			pstmt.setFetchSize(1000);		// number of rows to be fetched when needed
			pstmt.executeUpdate();			// execute sql statement
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
		}
		tempMessage = "finished resetting all GlobalMatch global ids to 0";
		writeLog(logTask, tempMessage, true);
	}

	public void assignMatchedGlobalIds() {
		int assignedGlobalIds = 0;
		int patGlobalIdMapCount = 0;
		tempMessage = "Assigning global ids to matching patients";
		writeLog(logTask, tempMessage, true);
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		patGlobalIdMapCount = patGlobalIdMap.size();	// get size of matched pat groups
		tempMessage = "Found " + patGlobalIdMapCount + " matching patient groups";
		writeLog(logInfo, tempMessage, true);
		if (patGlobalIdMapCount == 0) {		// quit now if no matched patients
			return;
		}
		// consolidate matching patients from patGlobalIdMap
		ArrayList<Set<Integer>> patIdArray = new ArrayList<Set<Integer>>(750000);	// holds match groups of unique pats

		patGlobalIdMapCount = 0;
		for (Map.Entry<Integer, Set<Integer>> entry : patGlobalIdMap.entrySet()) { // loop thru each entry in map
			patGlobalIdMapCount++;
			//Integer key1 = entry.getKey();
			Set<Integer> patSet = entry.getValue();	// get matching pats in this group
			patIdArray.add(patSet);					// add new entry in consolidated array
		}
		tempMessage = "consolidated matching patient groups to " + patIdArray.size();
		writeLog(logInfo, tempMessage, true);
		
		patGlobalIdMap.clear(); 		// clear hashmap to save memory
		patGlobalIdMapLink.clear();		// clear to save memory
		ArrayList<GlobalIdGroup> globalIdArray = new ArrayList<GlobalIdGroup>(750000); //holds matching pats and globalID
		StringBuilder sb1 = new StringBuilder();	// holds string with all pat ids of set
		
		// loop thru each consolidated set, find any existing global ids, give same global id to all in set
		for (int x = 0; x < patIdArray.size(); x++ ) {
			if ( x % 25000 == 0) {
				tempMessage = "getting globalId for matching patient group " + x;
				writeLog(logInfo, tempMessage, true);
			}

			Set<Integer> patSet = patIdArray.get( x );	// get next set of matched patients
			sb1.setLength( 0 );							// clear previous entry
			int patCount = 0;
			for (Integer num : patSet) {
				patCount++;
				if (patCount == 1) {
					sb1.append(num.toString());
				} else {
					sb1.append(delimComma + num.toString());
				}
			}
			Integer maxGlobalId = 0;
			String sql1 = "SELECT globalId FROM GlobalMatch WHERE id IN (" + sb1.toString() + ")"; // create sql 
			try ( PreparedStatement pstmt1 = db.prepareStatement(sql1)) {	// check if any already have global id
				pstmt1.setFetchSize(100);				//number of rows to be fetched when needed
				ResultSet resultSet = pstmt1.executeQuery();
				while (resultSet.next()) {				// get max global Id from this group
					Integer currGlobalId = resultSet.getInt("globalId");
					if (currGlobalId > 0) {
						if (currGlobalId < patAliasGlobalIdCutoff) {
							maxGlobalId = currGlobalId;		// if alias pat global id found, use that for all
							break;							// exit loop with alias pat global id
						} else if (maxGlobalId < currGlobalId) {
							maxGlobalId = currGlobalId;		// else keep track of max globalId
						}
					}
				}
				if (resultSet != null) {
					resultSet.close();
				}
			//} catch (SQLiteException e) {
				//System.out.println("SQLite error code: " + e.getErrorCode());
				//if (e.getErrorCode() == SQLiteErrorCode.SQLITE_BUSY.code) { }
				//if (e.getErrorCode() == SQLiteErrorCode.SQLITE_CONSTRAINT.code) { }
				//if (code.equals(SQLiteErrorCode.SQLITE_NOTADB) || code.equals(SQLiteErrorCode.SQLITE_CORRUPT)) { }
				//stopProcess(e.getMessage());
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
			} catch (Exception e) {
				System.out.println("**Exception while checking for max globalId");
				stopProcess(e.getMessage());
			}

			if (maxGlobalId == 0) {							// if no global id found
				maxGlobalId = globalId.incrementAndGet();	// get next global Id
				assignedGlobalIds++;	// increment count
			}
			
			GlobalIdGroup currGroup = new GlobalIdGroup();
			currGroup.groupGlobalId = maxGlobalId;			// save globalId and this pat set
			currGroup.groupSet = patSet;
			globalIdArray.add(currGroup);
		}
		
		// have matching patient groups with group globalId so update each pat with assigned globalId
		patIdArray.clear();				// clear array to save memory
		doAutoCommit(false);			// turn off AutoCommit to make storing faster
		final int batchSize = 1000;		// number of updates before commits
		int commitCount = 0;
		
		for (int k = 0; k < globalIdArray.size(); k++ ) {
			if ( k % 25000 == 0) {
				tempMessage = "updating globalId for matching patient group " + k;
				writeLog(logInfo, tempMessage, true);
			}
			GlobalIdGroup nextGroup = globalIdArray.get( k );	// get next set of matched patients
			Integer nextGlobalId = nextGroup.groupGlobalId;
			Set<Integer> nextSet = nextGroup.groupSet;
			
			for (Integer num : nextSet) {	// loop thru each pat in set to update global id
				String sql2 = "UPDATE GlobalMatch SET globalId = ? WHERE id = ?";
				try ( PreparedStatement pstmt2 = db.prepareStatement(sql2)) {
					pstmt2.setInt(1, nextGlobalId);		// set corresponding params
					pstmt2.setInt(2, num);
					pstmt2.executeUpdate();				// update this record 
					
					commitCount++;
					if (commitCount % batchSize == 0) {
						db.commit();	// if at batchSize then do commit since autocommit is off
					}
				//} catch (SQLiteException e) {
					//System.out.println("SQLite error code: " + e.getErrorCode());
					//System.out.println(e.getMessage());
					//if (e.getErrorCode() == SQLiteErrorCode.SQLITE_BUSY.code) { }
					//stopProcess(e.getMessage());
				} catch (SQLException e) {
					System.out.println(e.getMessage());
					if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
				} catch (Exception e) {
					System.out.println("**Exception while assigning globalId to matching patients");
					System.out.println(e.getMessage());
					stopProcess(e.getMessage());
				}
			}
		}
		try {
			db.commit();
			doAutoCommit(true);		// turn AutoCommit back on
		} catch (Exception e) {
			e.printStackTrace();
			stopProcess(e.getMessage());
		}
		tempMessage = "Assigned " + assignedGlobalIds + " new global ids to matching patients";
		writeLog(logInfo, tempMessage, true);
	}

	public void assignUnMatchedGlobalIds() {
		int unAssignedGlobalIds = 0;
		tempMessage = "Assigning global ids to unmatched patients";
		writeLog(logTask, tempMessage, true);
		
		createTempTableIdKey();		// go create temp table to hold pats without globalId
		int commitCount = 0;
		final int batchSize = 1000;	// number of records stored before commits
		doAutoCommit(false);		// turn off AutoCommit to make storing faster

		String sql3 = "SELECT id FROM GlobalMatch WHERE globalId = 0";	// get pats without globalId
		try ( PreparedStatement pstmt1 = db.prepareStatement(sql3)) {	
			pstmt1.setFetchSize(1000);				//number of rows to be fetched when needed
			ResultSet resultSet = pstmt1.executeQuery();
			while (resultSet.next()) {				// get row ids of unassigned patients, save to temp 
				Integer rowId = resultSet.getInt("id");
				Integer nextGlobalId = globalId.incrementAndGet();	// assign next global id
				storeToTempTableIdKey(rowId, nextGlobalId);			// save to temp file
				unAssignedGlobalIds++;
				
				commitCount++;
				if (commitCount % batchSize == 0) {
					db.commit();			// if at batchSize then do commit
				}
			}
			if (resultSet != null) {
				resultSet.close();
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
		}
		try {
			db.commit();
			db.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		tempMessage = "Assigning " + unAssignedGlobalIds + " global ids to patients without global ids";
		writeLog(logInfo, tempMessage, true);

		// update GlobalMatch.globalId using rowId, globalId from TempTable
		String sql2 = "UPDATE GlobalMatch SET globalId ="
				+ " (SELECT " + TempTableIdKey + ".gblId FROM " + TempTableIdKey
				+ " WHERE GlobalMatch.id = " + TempTableIdKey + ".rowId)"
				+ " WHERE GlobalMatch.globalId = 0";

		try ( PreparedStatement pstmt2 = db.prepareStatement(sql2)) {
			pstmt2.executeUpdate();				// update GlobalMatch record from temp table 
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
		}
	}
	
	public void assignPatientAliasGlobalIds() {
		Map<String, Integer> patAliasMap = new HashMap<String, Integer>(1000);
		String lastPat = "xx";
		int recordsRead = 0;
		int patAliasMapCount = 0;
		if (db == null) {
			connectDb();	// connect to database if no connection yet
		}
		tempMessage = "patient alias match starting";
		writeLog(logBegin, tempMessage, true);

		// query database
		String sqlQuery = "SELECT pidhash,siteId,projectId,id FROM GlobalMatch INDEXED BY pidindex";	// create prepared statement
		try ( PreparedStatement pstmt = db.prepareStatement(sqlQuery)) {
			pstmt.setFetchSize(1000);						//number of rows to be fetched when needed
			ResultSet resultSet = pstmt.executeQuery();		// executeQuery
			int columnCount = resultSet.getMetaData().getColumnCount();		// get column count
			while (resultSet.next()) {

				recordsRead++;
				StringBuilder sb1 = new StringBuilder();
				for (int k = 1; k < columnCount; k++) {		//make up key to compare, all but last column
					sb1.append(resultSet.getString(k) + delimPipe);
				}
				String currentPat = sb1.toString();			// get all parts to key for pat, all required columns, should be non null
				//currRowId = resultSet.getInt("id");		// get row id in last column

				if (currentPat.equals(lastPat)) {			//if key is =, means patients match
					patAliasMapCount++;
					patAliasMap.merge(currentPat, 1, Integer::sum);	// add 1 to this pat, stores if not there
				}
				lastPat = currentPat;		// save current patient key for next loop
			}
			if (resultSet != null) {
				resultSet.close();
			}
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
			if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
		} catch (Exception e1) {
			e1.printStackTrace();	
		}
		tempMessage = "records processed: " +recordsRead+ "  pat alias matches found: " +patAliasMapCount;
		writeLog(logInfo, tempMessage, true);				// write to log

		Integer patAliasLastGlobalId = 0;
		// loop thru patAliasMap entries to assign global ids
		for (String patPidKey : patAliasMap.keySet()) {		// only need map key not value

			Integer nextGlobalId = globalId.incrementAndGet();	// get next global id
			patAliasLastGlobalId = nextGlobalId;

			String[] keyPart = patPidKey.split("\\|");	// split key

			String sql2 = "UPDATE GlobalMatch INDEXED BY pidindex "
					+ "SET globalId = ? WHERE pidhash = ? AND siteId = ? AND projectId = ?";
			try ( PreparedStatement pstmt2 = db.prepareStatement(sql2)) {
				pstmt2.setInt(1, nextGlobalId);		// set the corresponding params
				pstmt2.setString(2, keyPart[0]);	// set pid
				pstmt2.setString(3, keyPart[1]);	// set site id
				pstmt2.setString(4, keyPart[2]);	// set project id
				pstmt2.executeUpdate();				// update these record 
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
			}
		}
		if (patAliasLastGlobalId > 0) {
			patAliasGlobalIdCutoff = ((patAliasLastGlobalId + 100) /100) * 100;		// round up to nearest 100
			globalId = new AtomicInteger(patAliasGlobalIdCutoff);	// set regular global id above last alias global id
			tempMessage = "last pat alias Global Id: " +patAliasLastGlobalId+ ", setting base Global Id to: " +patAliasGlobalIdCutoff;
			writeLog(logInfo, tempMessage, true); 
		} else {
			tempMessage = "no pat alias Global Ids assigned, base Global Id is at: " + globalId.get();	// get (but don't increment)
			writeLog(logInfo, tempMessage, true);
		}
	}

	private void runGlobalMatchRule0(boolean keySame) {
		tempMessage = "Starting Match Rule 0";
		writeLog(logInfo, tempMessage, true);
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();		  // connect to database if no connection yet
		}
		createTempTableAllKey();  // go create and populate temp table to hold pats to compare
		
		String[] colName = new String[11];
		colName[1] = "hash1";
		colName[2] = "hash2";
		colName[3] = "hash3";
		colName[4] = "hash4";
		colName[5] = "hash5";
		colName[6] = "hash6";
		colName[7] = "hash7";
		colName[8] = "hash8";
		colName[9] = "hash9";
		colName[10] = "hash10";
		String[] indexName = new String[11];
		indexName[1] = "index1";
		indexName[2] = "index2";
		indexName[3] = "index3";
		indexName[4] = "index4";
		indexName[5] = "index5";
		indexName[6] = "index6";
		indexName[7] = "index7";
		indexName[8] = "index8";
		indexName[9] = "index9";
		indexName[10] = "index10";
		
		for (int k = 1; k < 11; k++) {		// loop thru each column individually, check for match
			int recordsRead = 0;
			int patientMatches = 0;
			String lastPat = "xx";
			Integer currRowId = 0;
			Integer lastRowId = 0;
			boolean currMatch = false;
			boolean lastMatch = false;
		
			String sql9 = "SELECT "+colName[k]+",rowId FROM " +TempTableAllKey+ " INDEXED BY " +indexName[k];
			try ( PreparedStatement pstmt9 = db.prepareStatement(sql9)) { 
				pstmt9.setFetchSize(1000);						//number of rows to be fetched when needed
				ResultSet resultSet = pstmt9.executeQuery();					// executeQuery
				//int columnCount = resultSet.getMetaData().getColumnCount();	// get column count
				String nullKey = nullEntry;
				while (resultSet.next()) {
					recordsRead++;
					String currPat = resultSet.getString(colName[k]);	// get pat
					currRowId = resultSet.getInt("rowId");				// get row id

					if (currPat.equals(nullKey) || currPat.isEmpty()) {
						currMatch = false;							// skip empty values
					} else if (currPat.equals(lastPat)) {
						currMatch = true;
						patientMatches++;
						if (!lastMatch) {
							currGlobalIdGroup.add(lastRowId);	//if match and not last match add both to set
							currGlobalIdGroup.add(currRowId);						
						} else {
							currGlobalIdGroup.add(currRowId);	// else matched before so lastPat already included
						}
					} else {
						currMatch = false;
						if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {  // if have entry, then save
							addToGlobalIdGroup( currGlobalIdGroup );
							currGlobalIdGroup.clear();					// clear list for next match
						}
					}
					lastPat = currPat;		// save current patient key for next loop
					lastMatch = currMatch;
					lastRowId = currRowId;

					if ((recordsRead % 50000) == 0) {
						tempMessage = "Match Rule 0 " +colName[k]+ " records processed: " + recordsRead + "  matches found: " + patientMatches;
						writeLog(logInfo, tempMessage, true);
					}
				}
				if (resultSet != null) {
					resultSet.close();
				}
				if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
					addToGlobalIdGroup( currGlobalIdGroup );
					currGlobalIdGroup.clear();					// clear list for next match
				}
				tempMessage = "Match Rule 0 " +colName[k]+ " records processed: " + recordsRead + "  matches found: " + patientMatches;
				writeLog(logInfo, tempMessage, true);
			} catch (SQLException e1) {
				System.out.println(e1.getMessage());
				if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
			} catch (Exception e1) {
				e1.printStackTrace();	
			}
		}
	}

	private void addToGlobalIdGroup(ArrayList<Integer> globalIdGroup) {
		Set<Integer> nextPatSet = new HashSet<Integer>(globalIdGroup);		// convert to set to remove dups
		
		boolean entryNotFound = true;
		for (Integer nextPat : nextPatSet) {
			if (patGlobalIdMapLink.containsKey(nextPat)) {				// see if previous link for this pat
				Integer linkId = patGlobalIdMapLink.get(nextPat);		// if so get link to patGlobalIdMap
				Set<Integer> updatePatSet = patGlobalIdMap.get(linkId); // get existing pat set map entry
				updatePatSet.addAll(nextPatSet);			// add all pats in incoming set, removes duplicates
				patGlobalIdMap.put(linkId, updatePatSet);	// save updated set back into same map location
				savePatGlobalIdMapLink(linkId, nextPatSet);	// save map link for all pats in incoming set
				entryNotFound = false;
				break;										// exit loop since entry found
			}
		}
		
		if (entryNotFound) {										// if no previous entry found
			patGlobalIdCount++;
			patGlobalIdMap.put(patGlobalIdCount, nextPatSet);		// put new entry of matched patients in map
			savePatGlobalIdMapLink(patGlobalIdCount, nextPatSet);	// save map link for all pats in set
		}
	}

	// save link to patGlobalIdMap entry for all pats in set
	private void savePatGlobalIdMapLink(Integer tempLinkId, Set<Integer> tempPatSet) {
		for (Integer tempPatId : tempPatSet) {				// for all patients in set
			patGlobalIdMapLink.put(tempPatId, tempLinkId);	// adds if new or updates if existed previously
		}
	}

	private void runGlobalMatchRule1(boolean keySame) {
		tempMessage = "Starting Match Rule 1";
		writeLog(logInfo, tempMessage, true);
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();			// connect to database if no connection yet
		}
		createTempTableAllKey();	// go create and populate temp table to hold pats to compare
		
		String[] colName = new String[6];
		colName[1] = "hash1";
		colName[2] = "hash2";
		colName[3] = "hash5";
		colName[4] = "hash9";
		colName[5] = "hash10";
		String[] indexName = new String[6];
		indexName[1] = "index1";
		indexName[2] = "index2";
		indexName[3] = "index5";
		indexName[4] = "index9";
		indexName[5] = "index10";
		
		for (int k = 1; k < 6; k++) {		// loop thru each column individually, check for match
			int recordsRead = 0;
			int patientMatches = 0;
			String lastPat = "xx";
			Integer currRowId = 0;
			Integer lastRowId = 0;
			boolean currMatch = false;
			boolean lastMatch = false;

			String sql9 = "SELECT "+colName[k]+",rowId FROM " +TempTableAllKey+ " INDEXED BY " +indexName[k];
			try ( PreparedStatement pstmt9 = db.prepareStatement(sql9)) { 
				pstmt9.setFetchSize(1000);						//number of rows to be fetched when needed
				ResultSet resultSet = pstmt9.executeQuery();					// executeQuery
				//int columnCount = resultSet.getMetaData().getColumnCount();	// get column count
				String nullKey = nullEntry;
				while (resultSet.next()) {
					recordsRead++;
					String currPat = resultSet.getString(colName[k]);	// get pat
					currRowId = resultSet.getInt("rowId");				// get row id

					if (currPat.equals(nullKey) || currPat.isEmpty()) {
						currMatch = false;							// skip empty values
					} else if (currPat.equals(lastPat)) {
						currMatch = true;
						patientMatches++;
						if (!lastMatch) {
							currGlobalIdGroup.add(lastRowId);	//if match and not last match add both to set
							currGlobalIdGroup.add(currRowId);						
						} else {
							currGlobalIdGroup.add(currRowId);	// else matched before so lastPat already included
						}
					} else {
						currMatch = false;
						if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {  // if have entry, then save
							addToGlobalIdGroup( currGlobalIdGroup );
							currGlobalIdGroup.clear();					// clear list for next match
						}
					}
					lastPat = currPat;		// save current patient key for next loop
					lastMatch = currMatch;
					lastRowId = currRowId;

					if ((recordsRead % 50000) == 0) {
						tempMessage = "Match Rule 1 " +colName[k]+ " records processed: " + recordsRead + "  matches found: " + patientMatches;
						writeLog(logInfo, tempMessage, true);
					}
				}
				if (resultSet != null) {
					resultSet.close();
				}
				if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
					addToGlobalIdGroup( currGlobalIdGroup );
					currGlobalIdGroup.clear();					// clear list for next match
				}
				tempMessage = "Match Rule 1 " +colName[k]+ " records processed: " + recordsRead + "  matches found: " + patientMatches;
				writeLog(logInfo, tempMessage, true);
				if (resultSet != null) {
					resultSet.close();
				}
			} catch (SQLException e1) {
				System.out.println(e1.getMessage());
				if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
			} catch (Exception e1) {
				e1.printStackTrace();	
			}
		}
	}

	private void runGlobalMatchRule2(boolean keySame) {
		tempMessage = "Starting Match Rule 2";
		writeLog(logInfo, tempMessage, true);
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();			// connect to database if no connection yet
		}
		createTempTableAllKey();	// go create and populate temp table to hold pats to compare

		String[] colName = new String[4];
		colName[1] = "hash3";
		colName[2] = "hash4";
		colName[3] = "hash6";
		String[] indexName = new String[4];
		indexName[1] = "index3";
		indexName[2] = "index4";
		indexName[3] = "index6";

		for (int k = 1; k < 4; k++) {		// loop thru each column individually, check for match
			int recordsRead = 0;
			int patientMatches = 0;
			String lastPat = "xx";
			Integer currRowId = 0;
			Integer lastRowId = 0;
			boolean currMatch = false;
			boolean lastMatch = false;

			String sql9 = "SELECT "+colName[k]+",rowId FROM " +TempTableAllKey+ " INDEXED BY " +indexName[k];
			try ( PreparedStatement pstmt9 = db.prepareStatement(sql9)) { 
				pstmt9.setFetchSize(1000);						//number of rows to be fetched when needed
				ResultSet resultSet = pstmt9.executeQuery();					// executeQuery
				//int columnCount = resultSet.getMetaData().getColumnCount();	// get column count
				String nullKey = nullEntry;
				while (resultSet.next()) {
					recordsRead++;
					String currPat = resultSet.getString(colName[k]);	// get pat
					currRowId = resultSet.getInt("rowId");				// get row id

					if (currPat.equals(nullKey) || currPat.isEmpty()) {
						currMatch = false;							// skip empty values
					} else if (currPat.equals(lastPat)) {
						currMatch = true;
						patientMatches++;
						if (!lastMatch) {
							currGlobalIdGroup.add(lastRowId);	//if match and not last match add both to set
							currGlobalIdGroup.add(currRowId);						
						} else {
							currGlobalIdGroup.add(currRowId);	// else matched before so lastPat already included
						}
					} else {
						currMatch = false;
						if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {  // if have entry, then save
							addToGlobalIdGroup( currGlobalIdGroup );
							currGlobalIdGroup.clear();					// clear list for next match
						}
					}
					lastPat = currPat;		// save current patient key for next loop
					lastMatch = currMatch;
					lastRowId = currRowId;

					if ((recordsRead % 50000) == 0) {
						tempMessage = "Match Rule 2 " +colName[k]+ " records processed: " + recordsRead + "  matches found: " + patientMatches;
						writeLog(logInfo, tempMessage, true);
					}
				}
				if (resultSet != null) {
					resultSet.close();
				}
				if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
					addToGlobalIdGroup( currGlobalIdGroup );
					currGlobalIdGroup.clear();					// clear list for next match
				}
				tempMessage = "Match Rule 2 " +colName[k]+ " records processed: " + recordsRead + "  matches found: " + patientMatches;
				writeLog(logInfo, tempMessage, true);
			} catch (SQLException e1) {
				System.out.println(e1.getMessage());
				if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
			} catch (Exception e1) {
				e1.printStackTrace();	
			}
		}
	}

	private void runGlobalMatchRule3(boolean keySame) {
		tempMessage = "Starting Match Rule 3";
		writeLog(logInfo, tempMessage, true);
		int recordsRead = 0;
		int patientMatches = 0;
		String lastPat = "xx";
		Integer currRowId = 0;
		Integer lastRowId = 0;
		boolean currMatch = false;
		boolean lastMatch = false;
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		
		// query database
		String sqlQuery = "SELECT hash1,id "  				// must have all fields in index 
				+ "FROM GlobalMatch INDEXED BY match3";		// create prepared statement
		try ( PreparedStatement pstmt = db.prepareStatement(sqlQuery)) {
			pstmt.setFetchSize(1000);						//number of rows to be fetched when needed
			ResultSet resultSet = pstmt.executeQuery();						// executeQuery
			//int columnCount = resultSet.getMetaData().getColumnCount();	// get column count
			String nullKey = nullEntry;
			while (resultSet.next()) {
				recordsRead++;
				String currPat = resultSet.getString("hash1");	// get pat
				currRowId = resultSet.getInt("id");				// get row id in last column

				if (currPat.equals(nullKey) || currPat.isEmpty()) {
					currMatch = false;							// skip empty values
				} else if (currPat.equals(lastPat)) {
					currMatch = true;
					patientMatches++;
					if (!lastMatch) {
						currGlobalIdGroup.add(lastRowId);	//if match and not last match add both to set
						currGlobalIdGroup.add(currRowId);						
					} else {
						currGlobalIdGroup.add(currRowId);	// else matched before so lastPat already included
					}
				} else {
					currMatch = false;
					if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {  // if have entry, then save
						addToGlobalIdGroup( currGlobalIdGroup );
						currGlobalIdGroup.clear();					// clear list for next match
					}
				}
				lastPat = currPat;		// save current patient key for next loop
				lastMatch = currMatch;
				lastRowId = currRowId;

				if ((recordsRead % 10000) == 0) {
					tempMessage = "Match Rule 3 records processed: " + recordsRead + "  matches found: " + patientMatches;
					writeLog(logInfo, tempMessage, true);
				}
			}
			if (resultSet != null) {
				resultSet.close();
			}
			if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
				addToGlobalIdGroup( currGlobalIdGroup );
				currGlobalIdGroup.clear();					// clear list for next match
			}
			tempMessage = "Match Rule 3 records processed: " + recordsRead + "  matches found: " + patientMatches;
			writeLog(logInfo, tempMessage, true);
			if (resultSet != null) {
				resultSet.close();
			}
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
			if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
		} catch (Exception e1) {
			e1.printStackTrace();	
		}
	}

	private void runGlobalMatchRule4(boolean keySame) {
		int patientMatches = 0;
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		createTempTableAllKey();		// go create and populate temp table to hold pats to compare

		String sqlQuery = "SELECT hash1,hash2,id "  		// query database, must have all fields in index 
				+ "FROM GlobalMatch INDEXED BY match4";		// create prepared statement
		try ( PreparedStatement pstmt = db.prepareStatement(sqlQuery)) {
			pstmt.setFetchSize(1000);						//number of rows to be fetched when needed
			ResultSet resultSet = pstmt.executeQuery();		// executeQuery
			while (resultSet.next()) {
				String hash1 = resultSet.getString("hash1");		// get data from this resultset row
				//String hash2 = resultSet.getString("hash2");
				int rowId = resultSet.getInt("id");

				if (hash1.equals(nullEntry) || hash1.isEmpty()) { continue; } // if hash1 empty =nomatch, skip to next
				String sql9 = "SELECT hash2,rowId FROM " +TempTableAllKey+ " INDEXED BY index2 WHERE hash2 = '"+hash1+"'";
				try ( PreparedStatement pstmt9 = db.prepareStatement(sql9)) { 
					ResultSet rset9 = pstmt9.executeQuery();
					while (rset9.next()) {
						//String tempName = rset9.getString("hash2");		// get data from this resultset row
						int tempRowId = rset9.getInt("rowId");
						if (rowId != tempRowId) {
							currGlobalIdGroup.add(rowId);			// save ids of these 2 matching patients
							currGlobalIdGroup.add(tempRowId);
							patientMatches++;
						}
					}
					if (rset9 != null) { rset9.close(); }
					if (currGlobalIdGroup !=null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
						addToGlobalIdGroup( currGlobalIdGroup );
						currGlobalIdGroup.clear();  // clear list for next match
					}
				}
			}
			if (resultSet != null) {
				resultSet.close();
			}
			tempMessage = "Match Rule 4 found: " + patientMatches + " patient matches";
			writeLog(logInfo, tempMessage, true);
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
			if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
		} catch (Exception e1) {
			e1.printStackTrace();	
		}
	}

	private void runGlobalMatchRule5(boolean keySame) {
		int patientMatches = 0;
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();			// connect to database if no connection yet
		}
		createTempTableAllKey();	// go create and populate temp table to hold pats to compare

		String sqlQuery = "SELECT hash1,hash5,id "  		// query database, must have all fields in index 
				+ "FROM GlobalMatch INDEXED BY match5";		// create prepared statement
		try ( PreparedStatement pstmt = db.prepareStatement(sqlQuery)) {
			pstmt.setFetchSize(1000);						//number of rows to be fetched when needed
			ResultSet resultSet = pstmt.executeQuery();		// executeQuery
			while (resultSet.next()) {
				String hash1 = resultSet.getString("hash1");	// get data from this resultset row
				//String hash5 = resultSet.getString("hash5");
				int rowId = resultSet.getInt("id");

				if (hash1.equals(nullEntry) || hash1.isEmpty()) { continue; } // if hash1 empty =nomatch, skip to next
				String sql9 = "SELECT hash5,rowId FROM " +TempTableAllKey+ " INDEXED BY index5 WHERE hash5 = '"+hash1+"'";
				try ( PreparedStatement pstmt9 = db.prepareStatement(sql9)) { 
					ResultSet rset9 = pstmt9.executeQuery();
					while (rset9.next()) {
						//String tempName = rset9.getString("hash5");	// get data from this resultset row
						int tempRowId = rset9.getInt("rowId");
						if (rowId != tempRowId) {					// if not same pat, then is a match
							currGlobalIdGroup.add(rowId);			// save ids of these 2 matching patients
							currGlobalIdGroup.add(tempRowId);
							patientMatches++;
						}
					}
					if (rset9 != null) { rset9.close(); }
					if (currGlobalIdGroup !=null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
						addToGlobalIdGroup( currGlobalIdGroup );
						currGlobalIdGroup.clear();  // clear list for next match
					}
				} 
			}
			if (resultSet != null) {
				resultSet.close();
			}
			tempMessage = "Match Rule 5 found: " + patientMatches + " patient matches";
			writeLog(logInfo, tempMessage, true);
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
			if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
		} catch (Exception e1) {
			e1.printStackTrace();	
		}	
	}

	private void runGlobalMatchRule6(boolean keySame) {
		int patientMatches = 0;
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();			// connect to database if no connection yet
		}
		createTempTableAllKey();	// go create and populate temp table to hold pats to compare

		String sqlQuery = "SELECT hash1,hash9,id "  		// query database, must have all fields in index 
				+ "FROM GlobalMatch INDEXED BY match6";		// create prepared statement
		try ( PreparedStatement pstmt = db.prepareStatement(sqlQuery)) {
			pstmt.setFetchSize(1000);						//number of rows to be fetched when needed
			ResultSet resultSet = pstmt.executeQuery();		// executeQuery
			while (resultSet.next()) {
				String hash1 = resultSet.getString("hash1");	// get data from this resultset row
				//String hash9 = resultSet.getString("hash9");
				int rowId = resultSet.getInt("id");

				if (hash1.equals(nullEntry) || hash1.isEmpty()) { continue; } // if hash1 empty =nomatch, skip to next
				String sql9 = "SELECT hash9,rowId FROM " +TempTableAllKey+ " INDEXED BY index9 WHERE hash9 = '"+hash1+"'";
				try ( PreparedStatement pstmt9 = db.prepareStatement(sql9)) { 
					ResultSet rset9 = pstmt9.executeQuery();
					while (rset9.next()) {
						//String tempName = rset9.getString("hash9");	// get data from this resultset row
						int tempRowId = rset9.getInt("rowId");
						if (rowId != tempRowId) {						// if not same pat, then is a match
							currGlobalIdGroup.add(rowId);				// save ids of these 2  matching patients
							currGlobalIdGroup.add(tempRowId);
							patientMatches++;
						}
					}
					if (rset9 != null) { rset9.close(); }
					if (currGlobalIdGroup !=null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
						addToGlobalIdGroup( currGlobalIdGroup );
						currGlobalIdGroup.clear();  // clear list for next match
					}
				} 
			}
			if (resultSet != null) {
				resultSet.close();
			}
			tempMessage = "Match Rule 6 found: " + patientMatches + " patient matches";
			writeLog(logInfo, tempMessage, true);
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
			if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
		} catch (Exception e1) {
			e1.printStackTrace();	
		}
	}

	private void runGlobalMatchRule7(boolean keySame) {
		int patientMatches = 0;
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();			// connect to database if no connection yet
		}
		createTempTableAllKey();	// go create and populate temp table to hold pats to compare

		String sqlQuery = "SELECT hash1,hash10,id "  		// query database, must have all fields in index 
				+ "FROM GlobalMatch INDEXED BY match7";		// create prepared statement
		try ( PreparedStatement pstmt = db.prepareStatement(sqlQuery)) {
			pstmt.setFetchSize(1000);						//number of rows to be fetched when needed
			ResultSet resultSet = pstmt.executeQuery();		// executeQuery
			while (resultSet.next()) {
				String hash1 = resultSet.getString("hash1");	// get data from this resultset row
				//String hash10 = resultSet.getString("hash10");
				int rowId = resultSet.getInt("id");

				if (hash1.equals(nullEntry) || hash1.isEmpty()) { continue; }	// if hash1 empty =nomatch, skip to next
				String sql9 = "SELECT hash10,rowId FROM " +TempTableAllKey+ " INDEXED BY index10 WHERE hash10 = '"+hash1+"'";
				try ( PreparedStatement pstmt9 = db.prepareStatement(sql9)) { 
					ResultSet rset9 = pstmt9.executeQuery();
					while (rset9.next()) {
						//String tempName = rset9.getString("hash10");	// get data from this resultset row
						int tempRowId = rset9.getInt("rowId");
						if (rowId != tempRowId) {						// if not same pat, then is a match
							currGlobalIdGroup.add(rowId);				// save ids of these 2  matching patients
							currGlobalIdGroup.add(tempRowId);
							patientMatches++;
						}
					}
					if (rset9 != null) { rset9.close(); }
					if (currGlobalIdGroup !=null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
						addToGlobalIdGroup( currGlobalIdGroup );
						currGlobalIdGroup.clear();  // clear list for next match
					}
				} 
			}
			if (resultSet != null) {
				resultSet.close();
			}
			tempMessage = "Match Rule 7 found: " + patientMatches + " patient matches";
			writeLog(logInfo, tempMessage, true);
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
			if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
		} catch (Exception e1) {
			e1.printStackTrace();	
		}
	}

	private void runGlobalMatchRule8(boolean keySame) {
		int recordsRead = 0;
		int patientMatches = 0;
		String lastPat = "xx";
		Integer currRowId = 0;
		Integer lastRowId = 0;
		boolean currMatch = false;
		boolean lastMatch = false;
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		
		// query database
		String sqlQuery = "SELECT hash3,id "  				// must have all fields in index 
				+ "FROM GlobalMatch INDEXED BY match8";		// create prepared statement
		try ( PreparedStatement pstmt = db.prepareStatement(sqlQuery)) {
			pstmt.setFetchSize(1000);						//number of rows to be fetched when needed
			ResultSet resultSet = pstmt.executeQuery();		// executeQuery
			//int columnCount = resultSet.getMetaData().getColumnCount();	// get column count
			String nullKey = nullEntry;
			while (resultSet.next()) {

				recordsRead++;
				String currPat = resultSet.getString("hash3");	// get pat from this resultset row
				currRowId = resultSet.getInt("id");				// get row id in last column
				if (currPat.equals(nullKey) || currPat.isEmpty()) {
					currMatch = false;
				} else if (currPat.equals(lastPat)) {
					currMatch = true;
					patientMatches++;
					if (!lastMatch) {
						currGlobalIdGroup.add(lastRowId);	//if match and not last match add both to set
						currGlobalIdGroup.add(currRowId);
						//matchSet.add(currPat);						
					} else {
						currGlobalIdGroup.add(currRowId);	// else matched before so lastPat already included
					}
				} else {
					currMatch = false;
					if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
						addToGlobalIdGroup( currGlobalIdGroup );
						currGlobalIdGroup.clear();					// clear list for next match
					}
				}
				lastPat = currPat;		// save current patient key for next loop
				lastMatch = currMatch;
				lastRowId = currRowId;

				if ((recordsRead % 10000) == 0) {
					tempMessage = "Match Rule 8 records processed: " + recordsRead + "  matches found: " + patientMatches;
					writeLog(logInfo, tempMessage, true);
				}
			}
			if (resultSet != null) {
				resultSet.close();
			}
			if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
				addToGlobalIdGroup( currGlobalIdGroup );
				currGlobalIdGroup.clear();					// clear list for next match
			}
			tempMessage = "Match Rule 8 records processed: " + recordsRead + "  matches found: " + patientMatches;
			writeLog(logInfo, tempMessage, true);
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
			if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
		} catch (Exception e1) {
			e1.printStackTrace();	
		}
	}

	private void runGlobalMatchRule9(boolean keySame) {
		int patientMatches = 0;
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();			// connect to database if no connection yet
		}
		createTempTableAllKey();	// go create and populate temp table to hold pats to compare

		String sqlQuery = "SELECT hash3,hash4,id "  		// query database, must have all fields in index 
				+ "FROM GlobalMatch INDEXED BY match9";		// create prepared statement
		try ( PreparedStatement pstmt = db.prepareStatement(sqlQuery)) {
			pstmt.setFetchSize(1000);						//number of rows to be fetched when needed
			ResultSet resultSet = pstmt.executeQuery();		// executeQuery
			while (resultSet.next()) {
				String hash3 = resultSet.getString("hash3");	// get data from this resultset row
				//String hash4 = resultSet.getString("hash4");
				int rowId = resultSet.getInt("id");

				if (hash3.equals(nullEntry) || hash3.isEmpty()) { continue; } // if hash3 empty =nomatch, skip to next
				String sql9 = "SELECT hash4,rowId FROM " +TempTableAllKey+ " INDEXED BY index4 WHERE hash4 = '"+hash3+"'";
				try ( PreparedStatement pstmt9 = db.prepareStatement(sql9)) { 
					ResultSet rset9 = pstmt9.executeQuery();
					while (rset9.next()) {
						//String tempName = rset9.getString("hash4");	// get data from this resultset row
						int tempRowId = rset9.getInt("rowId");
						if (rowId != tempRowId) {						// if not same pat, then is a match
							currGlobalIdGroup.add(rowId);				// save ids of these 2 matching patients
							currGlobalIdGroup.add(tempRowId);
							patientMatches++;
						}
					}
					if (rset9 != null) { rset9.close(); }
					if (currGlobalIdGroup !=null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
						addToGlobalIdGroup( currGlobalIdGroup );
						currGlobalIdGroup.clear();  // clear list for next match
					}
				} 
			}
			if (resultSet != null) {
				resultSet.close();
			}
			tempMessage = "Match Rule 9 found: " + patientMatches + " patient matches";
			writeLog(logInfo, tempMessage, true);
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
			if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
		} catch (Exception e1) {
			e1.printStackTrace();	
		}
	}

	private void runGlobalMatchRule10(boolean keySame) {
		int patientMatches = 0;
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();			// connect to database if no connection yet
		}
		createTempTableAllKey();	// go create and populate temp table to hold pats to compare

		String sqlQuery = "SELECT hash3,hash6,id "  			// query database, must have all fields in index 
				+ "FROM GlobalMatch INDEXED BY match10";		// create prepared statement
		try ( PreparedStatement pstmt = db.prepareStatement(sqlQuery)) {
			pstmt.setFetchSize(1000);							//number of rows to be fetched when needed
			ResultSet resultSet = pstmt.executeQuery();			// executeQuery
			while (resultSet.next()) {
				String hash3 = resultSet.getString("hash3");	// get data from this resultset row
				//String hash6 = resultSet.getString("hash6");
				int rowId = resultSet.getInt("id");

				if (hash3.equals(nullEntry) || hash3.isEmpty()) { continue; } // if hash3 empty =nomatch, skip to next
				String sql9 = "SELECT hash6,rowId FROM " +TempTableAllKey+ " INDEXED BY index6 WHERE hash6 = '"+hash3+"'";
				try ( PreparedStatement pstmt9 = db.prepareStatement(sql9)) { 
					ResultSet rset9 = pstmt9.executeQuery();
					while (rset9.next()) {
						//String tempName = rset9.getString("name");	// get data from this resultset row
						int tempRowId = rset9.getInt("rowId");
						if (rowId != tempRowId) {						// if not same pat, then is a match
							currGlobalIdGroup.add(rowId);				// save ids of these 2 matching patients
							currGlobalIdGroup.add(tempRowId);
							patientMatches++;
						}
					}
					if (rset9 != null) { rset9.close(); }
					if (currGlobalIdGroup !=null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
						addToGlobalIdGroup( currGlobalIdGroup );
						currGlobalIdGroup.clear();  // clear list for next match
					}
				}
			}
			if (resultSet != null) {
				resultSet.close();
			}
			tempMessage = "Match Rule 10 found: " + patientMatches + " patient matches";
			writeLog(logInfo, tempMessage, true);
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
			if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
		} catch (Exception e1) {
			e1.printStackTrace();	
		}
	}

	private void runGlobalMatchRule11(boolean keySame) {
		int recordsRead = 0;
		int patientMatches = 0;
		String lastPat = "xx";
		Integer currRowId = 0;
		Integer lastRowId = 0;
		boolean currMatch = false;
		boolean lastMatch = false;
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		
		// query database
		String sqlQuery = "SELECT hash7,id "  					// must have all fields in index 
				+ "FROM GlobalMatch INDEXED BY match11";		// create prepared statement
		try ( PreparedStatement pstmt = db.prepareStatement(sqlQuery)) {
			pstmt.setFetchSize(1000);							//number of rows to be fetched when needed
			ResultSet resultSet = pstmt.executeQuery();			// executeQuery
			//int columnCount = resultSet.getMetaData().getColumnCount();	// get column count
			String nullKey = nullEntry;
			while (resultSet.next()) {

				recordsRead++;
				String currPat = resultSet.getString("hash7");	// get data from this resultset row
				currRowId = resultSet.getInt("id");				// get row id in last column
				if (currPat.equals(nullKey) || currPat.isEmpty()) {
					currMatch = false;
				} else if (currPat.equals(lastPat)) {
					currMatch = true;
					patientMatches++;
					if (!lastMatch) {
						currGlobalIdGroup.add(lastRowId);	//if match and not last match add both to set
						currGlobalIdGroup.add(currRowId);
						//matchSet.add(currPat);						
					} else {
						currGlobalIdGroup.add(currRowId);	// else matched before so lastPat already included
					}
				} else {
					currMatch = false;
					if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
						addToGlobalIdGroup( currGlobalIdGroup );
						currGlobalIdGroup.clear();					// clear list for next match
					}
				}
				lastPat = currPat;		// save current patient key for next loop
				lastMatch = currMatch;
				lastRowId = currRowId;

				if ((recordsRead % 10000) == 0) {
					tempMessage = "Match Rule 11 records processed: " + recordsRead + "  matches found: " + patientMatches;
					writeLog(logInfo, tempMessage, true);
				}
			}
			if (resultSet != null) {
				resultSet.close();
			}
			if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
				addToGlobalIdGroup( currGlobalIdGroup );
				currGlobalIdGroup.clear();					// clear list for next match
			}
			tempMessage = "Match Rule 11 records processed: " + recordsRead + "  matches found: " + patientMatches;
			writeLog(logInfo, tempMessage, true);
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
			if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
		} catch (Exception e1) {
			e1.printStackTrace();	
		}
	}

	private void runGlobalMatchRule12(boolean keySame) {
		int recordsRead = 0;
		int patientMatches = 0;
		String lastPat = "xx";
		Integer currRowId = 0;
		Integer lastRowId = 0;
		boolean currMatch = false;
		boolean lastMatch = false;
		ArrayList<Integer> currGlobalIdGroup = new ArrayList<Integer>();
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		
		// query database
		String sqlQuery = "SELECT hash8,id "  					// must have all fields in index 
				+ "FROM GlobalMatch INDEXED BY match12";		// create prepared statement
		try ( PreparedStatement pstmt = db.prepareStatement(sqlQuery)) {
			pstmt.setFetchSize(1000);							//number of rows to be fetched when needed
			ResultSet resultSet = pstmt.executeQuery();			// executeQuery (executeUpdate won't return result set)
			//int columnCount = resultSet.getMetaData().getColumnCount();	// get column count
			String nullKey = nullEntry;
			while (resultSet.next()) {

				recordsRead++;
				String currPat = resultSet.getString("hash8");	// get data from this resultset row
				currRowId = resultSet.getInt("id");				// get row id in last column
				if (currPat.equals(nullKey) || currPat.isEmpty()) {
					currMatch = false;
				} else if (currPat.equals(lastPat)) {
					currMatch = true;
					patientMatches++;
					if (!lastMatch) {
						currGlobalIdGroup.add(lastRowId);	//if match and not last match add both to set
						currGlobalIdGroup.add(currRowId);
						//matchSet.add(currPat);						
					} else {
						currGlobalIdGroup.add(currRowId);	// else matched before so lastPat already included
					}
				} else {
					currMatch = false;
					if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
						addToGlobalIdGroup( currGlobalIdGroup );
						currGlobalIdGroup.clear();					// clear list for next match
					}
				}
				lastPat = currPat;		// save current patient key for next loop
				lastMatch = currMatch;
				lastRowId = currRowId;

				if ((recordsRead % 10000) == 0) {
					tempMessage = "Match Rule 12 records processed: " + recordsRead + "  matches found: " + patientMatches;
					writeLog(logInfo, tempMessage, true);
				}
			}
			if (resultSet != null) {
				resultSet.close();
			}
			if (currGlobalIdGroup != null && !currGlobalIdGroup.isEmpty()) {	// if have entry, then save
				addToGlobalIdGroup( currGlobalIdGroup );
				currGlobalIdGroup.clear();					// clear list for next match
			}
			tempMessage = "Match Rule 12 records processed: " + recordsRead + "  matches found: " + patientMatches;
			writeLog(logInfo, tempMessage, true);
		} catch (SQLException e1) {
			System.out.println(e1.getMessage());
			if (e1.getMessage().contains(SQL_Exception1)) { stopProcess(e1.getMessage()); }
		} catch (Exception e1) {
			e1.printStackTrace();	
		}
	}

	public void writeInvalidData( String fileOut, String invalidData) {
		if (invalidData == null || invalidData.isEmpty()) {		// check if anything in invalidData
			return;
		}
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

	public void writeLog(int logLevel, String textMessage, boolean echoConsole) {
		String firstPart;
		switch (logLevel) {
		case 0 :
			firstPart = "    ";	// normal log message
			break;
		case 1 :
			firstPart = " ** ";	// warning log message
			break;
		case 2 :
			firstPart = " ## ";	// severe log message
			break;
		case 3 :
			firstPart = " :: ";	// begin task log message
			break;
		case 4 :
			firstPart = " >> ";	// begin section log message
			break;
		case 5 :
			firstPart = "  > ";	// begin subsection log message
			break;
		default :
			firstPart = "    ";
		}
		if (echoConsole) {
			System.out.println(firstPart + textMessage);			// echo to console if requested
		}
		String logFile1 = logFile + dateNowFormatted() + ".txt";	// create log file name
		String text = timeNowFormatted() + firstPart + textMessage;

		Path path = Paths.get( logFile1 );
		if (Files.notExists(path)) {
			try { Files.createFile(path);						// create file if doesn't exist
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

	private String makeFilePath(String filePath, String fileName) {
		filePath = changeDirectorySeparator(filePath);
		if (filePath.endsWith(directorySeparator)) {
			return filePath + fileName;
		} else {
			return filePath + directorySeparator + fileName;
		}
	}

	public String changeFileExtension(String fileToRename, String newExtension) {
		int index = fileToRename.lastIndexOf('.');
		String name = fileToRename.substring(0, index + 1);
		return name + newExtension;
	}

	private String getFileExtension(String fullFilePath) {
		String fullName = changeDirectorySeparator(fullFilePath);	// check if Windows separator
		String fileName = fullName.substring(fullName.lastIndexOf("/")+1);
		return fileName;
	}

	public boolean moveFileToDir(String processedFilesDirectory, String fileToMoveName) throws IOException {
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
			writeLog(logSevere, tempMessage, true);
			return false;
		}

		try {
			FileUtils.copyFile(fileToMove, fileNewLoc, true);	// copy to new location
			if (fileNewLoc.exists()) {
				FileUtils.deleteQuietly(fileToMove);			// delete orig if copy successful
			}
			tempMessage = "file: " + fileToMove.getAbsolutePath();
			writeLog(logInfo, tempMessage, true);
			tempMessage = "moved to: " + fileNewLoc.getAbsolutePath();
			writeLog(logInfo, tempMessage, true);
			success = true;
		} catch (IOException e) {
			e.printStackTrace();
			tempMessage = "file not moved: " + fileToMoveName;
			writeLog(logSevere, tempMessage, true);
		}
		return success;
	}


	private void readConfig(String projRoot) {
		
		configFilePath = makeFilePath(makeFilePath(projRoot, "config"), configFileName);  	// get config path
		atomicIntegerPath = makeFilePath(makeFilePath(projRoot, "config"), atomicIntegerFile);  // get path

		// read config data from properties file using try with resources, means autoclose
		Properties prop = new Properties();
		try ( InputStream input = new FileInputStream( configFilePath )) {
			prop.load( input );
			//prop.load(new FileInputStream(configFile));

			//configFileDir = prop.getProperty("ConfigFilesDirectory");
			inputDir = prop.getProperty("InputFilesDirectory");
			inputDir = changeDirectorySeparator(inputDir);				// change file separator if Windows
			inputDir = inputDir.replaceFirst(PROJECT_ROOT, projRoot);
			outputDir = prop.getProperty("OutputFilesDirectory");
			outputDir = changeDirectorySeparator(outputDir);			// change file separator if Windows
			outputDir = outputDir.replaceFirst(PROJECT_ROOT, projRoot);
			processedDir = prop.getProperty("ProcessedFilesDirectory");
			processedDir = changeDirectorySeparator(processedDir);		// change file separator if Windows
			processedDir = processedDir.replaceFirst(PROJECT_ROOT, projRoot);
			logFileDir = prop.getProperty("ProcessedFilesDirectory");
			logFileDir = logFileDir.replaceFirst(PROJECT_ROOT, projRoot);
			logFile = makeFilePath(logFileDir, logFileName);
			dbDirectory = prop.getProperty("DbDirectory");
			dbDirectory = dbDirectory.replaceFirst(PROJECT_ROOT, projRoot);
			dbName = prop.getProperty("DbName");
			inputFileNamePrefix = prop.getProperty("InputFileNamePrefix");
			inputFileNameSuffix = prop.getProperty("InputFileNameSuffix");
			System.out.println("database: " + dbDirectory + dbName);
			System.out.println("log file: " + logFile);

			String matchText = prop.getProperty("MatchingRules");		// get matching rules
			String[] tempArr = matchText.split(delimComma);				// split incoming text
			for (int i = 0; i < tempArr.length; i++) {	
				matchSequence.add( Integer.parseInt(tempArr[i]) );		// save match sequence as integer
			}

			writeLog(logBegin, "Starting Global Patient Match <<", true);
			writeLog(logInfo, "reading configuration file " + configFilePath, true);
			tempMessage = "match rule from config: " + matchSequence;
			writeLog(logSection, tempMessage, false); 
		} catch (Exception e) {
			//e.printStackTrace();
			System.out.println("**read config error message: "+e.getMessage());
			System.exit(1);
		}
		readAtomicIntegerSeed();		// read starting number to seed global Id generator
	}

	// read files from 1 input directory
	public ArrayList<String> getFileNames1Dir(String directory, final String prefix, final String suffix) {

		ArrayList<String> matchingFiles = new ArrayList<String>();
		File dir = new File(directory);
		if (!dir.exists()) {
			System.out.println("***" + directory + " directory does not exist");
			return matchingFiles;
		}
		FilenameFilter fileFilter = new FilenameFilter() {		// create a dir filter
			public boolean accept (File dir, String name) {
				if (prefix.isEmpty()) {
					return name.endsWith( suffix );		// if only have suffix
				} else if (suffix.isEmpty()) {
					return name.startsWith( prefix );	// if only have prefix
				} else {
					return name.startsWith( prefix ) && name.endsWith( suffix );  // if have prefix + suffix
				}
			} 
		};
		File[] paths = dir.listFiles(fileFilter);			// get files in dir returned by filter
		for (File path : paths) {
			matchingFiles.add(path.getAbsolutePath());		// get full file and directory path
		}
		return matchingFiles;
	}

	public void readAtomicIntegerSeed() {
		File fileIn = new File(atomicIntegerPath);
		try ( BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileIn))) ) {
			String line = null;
			while ((line = br.readLine()) != null) {
				atomicIntegerSeed = Integer.valueOf( line.substring(0, line.indexOf(delimPipe)).trim());
				globalId = new AtomicInteger(atomicIntegerSeed);	// set base Global id
				break;		// only 1 line in file so exit loop
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void writeAtomicIntegerSeed() {

		File fileOut = new File(atomicIntegerPath);
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileOut)) ) {

			Integer currGlobalId = globalId.get();		// get (but don't increment) current global Id
			String saveRecord = Integer.toString(currGlobalId) +" | "+ dateNowFormatted();
			bw.write(saveRecord);  		// write out record to be read in next restart

			String tempMessage = "Global Id ending seed: " + currGlobalId;
			writeLog(logInfo, tempMessage, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// generate report
	public void createReport1(String site) {
		String sql0;
		String sql1;
		ArrayList<String> exclusionPats = new ArrayList<String>();
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		// get ExclusionPatients list and give each a globalId
		if (site.equalsIgnoreCase("All")) {
			sql0 = "SELECT siteId,projectId,pidhash FROM ExclusionPatients";
		} else {
			sql0 = "SELECT siteId,projectId,pidhash FROM ExclusionPatients WHERE siteId = '" + site + "'";
		} 
		try ( PreparedStatement stmt0 = db.prepareStatement(sql0)) {
			stmt0.setFetchSize(1000);				//number of rows to be fetched when needed
			ResultSet rs0 = stmt0.executeQuery();
			while (rs0.next()) {
				Integer nextGlobalId = globalId.incrementAndGet();	// assign next global id
				String currSite = rs0.getString("siteId");			// format same as below
				String currProj = rs0.getString("projectId");
				String currPid = rs0.getString("pidhash");
				exclusionPats.add(currSite+","+currProj+","+currPid+","+Integer.toString(nextGlobalId));	
			}
			if (rs0 != null) { rs0.close(); }
			writeAtomicIntegerSeed();		// go save current value of next globalId
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
		}

		String tempFileName = reportFileName + site + "-" + dateTimeNowHL7() + ".txt";
		String report1File = makeFilePath(outputDir, tempFileName);
		tempMessage = "writing report for site " + site + " to: " + report1File;	// show output file name
		writeLog(logInfo, tempMessage, true);

		int lineCount = 0;
		Path path = Paths.get( report1File );
		if (Files.notExists(path)) {
			try { Files.createFile(path);						// create file if doesn't exist
			} catch (IOException e) { e.printStackTrace(); }
		}
		try(    FileWriter fw = new FileWriter( report1File, true);  //try-with-resources --> autoclose
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw)) {

			if (!site.equalsIgnoreCase("All")) {
				sql1 = "SELECT * FROM report1 WHERE siteId = '" + site + "'";
			} else {
				sql1 = "SELECT * FROM report1";
			}
			try ( PreparedStatement stmt1 = db.prepareStatement(sql1)) {
				stmt1.setFetchSize(1000);				//number of rows to be fetched when needed
				ResultSet rs1 = stmt1.executeQuery();
				while (rs1.next()) {
					lineCount++;
					if (lineCount == 1) {			// print header as first line
						out.println("siteId,projectId,pathash,globalId");
					}
					out.println(rs1.getString("siteId")+","+rs1.getString("projectId")+","+rs1.getString("pidhash")+","+rs1.getInt("globalId"));
				}
				if (rs1 != null) { rs1.close(); }
				
				for (String line : exclusionPats) {		// add on excluded pats at end
					out.println(line);
				}
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
			}
		} catch( IOException e ) {
			// File writing/opening failed at some stage.
			System.out.println("**Unable to write to  file " + report1File);
		}
	}

	// store to Temp table with integer primary key
	public void storeToTempTableIdKey(Integer rowId, Integer gblId) {
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		String sql = "INSERT INTO " + TempTableIdKey + " ("
				+ "tempId,gblId) "
				+ "VALUES(?,?)";
		try ( PreparedStatement pstmt8 = db.prepareStatement(sql)) {
			pstmt8.setInt(1, rowId);
			pstmt8.setInt(2, gblId);
			pstmt8.executeUpdate();			// store to database
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
		}
	}

	// create temp table with integer primary key
	public void createTempTableIdKey() {
		if (db == null) {
			connectDb();		// connect to database if no connection yet
		}
		boolean tableExists = false;			// check if table already exists
		String sql0 = "SELECT name FROM sqlite_temp_master WHERE type = 'table' AND name = '" +TempTableIdKey+ "'";
		try ( Statement stmt0 = db.createStatement() ) {
			ResultSet rs0 = stmt0.executeQuery(sql0);
			if (rs0.next()) {
				tableExists = true;
			} else {
				tableExists = false; 
			}
			if (rs0 != null) { rs0.close(); }
		} catch(SQLException ex) {
			System.out.println(ex.getMessage());
		}
		if (!tableExists) {				// create table if doesn't exist
			String sqlCreate1 = "CREATE TEMP TABLE IF NOT EXISTS " + TempTableIdKey + " ("
					+ "tempId integer PRIMARY KEY,"
					+ "gblId integer)";				// SQL statement for creating temp table
			try (	Statement stmt1 = db.createStatement() ) {
				stmt1.execute(sqlCreate1);			// create a new table
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
			tempMessage = "new temporary table created: " + TempTableIdKey;
			writeLog(logInfo, tempMessage, true);
		
		} else {			// else delete existing records
			String sqlDelete3 = "DELETE FROM " + TempTableIdKey;
			try ( Statement stmt3 = db.createStatement() ) {
				stmt3.execute(sqlDelete3);		// execute statement
				System.out.println("delete temp table records: " + TempTableIdKey);
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
			}
		}
	}

	// create temp table with all columns indexed
	public void createTempTableAllKey() {
		if (tempTableAllKeyCreated) {	// if already created just return
			return;
		}
		if (db == null) {
			connectDb();				// connect to database if no connection yet
		}
		boolean tableExists = false;	// check if temp table already exists
		String sql0 = "SELECT name FROM sqlite_temp_master WHERE type = 'table' AND name = '" +TempTableAllKey+ "'";
		try ( Statement stmt0 = db.createStatement() ) {
			ResultSet rs0 = stmt0.executeQuery(sql0);
			if (rs0.next()) {
				tableExists = true;		// check if table already exists
			} else {
				tableExists = false; 
			}
			if (rs0 != null) { rs0.close(); }
		} catch(SQLException ex) {
			System.out.println(ex.getMessage());
		}
		if (!tableExists) {
			String sqlCreate1 = "CREATE TEMP TABLE IF NOT EXISTS " + TempTableAllKey + " ("
					+ "id integer PRIMARY KEY,"
					+ "rowId integer,"
					+ "hash1 text,"
					+ "hash2 text,"
					+ "hash3 text,"
					+ "hash4 text,"
					+ "hash5 text,"
					+ "hash6 text,"
					+ "hash7 text,"
					+ "hash8 text,"
					+ "hash9 text,"
					+ "hash10 text)";	// SQL statement for creating a new table
			try ( Statement stmt1 = db.createStatement() ) {
				stmt1.execute(sqlCreate1);		// create a new table
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
			tempMessage = "new temporary table created: " + TempTableAllKey;
			writeLog(logInfo, tempMessage, true);
		
		} else {
			for (int i = 1; i < 11; i++) {				// delete existing indexes
				String sqlDelete2 = "DROP INDEX IF EXISTS index" + i;
				try ( Statement stmt2 = db.createStatement()) {
					stmt2.execute(sqlDelete2);		// delete index
				} catch (SQLException e) {
					System.out.println(e.getMessage());
					if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
				}
			}

			String sqlDelete3 = "DELETE FROM " + TempTableAllKey;	// delete existing records
			try ( Statement stmt3 = db.createStatement() ) {
				stmt3.execute(sqlDelete3);		// execute statement
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
			}
		}

		// copy GlobalMatch patients to temp file, then index each column individually
		int count = 0;
		String sql9 = 		
				"INSERT INTO " +TempTableAllKey+ " (rowId,hash1,hash2,hash3,hash4,hash5,hash6,hash7,hash8,hash9,hash10) "+
						"SELECT id,hash1,hash2,hash3,hash4,hash5,hash6,hash7,hash8,hash9,hash10 FROM GlobalMatch";
		try ( PreparedStatement pstmt9 = db.prepareStatement(sql9)) {
			count = pstmt9.executeUpdate();			// store to database
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			if (e.getMessage().contains(SQL_Exception1)) { stopProcess(e.getMessage()); }
		}
		tempMessage = "transferring " + count + " GlobalMatch patients to Temporary table";
		writeLog(logInfo, tempMessage, true);

		for (int i = 1; i < 11; i++) {
			String sql8 = "CREATE INDEX index"+ i +" ON " + TempTableAllKey + " (hash"+ i +",rowId)";
			try ( Statement stmt8 = db.createStatement()) {
				stmt8.execute(sql8);		// create index
			} catch (SQLException e) {
				System.out.println(e.getMessage());
			}
		}
		tempTableAllKeyCreated = true;
		tempMessage = "created indexes on Temporary table " + TempTableAllKey;
		writeLog(logInfo, tempMessage, true);
	}

	public void stopProcess(String errorMessage) {
		writeLog(logInfo, "** Exception encountered during processing", true);
		writeLog(logInfo, errorMessage, true);
		writeLog(logInfo, "Please make sure DB Browser is not running", true);
		System.exit(-1);
	}

	public static void main(String[] args) {

		int processStep = 0;
		String targetSite = "";
		String tempMatchRules = "";
		String tempFileNamePrefix = "";
		String tempFileNameSuffix = "";
		int tempAtomicIntegerSeed = 0;
		configRootPath = "";
		matchSequence = new ArrayList<Integer>();	// holds match rules to run from config file
		System.out.println(SW_NAME + SW_VERSION);	// show program name and version
		try {
			for (int param = 0; param < args.length; ++param) {
				if (args[param].contains(directorySeparator) || args[param].contains("\\")) {	// check for project root
					configRootPath = args[param];
				} else if (args[param].equalsIgnoreCase("--directory") && args.length > param + 1) {
					param++;
					configRootPath = args[param];
				} else if (args[param].equalsIgnoreCase("--load")) {
					processStep = 1;
				} else if (args[param].equalsIgnoreCase("--match") && args.length > param + 1) {
					processStep = 2;
					param++;
					tempMatchRules = args[param];
				} else if (args[param].equalsIgnoreCase("--prefix") && args.length > param + 1) {
					param++;
					tempFileNamePrefix = args[param];
				} else if (args[param].equalsIgnoreCase("--suffix") && args.length > param + 1) {
					param++;
					tempFileNameSuffix = args[param];
				} else if (args[param].equalsIgnoreCase("--report") && args.length > param + 1) {
					processStep = 3;
					param++;
					targetSite = args[param];
				} else if (args[param].equalsIgnoreCase("--seed") && args.length > param + 1) {
					param++;
					tempAtomicIntegerSeed = Integer.valueOf( args[param] );
				} else if ((args[param].equals("1") || args[param].equals("2"))) {
					processStep = Integer.valueOf(args[param]);
				}
			}
		} catch (Exception e){
			System.out.println("Valid params: Project Root,  Step number: 1=process input files and 2=run match rules.");
			System.exit(-1);
		}

		if (processStep <= 0 || processStep > 3) {
			System.out.println("Valid params: Project Root,  Step number: 1=process input files and 2=run match rules.");
			return;
		}
		
		//configRootPath = System.getenv("GLOBAL_MATCH_BASE");		// read root dir from System environment variable
		configRootPath = configRootPath.replaceAll("\\\\", directorySeparator);	// change dir separator if Windows
		if (configRootPath.endsWith(directorySeparator)) {
			configRootPath = configRootPath.substring(0, configRootPath.length() - 1 ); // remove last / 
		}
		System.out.println("GLOBAL_MATCH_BASE directory: " + configRootPath);
		GlobalMatchSqlite gms = new GlobalMatchSqlite(configRootPath, tempAtomicIntegerSeed);	//create instance
		gms.readConfig(configRootPath);			// read configuration properties file from config subdirectory
		
		if (!tempMatchRules.isEmpty()) {	// if match rules passed in
			matchSequence.clear();			// clear those read from properties file
			String[] tempArr = tempMatchRules.split(delimComma);	// split incoming text
			for (int i = 0; i < tempArr.length; i++) {	
				matchSequence.add( Integer.parseInt(tempArr[i]) );	// save match sequence as integer
			}
		}
		
		if (tempAtomicIntegerSeed > 0) {				// if user value passed in, override config global id seed 
			atomicIntegerSeed = tempAtomicIntegerSeed;
			globalId = new AtomicInteger(atomicIntegerSeed);	// set base Global id
		}

		if (processStep == 1) {
			gms.processInputFiles(processStep, tempFileNamePrefix, tempFileNameSuffix);	// go process input files in input dir
		} else if (processStep == 2) {
			gms.runMatchRules(processStep, matchSequence);	// go run match rules
		} else if (processStep == 3) {
			gms.createReport1(targetSite);					// go create report
		} else {
			System.out.println("Unable to determine which step to process");
		}

		gms.writeAtomicIntegerSeed();		// go save current value of next globalId
		if (db != null) {
			try {
				db.close();				// close database					
			} catch (SQLException e) { /* ignored */}
		}
	}
}
