package externalSortPackage;

import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.net.URISyntaxException;
//import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
//import java.util.Properties;
import java.util.stream.Collectors;

import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.binding.When;
import javafx.concurrent.Task;
import javafx.geometry.Insets;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonBar.ButtonData;
import javafx.scene.control.ButtonType;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Dialog;
import javafx.scene.control.DialogPane;
import javafx.scene.control.Label;
import javafx.scene.control.Separator;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.TextInputDialog;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.DirectoryChooser;
import javafx.stage.Stage;
import javafx.util.Pair;

public class GlobalMatchTasks extends Application {
	
	private String projectRoot;
	private GlobalMatchSqlite gMatch;
	private String selectedDirectory;
	private String prefixText;
	private String suffixText;

	private BorderPane root = null;
	private Button rootButton = new Button("Root");			// Create the Buttons
	private Button inputButton = new Button("Load Data");
	private Button matchButton = new Button("Match Rules");
	private Button reportButton = new Button("Report Data");
	private Button helpButton = new Button("Help");
	private Button exitButton = new Button("Exit");
	
	private TextField baseDirectory;
	private Label runStatus;
	private TextArea message;
	private TextArea exception;
	private TextField matchRule;
	
	private static String configRootPath;
	private static String inputFileNamePrefix;
	private static String inputFileNameSuffix;
	//private static String inputDir;
	//private static String outputDir;
	//private static String processedDir;
	//private static String dbDirectory;
	//private static String dbName;
	//private static final String PROJECT_ROOT = "%ProjectRoot%";
	//private static final String configFileName = "global-match.properties";

	private static final DirectoryChooser directoryChooser = new DirectoryChooser();
	private static final String directorySeparator = "/";
	private final String[] checkBoxNames = new String[]{
			"Rule 0","Rule 1","Rule 2","Rule 3","Rule 4","Rule 5","Rule 6",
			"Rule 7","Rule 8","Rule 9","Rule 10","Rule 11","Rule 12",};
	private final CheckBox[] checkBoxGroup = new CheckBox[checkBoxNames.length];	// array of check boxes

	public static void main(String[] args) {
		configRootPath = "";
		try {
			for (int param = 0; param < args.length; ++param) {
				if (args[param].contains(directorySeparator) || args[param].contains("\\")) {	// check for project root
					configRootPath = args[param];
				} else if (args[param].equalsIgnoreCase("--directory") && args.length > param + 1) {
					param++;
					configRootPath = args[param];
				}
			}
		} catch (Exception e){
			System.out.println("Valid params: Project Root,  Step number: 1=process input files and 2=run match rules.");
			System.exit(-1);
		}

		/*
		try {
			configRootPath = getJarPath();
		} catch (IOException | URISyntaxException e) { e.printStackTrace(); }
		*/
		configRootPath = changeDirectorySeparator(configRootPath);		// change file separator if Windows
		if (configRootPath.endsWith(directorySeparator)) {
			configRootPath = configRootPath.substring(0, configRootPath.length() - 1 ); // remove last / 
		}
		//readConfig(configRootPath);					// read config file
		inputFileNamePrefix = "Site";
		inputFileNameSuffix = ".csv";
		Application.launch(args);
	}

	@Override
	public void start(Stage stage) {
		
		// Create the ButtonBox
		HBox buttonBox = new HBox();
		buttonBox.setPrefWidth(110);	// set preferred width of nodes in HBox
		buttonBox.setSpacing(5);		// set spacing between nodes
		
		baseDirectory = new TextField(configRootPath);
		baseDirectory.setPrefColumnCount(60);
		runStatus = new Label("");
		matchRule = new TextField("match rule");
		matchRule.setPrefColumnCount(60);
		message = new TextArea("");		// Create the TextAreas
		message.setPrefColumnCount(60);
		message.setPrefRowCount(5);
		exception = new TextArea("");
		exception.setPrefColumnCount(60);
		exception.setPrefRowCount(3);

		GridPane topGrid = new GridPane();
		topGrid.setVgap(5);					//Set vertical gap 
		topGrid.setHgap(10);				//Set vertical gap 
		topGrid.addRow(0, new Label("Project Root: "), baseDirectory);
		topGrid.addRow(1, new Label("Processing: "), runStatus);

		// create match rule checkbox area
		Label topLabel = new Label("Global Match Available Rules");
		HBox topHBox = new HBox(topLabel);
		topHBox.setPadding(new Insets(2, 5, 5, 5));

		GridPane checkBoxGrid = new GridPane();
		//checkBoxGrid.setStyle("-fx-background-color: LIGHTBLUE;");
		checkBoxGrid.setMinSize(450, 250);		//Setting size for the pane 
		checkBoxGrid.setPadding(new Insets(10, 10, 10, 80)); // Insets(top, right, bottom, left)
		checkBoxGrid.setVgap(10);		//Set vertical and horizontal gaps between columns
		checkBoxGrid.setHgap(70);  

		Label checkBoxesLabel = new Label("Select match rule(s) to run");
		for (int i = 0; i < checkBoxNames.length; i++) {
			checkBoxGroup[i] = new CheckBox(checkBoxNames[i]);		// create check boxes
		}

		final Separator separator = new Separator();
		final Separator separator2 = new Separator();
		checkBoxGrid.add(checkBoxesLabel, 0, 0, 2, 1); // grid.add(Node, colIndex, rowIndex, colSpan, rowSpan):
		checkBoxGrid.add(separator, 0, 1, 2, 1);
		for (int i = 2; i < 9; i++) {
			checkBoxGrid.add(checkBoxGroup[i - 2], 0, i);
			if (i < 8) {
				checkBoxGrid.add(checkBoxGroup[i + 5], 1, i);
			}
		}
		checkBoxGrid.add(separator2, 0, 10, 2, 1);
		// end of checkbox grid

		GridPane messageGrid = new GridPane();				// message areas grid
		messageGrid.addRow(0, new Label("Match Rules:  "), matchRule);
		messageGrid.addRow(1, new Label("Message: "), message);
		messageGrid.addRow(2, new Label("Exception:"), exception);

		// Create the Event-Handlers for the Buttons
		rootButton.setMinWidth(buttonBox.getPrefWidth());
		rootButton.setStyle("-fx-font-weight: bold;");
		Tooltip tt0 = new Tooltip();
		tt0.setText("Set project root directory");
		tt0.setStyle("-fx-font: normal bold 14 serif; -fx-base: mistyrose; -fx-text-fill: orange;");
		rootButton.setTooltip( tt0 );
		rootButton.setOnAction( event -> {
			directoryChooser.setTitle("Select Project Root directory");		// Set title
	        directoryChooser.setInitialDirectory(new File(configRootPath));	// Set Initial Directory
	        //directoryChooser.setInitialDirectory(new File(System.getProperty("user.home")));
			File selectedDirectoryFile = directoryChooser.showDialog(stage);
			if(selectedDirectoryFile == null){
				selectedDirectory = "";
			}else{
				selectedDirectory = selectedDirectoryFile.getAbsolutePath();
			}
			baseDirectory.setText(selectedDirectory);
		});

		inputButton.setMinWidth(buttonBox.getPrefWidth());
		inputButton.setStyle("-fx-font-weight: bold;");
		Tooltip tt1 = new Tooltip();
		tt1.setText("Process incoming hash files");
		tt1.setStyle("-fx-font: normal bold 14 serif; -fx-base: mistyrose; -fx-text-fill: orange;");
		inputButton.setTooltip( tt1 );
		inputButton.setOnAction( event -> {
			clearMessageAreas();
			String targetDir = baseDirectory.getText().trim();
			if (targetDir.isEmpty() || (!targetDir.contains(directorySeparator) && !targetDir.contains("\\"))) {
				exception.setText("Project root must contain directory separator\nExample:  C:/GlobalMatch");
			} else {
				//https://code.makery.ch/blog/javafx-dialogs-official/
				//https://stackoverflow.com/questions/31556373/javafx-dialog-with-2-input-fields
				// start of popup dialog for input file filters
				Dialog<Pair<String, String>> dialog = new Dialog<>();  // Create the custom dialog.
				dialog.setTitle("Select input file filters");
				dialog.setHeaderText("Enter Prefix and Suffix for input file filters");
				ButtonType okButtonType = new ButtonType("OK", ButtonData.OK_DONE);	// Set the button types
				dialog.getDialogPane().getButtonTypes().addAll(okButtonType, ButtonType.CANCEL);

				GridPane diagPane = new GridPane();		// gridpane to hold 2 input fields
				diagPane.setHgap(10);
				diagPane.setVgap(10);
				diagPane.setPadding(new Insets(20, 150, 10, 10));
				TextField prefix = new TextField(inputFileNamePrefix);	// set default value
				prefix.setPromptText("Prefix");
				TextField suffix = new TextField(inputFileNameSuffix);	// set default value
				suffix.setPromptText("Suffix");
				diagPane.add(new Label("Prefix:"), 0, 0);				// position inputfields
				diagPane.add(prefix, 1, 0);
				diagPane.add(new Label("Suffix:"), 0, 1);
				diagPane.add(suffix, 1, 1);
				dialog.getDialogPane().setContent(diagPane);
				Platform.runLater(() -> prefix.requestFocus());  // request focus for prefix field
				// Convert the result to a prefix/suffix-pair when button is clicked.
				dialog.setResultConverter(dialogButton -> {
					if (dialogButton == okButtonType) {
						return new Pair<>(prefix.getText(), suffix.getText());
					}
					return null;
				});
				Optional<Pair<String, String>> result = dialog.showAndWait();
				result.ifPresent(pair -> {
					prefixText = pair.getKey();		// get text that was entered
					suffixText = pair.getValue();
				});		//*** end of popup dialog
				
				// start of task1 in separate thread
				final int currentStep = 1;
				Task<Void> task1 = new Task<Void>() {
					@Override protected Void call() throws Exception {
						updateMessage("Begin processing input hash files");
						projectRoot = baseDirectory.getText();			// get which project working on
						gMatch = new GlobalMatchSqlite(projectRoot, 0);	// create instance of global match
						gMatch.processInputFiles(currentStep, prefixText, suffixText); // go read input files
						updateMessage(message.getText() + "\nFinished processing input hash files\nCheck log for additional details.");
						return null;
					}
				};
				message.textProperty().bind(task1.messageProperty());	// bind message area for communication
				runStatus.textProperty().bind(task1.stateProperty().asString());	// bind runStatus label for communication
				//runStatus.textProperty().bind(task1.runningProperty().asString());	// bind runStatus label for communication
				//runStatus.textProperty().bind(
				//		new When(task1.runningProperty().asString().isEqualTo("true")).then("Running")
				//		.otherwise("Idle"));
				new Thread(task1).start();								// start task in new thread
				// end of task1 in separate thread
			}
		});

		matchButton.setMinWidth(buttonBox.getPrefWidth());
		matchButton.setStyle("-fx-font-weight: bold;");
		Tooltip tt2 = new Tooltip();
		tt2.setText("Run match rules and assign Global Ids");
		tt2.setStyle("-fx-font: normal bold 14 Helvetica; -fx-base: mistyrose; -fx-text-fill: orange;");
		matchButton.setTooltip( tt2 );
		matchButton.setOnAction( (event) -> {	// define on action event
			Integer globalIdStart = 100;
			clearMessageAreas();
			List<Integer> checkedRules = getCheckedRules();
			String targetDir = baseDirectory.getText().trim();
			if (targetDir.isEmpty() || (!targetDir.contains(directorySeparator) && !targetDir.contains("\\"))) {
				exception.setText("Project root must contain directory separator\nExample:  C:/GlobalMatch");
			} else if (checkedRules.size() == 0) {
				exception.setText("No match rule(s) selected");
			} else {
				// start of popup dialog to ask Global Id seed
				TextInputDialog dialog2 = new TextInputDialog("100");	// set default value
				dialog2.setTitle("Global Id Selection Dialog");
				dialog2.setHeaderText("Select Global Id seed to use");	// set title and header text
				dialog2.setContentText("Please enter Global Id seed:");

				Optional<String> result = dialog2.showAndWait();		// get result
				if (result.isPresent()){
					globalIdStart = Integer.valueOf(result.get());	// get seed, defaults to 100 above
				}		// end of popup dialog
				
				// start of task2 in separate thread
				final List<Integer> ruleList = checkedRules.stream()
						.collect(Collectors.toList());		// copy list so is independent list - Java 8
				final int currentStep = 2;
				final Integer globalIdSeed = globalIdStart;
				Task<Void> task2 = new Task<Void>() {			// do task in separate thread
					@Override protected Void call() throws Exception {
						updateMessage("Running match rules\nReloading GlobalMatch Table\nChecking for derived (alias) patients");
						projectRoot = baseDirectory.getText();			// get which project working on
						gMatch = new GlobalMatchSqlite(projectRoot, globalIdSeed);	// create instance of global match
						gMatch.runMatchRules(currentStep, ruleList);	// go do global patient match
						updateMessage(message.getText() + "\nCompleted global patient match rules\nCheck log for additional details.");
						return null;
					}
				};
				message.textProperty().bind(task2.messageProperty());	// bind message area for communication
				runStatus.textProperty().bind(task2.stateProperty().asString());	// bind runStatus label for communication
				//runStatus.textProperty().bind(task2.runningProperty().asString());	// bind run status label for communication
				new Thread(task2).start();
				// end of task2 in separate thread
			}
		});

		reportButton.setMinWidth(buttonBox.getPrefWidth());
		reportButton.setStyle("-fx-font-weight: bold;");
		Tooltip tt3 = new Tooltip();
		tt3.setText("Report out data");
		tt3.setStyle("-fx-font: normal bold 14 serif; -fx-base: mistyrose; -fx-text-fill: orange;");
		reportButton.setTooltip( tt3 );
		reportButton.setOnAction( (event) -> {
			String site;
			clearMessageAreas();
			String targetDir = baseDirectory.getText().trim();
			if (targetDir.isEmpty() || (!targetDir.contains(directorySeparator) && !targetDir.contains("\\"))) {
				exception.setText("Project root must contain directory separator\nExample:  C:/GlobalMatch");
			} else {
				// start of popup dialog to ask site
				TextInputDialog dialog3 = new TextInputDialog("");
				dialog3.setTitle("Site Selection Dialog");
				dialog3.setHeaderText("Select site id for report");
				dialog3.setContentText("Please enter site id:");
				Optional<String> result = dialog3.showAndWait();		// get result
				if (result.isPresent()){
					site = result.get();
				} else {
					site = "xx";
				}
				// end of popup dialog

				// start of task3 in separate thread
				final String targetSite = site;
				Task<Void> task3 = new Task<Void>() {			// do task in separate thread
					@Override protected Void call() throws Exception {
						updateMessage("Generating Report 1 from GlobalMatch table for site " + targetSite);
						projectRoot = baseDirectory.getText();			// get which project working on
						gMatch = new GlobalMatchSqlite(projectRoot, 0);	// create instance of global match
						gMatch.createReport1( targetSite );
						updateMessage(message.getText() + "\nCompleted Report 1\nCheck log for additional details.");
						//updateRunning("idle");
						return null;
					}
				};
				message.textProperty().bind(task3.messageProperty());	// bind message area for communication
				runStatus.textProperty().bind(task3.stateProperty().asString());	// bind runStatus label for communication
				//runStatus.textProperty().bind(task3.runningProperty().asString());	// bind run status label for communication
				new Thread(task3).start();
				// end of task3 in separate thread
			}
		});

		exitButton.setMinWidth(buttonBox.getPrefWidth());
		exitButton.setStyle("-fx-font-weight: bold;");
		exitButton.setOnAction( (event) -> {
			Platform.exit();
		});

		helpButton.setMinWidth(buttonBox.getPrefWidth());
		helpButton.setStyle("-fx-font-weight: bold;");
		helpButton.setOnAction( (event) -> {
			// show popup help https://code.makery.ch/blog/javafx-dialogs-official/
			Alert alert = new Alert(AlertType.INFORMATION);
			alert.setTitle("Global Matching Rules");
			alert.setHeaderText("Matching Rule Comparisons");
			alert.setContentText(
					"Rule 1:  column 1, 2, 5, 9, 10\n" +
					"Rule 2:  column 3, 4, 6\n" +
					"Rule 3:  column 1, 1\n" +
					"Rule 4:  column 1, 2\n" +
					"Rule 5:  column 1, 5\n" +
					"Rule 6:  column 1, 9\n" +
					"Rule 7:  column 1, 10\n" +
					"Rule 8:  column 3, 3\n" +
					"Rule 9:  column 3, 4\n" +
					"Rule 10: column 3, 6\n" +
					"Rule 11: column 7, 7\n" +
					"Rule 12: column 8, 8");
			alert.showAndWait();
		});

		// add buttons to ButtonBox
		buttonBox.getChildren().addAll(rootButton,inputButton,matchButton,reportButton,helpButton,exitButton);
		
		clearMessageAreas();
		VBox centerVBox = new VBox();			// create VBox
		centerVBox.getChildren().addAll(checkBoxGrid, messageGrid);
		
		root = new BorderPane();				// Create the BorderPane
		root.setStyle("-fx-padding: 10;" +
				"-fx-border-style: solid inside;" +
				"-fx-border-width: 2;" +
				"-fx-border-insets: 5;" +
				"-fx-border-radius: 5;" +
				"-fx-border-color: blue;");		// Set Style-properties of BorderPane
		root.setTop(topGrid);
		root.setCenter(centerVBox);
		root.setBottom(buttonBox);

		Scene scene = new Scene(root, 975, 650);			// Create the Scene
		stage.setScene(scene);								// Add the scene to the Stage
		stage.setTitle("Global Patient Matching Tasks");	// Set title of Stage
		stage.show();										// Display the Stage
	}
	
	private void clearMessageAreas() {
		//message.setText("");			// can't clear because bound with task	
		//runStatus.setText("idle");	// can't clear because bound with task
		exception.setText("");			// clear message areas
		matchRule.setText("");
	}
	
	private List<Integer> getCheckedRules() {
		List<Integer> checkedList = new ArrayList<Integer>();
		int ruleCount = 0;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < checkBoxGroup.length; i++) {
			if (checkBoxGroup[i].isSelected()) {
				checkedList.add(i);
				ruleCount++;
				if (ruleCount > 1) {
					sb.append(", " +checkBoxNames[i]);
				} else {
					sb.append(checkBoxNames[i]);
				}
			}
		}
		matchRule.setText(sb.toString());
		return checkedList;
	}
		
	private static String changeDirectorySeparator(String filePath) {
		return filePath.replaceAll("\\\\", directorySeparator);	// change dir separator if Windows
	}

	
	/*
	private static void readConfig(String projRoot) {
		
		String configFilePath = makeFilePath(makeFilePath(projRoot, "config"), configFileName);  	// get config path

		// read config data from properties file using try with resources, means autoclose
		Properties prop = new Properties();
		try ( InputStream input = new FileInputStream( configFilePath )) {
			prop.load( input );
			//configFileDir = prop.getProperty("ConfigFilesDirectory");
			//inputDir = prop.getProperty("InputFilesDirectory");
			//inputDir = changeDirectorySeparator(inputDir);				// change file separator if Windows
			//inputDir = inputDir.replaceFirst(PROJECT_ROOT, projRoot);
			//outputDir = prop.getProperty("OutputFilesDirectory");
			//outputDir = changeDirectorySeparator(outputDir);			// change file separator if Windows
			//outputDir = outputDir.replaceFirst(PROJECT_ROOT, projRoot);
			//processedDir = prop.getProperty("ProcessedFilesDirectory");
			//processedDir = changeDirectorySeparator(processedDir);		// change file separator if Windows
			//processedDir = processedDir.replaceFirst(PROJECT_ROOT, projRoot);
			//dbDirectory = prop.getProperty("DbDirectory");
			//dbDirectory = dbDirectory.replaceFirst(PROJECT_ROOT, projRoot);
			//dbName = prop.getProperty("DbName");
			inputFileNamePrefix = prop.getProperty("InputFileNamePrefix");
			inputFileNameSuffix = prop.getProperty("InputFileNameSuffix");
		} catch (Exception e) {
			//e.printStackTrace();
			System.out.println("**read config error message: "+e.getMessage());
			System.exit(1);
		}
	}
	*/
	
	/*
	private static String makeFilePath(String filePath, String fileName) {
		filePath = changeDirectorySeparator(filePath);
		if (filePath.endsWith(directorySeparator)) {
			return filePath + fileName;
		} else {
			return filePath + directorySeparator + fileName;
		}
	}
	*/

	/*
	private static String getJarPath() throws IOException, URISyntaxException {
		File f = new File(GlobalMatchTasks.class.getProtectionDomain().getCodeSource().getLocation().toURI());
		String jarPath = f.getCanonicalPath().toString();
		String jarDir = jarPath.substring( 0, jarPath.lastIndexOf( File.separator ));
		return jarDir;
	}
	*/
}
