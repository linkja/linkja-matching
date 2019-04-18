# linkja-disambiguate
Code to match patients across multiple hospitals

## Building
linkja-disambiguate was built using Java 10 (specifically [OpenJDK](https://openjdk.java.net/)).  It can be opened from within an IDE like Eclipse or IntelliJ IDEA and compiled, or compiled from the command line using [Maven](https://maven.apache.org/).

You can build linkja-disambiguate via Maven:

`mvn clean package`

This will compile the code, run all unit tests, and create an executable JAR file under the .\target folder with all dependency JARs included.  The JAR will be named something like `Disambiguate-1.0-jar-with-dependencies.jar`.

## Program Use
You can run the executable JAR file using the standard Java command:
`java -jar Disambiguate-1.0-jar-with-dependencies.jar `

The program has two modes: command line, and a GUI.  By default it will run as a command line application.  If you would like to use the GUI, specify `--gui` from the command line.
