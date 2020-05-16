# linkja-matching
Code to match patients across multiple hospitals

## Building
linkja-matching was built using Java 10 (specifically [OpenJDK](https://openjdk.java.net/)).  It can be opened from within an IDE like Eclipse or IntelliJ IDEA and compiled, or compiled from the command line using [Maven](https://maven.apache.org/).

You can build linkja-matching via Maven:

`mvn clean package`

This will compile the code, run all unit tests, and create an executable JAR file under the .\target folder with all dependency JARs included.  The JAR will be named something like `Matching-1.0-jar-with-dependencies.jar`.

## Program Use
You can run the executable JAR file using the standard Java command:
`java -jar Matching-1.0-jar-with-dependencies.jar `

The program has two modes: command line, and a GUI.  By default it will run as a command line application.  If you would like to use the GUI, specify `--gui` from the command line.

### Decryption

You can run the executable JAR file (the one including `with-dependencies` in the file name) using the standard Java command:
`java -jar <JAR path>`

For decryption, linkja-matching uses a special C library (.dll/.dylib/.so, depending on your operating system).  You will need to tell Java where to find this library when you try to run the program.  Otherwise, you will get an error:

```
Exception in thread "main" java.lang.UnsatisfiedLinkError: no linkjacrypto in java.library.path:
```

The library may be placed in any directory found by the Java library path.  If you would like to specify the library, you can include the `-Djava.library.path=` option when running the program.
This can be the same directory as the linkja-hashing JAR file (e.g., `-Djava.library.path=.`).

Note that where files are used for input, they can be specified as a relative or absolute path.

Decryption uses the following parameters, all of which are required:

```
--decrypt            Enables the decryption mode
--directory          The root directory of the project.  The program assumes there the project structure
                     follows our convention of a `data/input` sub folder under which the encrypted files
                     and private RSA key are stored.
--prefix             The prefix of all encrypted file names (e.g., hashes)
--suffix             The suffix (extension) of all encrypted file names (e.g., .enc).
                     It is recommended to include the '.' in the extension.
--decryptionKey      The name of the RSA private key to decrypt the files.  This should be the private
                     part of the public key shared with each hashing site.
                     This is JUST the file name - the program will look under --directory to find the path
                     to the file.
```

**Example**

Given the following project structure:

```
C:\Projects\Project1\
                     data\
                          input\
                                hashes_1_Linkja Project_20200516142804.enc
                                hashes_2_Linkja Project_20200516142804.enc
                                private.key
```

The following command would be used to decrypt the two files:

```
java -Djava.library.path=. -jar linkja-matching.jar --decrypt
  --directory C:\Projects\Project1 --prefix hashes --suffix .enc
  --decryptionKey private.key

```
