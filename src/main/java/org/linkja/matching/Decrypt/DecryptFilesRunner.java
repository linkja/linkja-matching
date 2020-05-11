package org.linkja.matching.Decrypt;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Paths;

public class DecryptFilesRunner {
  private static final String PROGRAM_NAME = "Linkja Matching - Decrypt Utility";
  private static final String PROGRAM_VERSION = "v1.1.0-beta";

  public static void main(String[] args) {
    System.out.println(String.format("%s %s", PROGRAM_NAME, PROGRAM_VERSION));

    String dataFileDirectory = "";
    String encryptedFilePrefix = "";
    String encryptedFileSuffix = "";
    String decryptionKeyName = "";

    try {
      for (int param = 0; param < args.length; ++param) {
        if (args[param].equalsIgnoreCase("--directory") && args.length > param + 1) {
          param++;
          dataFileDirectory = args[param];
        } else if (args[param].equalsIgnoreCase("--prefix") && args.length > param + 1) {
          param++;
          encryptedFilePrefix = args[param];
        } else if (args[param].equalsIgnoreCase("--suffix") && args.length > param + 1) {
          param++;
          encryptedFileSuffix = args[param];
          if (!encryptedFileSuffix.startsWith(".")) {
            encryptedFileSuffix = "." + encryptedFileSuffix;
          }
        } else if (args[param].equalsIgnoreCase("--decryptionKey") && args.length > param + 1) {
          param++;
          decryptionKeyName = args[param];
        }
      }
    } catch (Exception e) {
      printUsage();
      System.exit(-1);
    }

    boolean invalidParameters = false;
    if (dataFileDirectory.equals("")) {
      System.out.println("You must specify a data file directory with --directory (e.g., --directory /Users/linkja/data)");
      invalidParameters = true;
    }
    if (encryptedFilePrefix.equals("")) {
      System.out.println("You must specify a prefix for the encrypted files with --prefix (e.g., --prefix TestProject-)");
      invalidParameters = true;
    }
    if (encryptedFileSuffix.equals("")) {
      System.out.println("You must specify a suffix for the encrypted files with --suffix (e.g., --suffix enc)");
      invalidParameters = true;
    }
    if (decryptionKeyName.equals("")) {
      System.out.println("You must specify a path to your private RSA key with --decryptionKey (e.g., --decryptionKey test-project-priv.key)");
      invalidParameters = true;
    }

    if (invalidParameters) {
      printUsage();
      System.exit(-1);
    }

    File projectDirectory = FileSystems.getDefault().getPath(dataFileDirectory).normalize().toAbsolutePath().toFile();
    String finalEncryptedFilePrefix = encryptedFilePrefix;
    String finalEncryptedFileSuffix = encryptedFileSuffix;
    File inputDirectory = Paths.get(projectDirectory.getAbsolutePath(), "data", "input").toFile();

    File[] fileList = inputDirectory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(finalEncryptedFilePrefix) && name.endsWith(finalEncryptedFileSuffix);
      }
    });

    File privateKeyFile = Paths.get(projectDirectory.getAbsolutePath(), decryptionKeyName).toFile();
    for (File file : fileList) {
      String printableFilePath = file.getAbsolutePath();
      System.out.printf("Processing %s\r\n", printableFilePath);
      try {
        DecryptFiles.decrypt(file, privateKeyFile, encryptedFileSuffix);
      }
      catch (FileNotFoundException fnfe) {
        System.out.println(String.format("Unable to find the file %s.  We will proceed with the next file", printableFilePath));
        System.out.println(fnfe.getMessage());
        fnfe.printStackTrace();
      }
      catch (Exception exc) {
        System.out.println(String.format("We encountered an exception when processing %s", printableFilePath));
        System.out.println(exc.getMessage());
        exc.printStackTrace();
      }
    }
  }

  private static void printUsage() {
    System.out.println("Valid parameters: --directory, --prefix, --suffix, --decryptionKey");
  }
}
