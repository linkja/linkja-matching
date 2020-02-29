package org.linkja.matching.Decrypt;

import java.io.*;
import java.nio.file.FileSystems;

public class DecryptFilesRunner {
  private static final String PROGRAM_NAME = "Linkja Matching - Decrypt Utility";
  private static final String PROGRAM_VERSION = "1.0";

  public static void main(String[] args) {
    System.out.println(String.format("%s %s", PROGRAM_NAME, PROGRAM_VERSION));

    String dataFileDirectory = "";
    String encryptedFilePrefix = "";
    String encryptedFileSuffix = "";
    String decryptionKeyPath = "";

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
          decryptionKeyPath = args[param];
        }
      }
    } catch (Exception e) {
      System.out.println("Valid parameters: --directory, --prefix, --suffix, --decryptionKey");
      System.exit(-1);
    }

    File directory = FileSystems.getDefault().getPath(dataFileDirectory).normalize().toAbsolutePath().toFile();
    String finalEncryptedFilePrefix = encryptedFilePrefix;
    String finalEncryptedFileSuffix = encryptedFileSuffix;
    File[] fileList = directory.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(finalEncryptedFilePrefix) && name.endsWith(finalEncryptedFileSuffix);
      }
    });

    File privateKeyFile = new File(decryptionKeyPath);
    for (File file : fileList) {
      String printableFilePath = file.getAbsolutePath();
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
}
