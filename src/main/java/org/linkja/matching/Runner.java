package org.linkja.matching;

import org.linkja.matching.Decrypt.DecryptFilesRunner;

import java.util.Arrays;

public class Runner {
  public static void main(String[] args) {
    if (args.length == 0) {
      GlobalMatchSqlite.main(args);
    }
    else {
      // First, check to see if this is to be run to decrypt files.  We want to catch that before the GUI option since
      // we haven't yet built a GUI for it...
      String decryptOption = Arrays.stream(args).filter(x -> x.equalsIgnoreCase("--decrypt")).findFirst().orElse(null);
      if (decryptOption != null) {
        DecryptFilesRunner.main(args);
      }
      else {
        // Check to see if the user specified that they want to run with a GUI or not
        String guiOption = Arrays.stream(args).filter(x -> x.equalsIgnoreCase("--gui")).findFirst().orElse(null);
        if (guiOption != null) {
          GlobalMatchTasks.main(args);
        }
        else {
          GlobalMatchSqlite.main(args);
        }
      }
    }
  }
}
