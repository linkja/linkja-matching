package org.linkja.disambiguate;

import java.util.Arrays;

public class Runner {
  public static void main(String[] args) {
    if (args.length == 0) {
      GlobalMatchSqlite.main(args);
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
