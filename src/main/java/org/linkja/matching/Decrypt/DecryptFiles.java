package org.linkja.matching.Decrypt;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.linkja.core.EncryptedHashFileMetadata;
import org.linkja.core.LinkjaException;
import org.linkja.core.crypto.AesEncryptParameters;
import org.linkja.crypto.AesResult;
import org.linkja.crypto.Library;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DecryptFiles {
  private static final int HASH_BLOCK_SIZE = 64;  // 32 bytes per SHA-256, x2 as byte representation

  public static void decrypt(File encryptedFile, File privateKeyFile, String encryptedFileSuffix) throws LinkjaException, IOException {
      String printableFilePath = encryptedFile.getAbsolutePath();
      FileInputStream fileInStream = new FileInputStream(encryptedFile);
      BufferedInputStream inputStream = new BufferedInputStream(fileInStream);

      // First pull out the header information.  The metadata class handles a lot of the validation checks, but we will
      // still assert any of the assumptions we have here for documentation purposes.
      EncryptedHashFileMetadata metadata = EncryptedHashFileMetadata.read(inputStream, privateKeyFile);
      if (metadata == null) {
        System.out.println(String.format("We failed to read the file metadata for %s - it may be invalid", printableFilePath));
      }

      BufferedWriter writer = Files.newBufferedWriter(Paths.get(encryptedFile.getAbsolutePath().replace(encryptedFileSuffix, ".csv")));
      CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);
      csvPrinter.print("siteid");
      csvPrinter.print("projectid");
      csvPrinter.print("PIDHASH");

      int numHashColumns = metadata.getNumHashColumns();
      for (int columnCounter = 1; columnCounter <= (numHashColumns - 1); columnCounter++) {
        csvPrinter.print(String.format("hash%d", columnCounter));
      }
      csvPrinter.println();

      // Write out the site ID and project ID
      csvPrinter.print(metadata.getSiteId().trim());
      csvPrinter.print(metadata.getProjectId().trim());

      // Note that inputStream will have progressed as we read out the metadata.  At this point we can just read fixed-size
      // blocks from the input stream and begin the decryption process.  The assumption of course is that they represent
      // the full SHA-256 block.
      long expectedBlocks = metadata.getNumHashColumns() * metadata.getNumHashRows();
      long readBlocks = 0;
      long rowCounter = 1;
      int tokenCounter = 1;
      boolean eof = false;
      while (!eof) {
        byte[] data = new byte[HASH_BLOCK_SIZE];
        int dataSize = inputStream.read(data, 0, HASH_BLOCK_SIZE);
        if (dataSize != HASH_BLOCK_SIZE) {
          eof = true;
          continue;
        }

        readBlocks++;

        // If both the tag and data start with 0s, that indicates they are empty blocks and that this particular hash
        // is empty.  We will skip trying to decrypt it.
        if (data[0] == 0x00) {
          csvPrinter.print("");
        }
        else {
          csvPrinter.print(Library.revertSecureHash(
            new String(data), metadata.getSessionKey(), Long.toString(rowCounter), Integer.toString(tokenCounter)).toUpperCase());
        }

        tokenCounter++;

        if (readBlocks % numHashColumns == 0) {
          rowCounter++;      // Progress to the next row
          tokenCounter = 1;  // Reset to start over
          csvPrinter.println();

          // When we start a new line, if we have more data to process we need to prefix it with the site ID and project ID
          if (readBlocks < expectedBlocks) {
            csvPrinter.print(metadata.getSiteId().trim());
            csvPrinter.print(metadata.getProjectId().trim());
          }
        }
      }

      csvPrinter.close(true);

      if (readBlocks != expectedBlocks) {
        throw new LinkjaException(String.format("Expected %d data blocks but read %d", expectedBlocks, readBlocks));
      }
  }
}
