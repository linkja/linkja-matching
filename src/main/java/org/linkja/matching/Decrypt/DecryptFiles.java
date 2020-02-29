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
  private static final int AES_TAG_SIZE = 16;
  private static final int AES_BLOCK_SIZE = 128;

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

      // Note that inputStream will have progressed as we read out the metadata.  At this point we can just read fixed-size
      // blocks from the input stream and begin the decryption process.  The assumption of course is that they represent
      // the full AES encrypted block.
      long expectedBlocks = metadata.getNumHashColumns() * metadata.getNumHashRows();
      long readBlocks = 0;
      AesEncryptParameters encryptParameters = metadata.getEncryptParameters();
      boolean eof = false;
      while (!eof) {
        byte[] data = new byte[AES_BLOCK_SIZE];
        int dataSize = inputStream.read(data, 0, AES_BLOCK_SIZE);
        if (dataSize != AES_BLOCK_SIZE) {
          eof = true;
          continue;
        }

        byte[] tag = new byte[AES_TAG_SIZE];
        int tagSize = inputStream.read(tag, 0, AES_TAG_SIZE);
        if (tagSize != AES_TAG_SIZE) {
          eof = true;
          continue;
        }

        readBlocks++;

        // If both the tag and data start with 0s, that indicates they are empty blocks and that this particular hash
        // is empty.  We will skip trying to decrypt it.
        if (tag[0] == 0x00 && data[0] == 0x00) {
          csvPrinter.print("");
        }
        else {
          AesResult decryptResult = Library.aesDecrypt(data, encryptParameters.getAad(), encryptParameters.getKey(), encryptParameters.getIv(), tag);
          if (decryptResult == null || decryptResult.data == null) {
            throw new LinkjaException("There was an error when trying to decrypt one of the hash records.");
          }

          csvPrinter.print(new String(decryptResult.data));
        }

        if (readBlocks % metadata.getNumHashColumns() == 0) {
          csvPrinter.println();
        }
      }

      csvPrinter.close(true);

      if (readBlocks != expectedBlocks) {
        throw new LinkjaException(String.format("Expected %d data blocks but read %d", expectedBlocks, readBlocks));
      }
  }
}
