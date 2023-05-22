package com.github.sbcharr;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class Utils {
  public static String generateRandomText(int length) {
    String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    StringBuilder sb = new StringBuilder(length);
    Random random = new Random();

    for (int i = 0; i < length; i++) {
      int index = random.nextInt(characters.length());
      char randomChar = characters.charAt(index);
      sb.append(randomChar);
    }
    return sb.toString();
  }

  public static int getByteSize(String str, String charsetName) {
    try {
      byte[] bytes = str.getBytes(charsetName);
      return bytes.length;
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      return -1;
    }
  }

  public static int getByteSizeOfJsonString(String jsonString) {
    byte[] bytes = jsonString.getBytes(StandardCharsets.UTF_8);
    return bytes.length;
  }
}
