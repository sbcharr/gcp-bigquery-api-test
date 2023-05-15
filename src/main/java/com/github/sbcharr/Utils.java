package com.github.sbcharr;

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
}
