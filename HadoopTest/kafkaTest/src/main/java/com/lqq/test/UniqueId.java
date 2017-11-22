package com.lqq.test;

import sun.applet.Main;

public class UniqueId {

    public static void main(String[] args) {
    }

    static byte[] cmb = { 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90 };
    public static String curTimeStr = getRadixOf62(System.currentTimeMillis());
    public static String countPerSecondS = "9999999999999";
    public static long identifySeed = 1L;
    public static long countPerSecond = new Long(countPerSecondS).longValue();

    public static synchronized String getUniqueIdentify(){
        if(identifySeed >= countPerSecond){
            identifySeed = 1L;
            curTimeStr = getRadixOf62(System.currentTimeMillis());
        }
        String tmp = String.valueOf(identifySeed);
        int len = tmp.length();
        for (int i = 0; i < countPerSecondS.length() - len; i++) tmp = "0" + tmp;
        System.out.println(tmp);
        identifySeed += 1L;
        String result = curTimeStr + getRadixOf62(Long.parseLong(new StringBuilder("1").append(tmp).toString()));
        System.out.println(curTimeStr);
        System.out.println(result);
        return null;
    }

    public static String getRadixOf62(long src) {
        StringBuffer sb = new StringBuffer();
        while (src > 0L) {
            int mod = (int)(src % cmb.length);
            sb.append(new String(new byte[] { cmb[mod] }));
            src /= cmb.length;
        }
        return reverse(sb.toString());
    }

    public static String reverse(String str) { int n = str.length();
        char[] chars = new char[n];
        str.getChars(0, n, chars, 0);

        int length = chars.length;
        StringBuffer sbStr = new StringBuffer();
        for (int i = 0; i < length; i++) {
            sbStr.append(chars[(length - i - 1)]);
        }
        return sbStr.toString();
    }
}
