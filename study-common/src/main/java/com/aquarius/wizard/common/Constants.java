package com.aquarius.wizard.common;

/**
 * @author zhaoyijie
 * @since 2022/3/12 13:23
 */
public class Constants {

    private Constants() {
        throw new IllegalStateException("Construct Constants");
    }

    /**
     * SINGLE_QUOTES "'"
     */
    public static final String SINGLE_QUOTES = "'";

    /**
     * double brackets left
     */
    public static final String DOUBLE_BRACKETS_LEFT_SPACE = "{ {";

    /**
     * double brackets left
     */
    public static final String DOUBLE_BRACKETS_LEFT = "{{";

    /**
     * double brackets left
     */
    public static final String DOUBLE_BRACKETS_RIGHT = "}}";

    /**
     * double brackets left
     */
    public static final String DOUBLE_BRACKETS_RIGHT_SPACE = "} }";

    /**
     *
     */
    public static final String NULL = "NULL";

    /**
     *
     */
    public static final String EMPTY = "";

    /**
     *
     */
    public static final String DOTS = ".";

    /**
     * comma ,
     */
    public static final String COMMA = ",";

    /**
     * COLON :
     */
    public static final String COLON = ":";

    /**
     * QUESTION ?
     */
    public static final String QUESTION = "?";

    /**
     * SPACE " "
     */
    public static final String SPACE = " ";

    /**
     * SINGLE_SLASH /
     */
    public static final String SINGLE_SLASH = "/";

    /**
     * DOUBLE_SLASH //
     */
    public static final String DOUBLE_SLASH = "//";

    /**
     * EQUAL SIGN
     */
    public static final String EQUAL_SIGN = "=";

    /**
     * AT SIGN
     */
    public static final String AT_SIGN = "@";

    /**
     * date format of yyyy-MM-dd HH:mm:ss
     */
    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    /**
     * date format of yyyyMMdd
     */
    public static final String YYYYMMDD = "yyyyMMdd";

    /**
     * date format of yyyyMMddHHmmss
     */
    public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";

    /**
     * date format of yyyyMMddHHmmssSSS
     */
    public static final String YYYYMMDDHHMMSSSSS = "yyyyMMddHHmmssSSS";

    /**
     * jar
     */
    public static final String JAR = "jar";

    /**
     * underline  "_"
     */
    public static final String UNDERLINE = "_";

    /**
     * application regex
     */
    public static final String APPLICATION_REGEX = "application_\\d+_\\d+";

    /**
     * exec shell scripts
     */
    public static final String SH = "sh";

    /**
     * timestamp
     */
    public static final String TIMESTAMP = "timestamp";
    public static final char SUBTRACT_CHAR = '-';
    public static final char ADD_CHAR = '+';
    public static final char MULTIPLY_CHAR = '*';
    public static final char DIVISION_CHAR = '/';
    public static final char LEFT_BRACE_CHAR = '(';
    public static final char RIGHT_BRACE_CHAR = ')';
    public static final String ADD_STRING = "+";
    public static final String STAR = "*";
    public static final String DIVISION_STRING = "/";
    public static final String LEFT_BRACE_STRING = "(";

    public static final String AUTHORIZATION = "Authorization";

    public static final String JSESSIONID = "JSESSIONID";

    public static final String USER_CACHE_PREFIX_KEY = "user:";

    /**
     * 登录验证码在cache中的key
     */
    public final static String Login_VCODE_CACHE_KEY = "loginVerifyCode";

    /**
     * 图片文字验证码在cache中的key
     */
    public final static String VCODE_CACHE_KEY_PREFIX = "verifyCode:";

    public final static String UA = "User-Agent";

    public final static String X_USER_IP = "X-USER-IP";
}
