package com.netcentric.weather.common;

/** Reads configuration from environment variables (12-factor style, no hardcoded secrets). */
public final class Config {

    private Config() {
    }

    public static String get(String key, String defaultValue) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            v = System.getProperty(key);
        }
        return (v == null || v.isBlank()) ? defaultValue : v.trim();
    }

    public static String require(String key) {
        String v = get(key, null);
        if (v == null) {
            throw new IllegalStateException("Missing required environment variable: " + key);
        }
        return v;
    }

    public static int getInt(String key, int defaultValue) {
        return Integer.parseInt(get(key, String.valueOf(defaultValue)));
    }

    public static long getLong(String key, long defaultValue) {
        return Long.parseLong(get(key, String.valueOf(defaultValue)));
    }

    public static double getDouble(String key, double defaultValue) {
        return Double.parseDouble(get(key, String.valueOf(defaultValue)));
    }
}
