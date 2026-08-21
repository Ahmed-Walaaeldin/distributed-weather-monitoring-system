package com.netcentric.weather.station;

import com.netcentric.weather.common.Config;

/**
 * Resolves the station id.
 *
 * Priority:
 *  1. STATION_ID environment variable (docker / docker-compose / cloud VM).
 *  2. The ordinal of the Kubernetes StatefulSet pod name, e.g. "weather-station-3" -> 4.
 *  3. Fallback: 1.
 *
 * Using a StatefulSet is what gives every pod a stable, unique identity, so the 10
 * pods automatically become stations 1..10 without 10 separate manifests.
 */
public final class StationIdentity {

    private StationIdentity() {
    }

    public static long resolve() {
        String explicit = Config.get("STATION_ID", null);
        if (explicit != null) {
            return Long.parseLong(explicit);
        }

        String hostname = Config.get("HOSTNAME", "");
        int dash = hostname.lastIndexOf('-');
        if (dash > -1 && dash < hostname.length() - 1) {
            String suffix = hostname.substring(dash + 1);
            if (suffix.chars().allMatch(Character::isDigit)) {
                return Long.parseLong(suffix) + 1;   // ordinal 0 -> station 1
            }
        }
        return 1L;
    }
}
