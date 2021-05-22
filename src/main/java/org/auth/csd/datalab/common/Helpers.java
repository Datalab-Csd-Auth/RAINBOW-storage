package org.auth.csd.datalab.common;

public class Helpers {

    /**
     * Method to read environment variables
     *
     * @param key The key of the variable
     * @return The variable
     */
    public static String readEnvVariable(String key) {
        if (System.getenv().containsKey(key)) {
            return System.getenv(key);
        } else return null;
    }

}
