package com.ef;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

class ConfigProperties{
    private Properties prop = new Properties();
    private InputStream input;

    ConfigProperties() {
        try {
            input = new FileInputStream("/home/hasitha/IdeaProjects/parser/config/config.properties");
            this.prop.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Properties getMySqlDbConfig() {
        if (prop == null){
            prop = new Properties();
            try {
                input = new FileInputStream("/home/hasitha/IdeaProjects/parser/config/config.properties");
                prop.load(input);
            } catch (IOException ex) {
                ex.printStackTrace();
            } finally {
                if (input != null) {
                    try {
                        input.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            return prop;
        }
        return prop;
    }
}