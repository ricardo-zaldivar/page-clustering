package edu.usc.irds.autoext;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * The configuration framework.
 */
public class Config {

    public static final String CONFIG_FILE = "autoext.properties";
    public static final Properties DEF_PROPS = new Properties();
    public static final Config INSTANCE;

    static {
        try(InputStream stream = Config.class.getClassLoader().getResourceAsStream(CONFIG_FILE)){
            DEF_PROPS.load(stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        INSTANCE = new Config(DEF_PROPS);
    }

    public static Config getInstance(){
        return INSTANCE;
    }

    private String tedImpl;
    private double simWeight;

    public Config(){
        this(DEF_PROPS);
    }

    public Config(Properties props) {
        this.tedImpl = props.getProperty("ted.impl").trim();
        this.simWeight = Double.parseDouble(props.getProperty("sim.weight").trim());
    }

    public String getTedImpl() {
        return tedImpl;
    }

    public double getSimWeight() {
        return simWeight;
    }

}

