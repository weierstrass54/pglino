package ru.weierstrass.pglino;

import java.util.Properties;

public class PglinoConfig {

    private final String _host;
    private final String _user;
    private final String _password;

    public PglinoConfig(String host, String user, String password) {
        this._host = host;
        this._user = user;
        this._password = password;
    }

    public String getHost() {
        return this._host;
    }

    public Properties getAuth() {
        Properties auth = new Properties();
        auth.put("user", this._user);
        auth.put("password", this._password);
        return auth;
    }

}
