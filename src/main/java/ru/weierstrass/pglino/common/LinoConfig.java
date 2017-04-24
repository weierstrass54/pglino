package ru.weierstrass.pglino.common;

import java.util.Properties;

public class LinoConfig {

    private final String _host;
    private final String _user;
    private final String _password;

    private final int _threadCount;

    public LinoConfig(String host, String user, String password, int threadCount) {
        this._host = host;
        this._user = user;
        this._password = password;
        this._threadCount = threadCount;
    }

    public LinoConfig(String host, String user, String password) {
        this(host, user, password, 1);
    }

    String getHost() {
        return this._host;
    }

    Properties getAuth() {
        Properties auth = new Properties();
        auth.put("user", this._user);
        auth.put("password", this._password);
        return auth;
    }

    int getThreadCount() {
        return this._threadCount;
    }

}
