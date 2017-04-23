package ru.weierstrass.pglino;

import java.sql.SQLException;
import ru.weierstrass.pglino.common.LinoConfig;
import ru.weierstrass.pglino.common.LinoHandler;
import ru.weierstrass.pglino.pg.PgLinoListener;

public class Application {

    //An example

    public static void main(String[] args) throws SQLException {
        LinoConfig config = new LinoConfig("host", "user", "password");
        PgLinoListener listener = new PgLinoListener("test", config);
        listener.addHandler(new LinoHandler() {
            @Override
            public String getChannel() {
                return "test";
            }

            @Override
            public Runnable process(String notify) {
                return () -> {
                    //do smth
                };
            }
        });
    }

}
