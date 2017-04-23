package ru.weierstrass.pglino.pg;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.weierstrass.pglino.common.LinoConfig;
import ru.weierstrass.pglino.common.LinoHandler;
import ru.weierstrass.pglino.common.LinoListener;

public final class PgLinoListener extends LinoListener<PGConnection> {

    private static final Logger _log = LoggerFactory.getLogger(PgLinoListener.class);

    public PgLinoListener(String name, LinoConfig config) throws SQLException {
        super(name, config);
    }

    @Override
    protected boolean addNotificationListener(String name, LinoHandler listener) {
        try (Statement query = _connection.createStatement()) {
            _log.debug("Executing LISTEN query for channel {} with name {}.", listener.getChannel(),
                name);
            query.execute("LISTEN " + listener.getChannel());
            _connection
                .addNotificationListener(name, listener.getChannel(), _createListener(listener));
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    protected boolean removeNotificationListener(String name) {
        try (Statement query = _connection.createStatement()) {
            _log.debug("Executing UNLISTEN query for {}.", name);
            query.execute("UNLISTEN " + name);
            _connection.removeNotificationListener(name);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    protected PGConnection getConnection(String host, Properties info) throws SQLException {
        _log.debug("Establish new connection for {} as {}", host, info.get("user"));
        return (PGConnection) DriverManager.getConnection("jdbc:pgsql://" + host, info);
    }

    @Override
    protected boolean isValid() {
        _log.debug("Check connection for {}", _name);
        if (_connection == null) {
            return false;
        }
        if (_lock.tryLock()) {
            try (Statement query = _connection.createStatement()) {
                query.execute("SELECT 1::CHAR");
                return true;
            } catch (SQLException e) {
                return false;
            } finally {
                _lock.unlock();
            }
        }
        return true;
    }

    @Override
    protected void close() {
        if (_connection != null) {
            try {
                _connection.close();
            } catch (SQLException e) {
                _log.warn("Connection for {} has not been closed properly.", _name);
            }

        }
    }

    private PGNotificationListener _createListener(LinoHandler handler) {
        return (processId, channelName, payload) -> {
            _lock.lock();
            try {
                _log.debug("Notification for {} with channel {} and payload {} has been received.",
                    _name, channelName, payload);
                _queue.submit(handler.process(payload));
            } finally {
                _lock.unlock();
            }
        };
    }

}
