package ru.weierstrass.pglino;

import com.impossibl.postgres.api.jdbc.PGConnection;
import com.impossibl.postgres.api.jdbc.PGNotificationListener;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Pglino {

    private final Lock _lock;

    //TODO: driver is required
    private final Set<NotifyHandler> _handlers;
    private final ScheduledExecutorService _keeper;
    private final ExecutorService _queue;
    private volatile boolean _isStarted;
    private PGConnection _connection;

    public Pglino(String name, PglinoConfig conf) {
        _isStarted = false;
        _lock = new ReentrantLock();
        _queue = Executors
            .newSingleThreadExecutor((r) -> new Thread(r, "Pglino " + name + " queue"));
        _keeper = Executors
            .newSingleThreadScheduledExecutor((r) -> new Thread(r, "Pglino " + name + " keeper"));
        _handlers = new HashSet<>();
    }

    public Pglino(PglinoConfig conf) {
        this("default", conf);
    }

    public boolean attach(NotifyHandler handler) {
        boolean result = _handlers.add(handler);
        if (_isStarted) {
            return _addNotifyListener(handler);
        }
        return result;
    }

    public void start() {
        if (_isStarted) {
            return;
        }
        _keeper.scheduleAtFixedRate(new KeeperTask(), 0, 15, TimeUnit.SECONDS);
        _isStarted = true;
    }

    private boolean _addNotifyListener(NotifyHandler handler) {
        Statement query = null;
        PGNotificationListener listener = new PglinoListener(handler);
        _lock.lock();
        try {
            //TODO: weak reference is here! Keep an eye on this.
            _connection.addNotificationListener(handler.getChannel(), ".*", listener);
            query = _connection.createStatement();
            query.execute("LISTEN " + handler.getChannel());
        } catch (SQLException e) {
            _connection.removeNotificationListener(handler.getChannel());
            return false;
        } finally {
            _close(query);
            _lock.unlock();
        }
        return true;
    }

    private boolean _isConnectionValid() {
        Statement query = null;
        if (_connection == null) {
            return false;
        }
        if (_lock.tryLock()) {
            try {
                query = _connection.createStatement();
                query.execute("SELECT 1::CHAR");
                return true;
            } catch (SQLException e) {
                return false;
            } finally {
                _close(query);
                _lock.unlock();
            }
        }
        return true;
    }

    private void _close(Statement st) {
        try {
            st.close();
        } catch (SQLException e) {
            /* ignore for a while */
        }
    }

    private void _closeConnection() {
        try {
            _connection.close();
        } catch (SQLException e) {
            /* ignore for a while */
        }
    }

    private class KeeperTask implements Runnable {

        @Override
        public void run() {
            if (_isConnectionValid()) {
                return;
            }
            try {
                //new connection here
                _connection.setNetworkTimeout(null, 1000);
                for (NotifyHandler handler : _handlers) {
                    _addNotifyListener(handler);
                }
            } catch (SQLException e) {
                _closeConnection();
            }
        }

    }

    private class PglinoListener implements PGNotificationListener {

        private final NotifyHandler _handler;

        private PglinoListener(NotifyHandler handler) {
            this._handler = handler;
        }

        @Override
        public void notification(int processId, String channelName, String payload) {
            _lock.lock();
            try {
                _queue.submit(this._handler.process(payload));
            } finally {
                _lock.unlock();
            }
        }

    }

}
