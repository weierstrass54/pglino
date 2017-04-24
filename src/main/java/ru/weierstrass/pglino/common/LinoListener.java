package ru.weierstrass.pglino.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LinoListener<E extends Connection> {

    private static final Logger _log = LoggerFactory.getLogger(LinoListener.class);

    protected final String _name;

    private final LinoConfig _config;
    private final ScheduledExecutorService _keeper;
    private final Map<String, LinoHandler> _handlers;
    protected final Lock _lock;
    protected final ExecutorService _queue;
    protected E _connection;

    protected LinoListener(String name, LinoConfig config) {
        _name = name;
        _config = config;
        _handlers = new ConcurrentHashMap<>();
        _lock = new ReentrantLock();
        _log.debug("Initializing {} queue.", name);
        _queue = Executors.newFixedThreadPool(config.getThreadCount(),
            (r) -> new Thread(r, "Lino " + name + " queue"));
        _log.debug("Initializing {} keeper.", name);
        _keeper = Executors.newSingleThreadScheduledExecutor(
            (r) -> new Thread(r, "Lino " + name + " keeper"));
        _keeper.scheduleAtFixedRate(new KeeperTask(), 0, 15, TimeUnit.SECONDS);
    }

    public void destroy() {
        _queue.shutdownNow();
        _keeper.shutdownNow();
        _log.info("Listener {} is destroyed.", _name);
    }

    public void addHandler(LinoHandler handler) {
        _handlers.put(handler.getChannel(), handler);
    }

    public void removeHandler(String name) {
        _handlers.remove(name);
    }

    abstract protected E getConnection(String host, Properties info) throws SQLException;

    abstract protected boolean addNotificationListener(String name, LinoHandler handler);

    abstract protected boolean removeNotificationListener(String name);

    abstract protected boolean isValid();

    abstract protected void close();

    private class KeeperTask implements Runnable {

        @Override
        public void run() {
            if (isValid()) {
                _log.debug("Connection for {} is alive.", _name);
                return;
            }
            try {
                _log.debug("Reestablish connection for {}.", _name);
                _connection = getConnection(_config.getHost(), _config.getAuth());
                for (LinoHandler handler : _handlers.values()) {
                    addNotificationListener(handler.getChannel(), handler);
                }
            } catch (SQLException e) {
                close();
            }
        }
    }

}
