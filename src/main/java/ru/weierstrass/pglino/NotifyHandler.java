package ru.weierstrass.pglino;

public interface NotifyHandler {

    String getChannel();

    Runnable process(String notify);

}
