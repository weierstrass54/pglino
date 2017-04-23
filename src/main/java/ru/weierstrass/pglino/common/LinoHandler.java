package ru.weierstrass.pglino.common;

public interface LinoHandler {

    String getChannel();

    Runnable process(String notify);

}
