
package com.github.wuzguo.es;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.PreDestroy;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * TODO ES客户端连接，单列模式
 *
 * @author wuzguo at 2017/6/14 17:47
 */

public class ClientUtils {

    private static final Logger LOG = LogManager.getLogger(ClientUtils.class);

    // 创建私有对象
    private static TransportClient sourceClient;

    static {
        LOG.info("init es connection start.");
        try {
            Class<?> clazz = Class.forName(PreBuiltTransportClient.class.getName());
            Constructor<?> constructor = clazz.getDeclaredConstructor(Settings.class);
            constructor.setAccessible(true);
            Settings settings = Settings.builder()
                    .put("cluster.name", "es-hl-test").build();

            sourceClient = (TransportClient) constructor.newInstance(settings);
            sourceClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9301))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9302));

        } catch (InstantiationException e) {
            LOG.error("init es connection error: InstantiationException, " + e.getMessage());
        } catch (InvocationTargetException e) {
            LOG.error("init es connection error: InvocationTargetException, " + e.getMessage());
        } catch (NoSuchMethodException e) {
            LOG.error("init es connection error: NoSuchMethodException, " + e.getMessage());
        } catch (IllegalAccessException e) {
            LOG.error("init es connection error: IllegalAccessException, " + e.getMessage());
        } catch (UnknownHostException e) {
            LOG.error("init es connection error: UnknownHostException, " + e.getMessage());
        } catch (ClassNotFoundException e) {
            LOG.error("init es connection error: ClassNotFoundException, " + e.getMessage());
        }
        LOG.info("init es connection end.");
    }

    private ClientUtils() {
    }

    // 取得源实例
    public static synchronized Client connect() {
        return sourceClient;
    }


    @PreDestroy
    public void close() {
        System.out.println(" es clint connection close.");
        if (sourceClient != null) {
            sourceClient.close();
        }
    }

    @Override
    public String toString() {
        return "ElasticSearchClient{" +
                "client=" + sourceClient +
                '}';
    }
}
