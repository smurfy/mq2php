package com.happyr.mq2php;

import com.happyr.mq2php.executors.ExecutorInterface;
import com.happyr.mq2php.executors.FastCgiExecutor;
import com.happyr.mq2php.executors.ShellExecutor;
import com.happyr.mq2php.queue.QueueInterface;
import com.happyr.mq2php.queue.RabbitMq;

import java.util.Vector;

/**
 * @author Tobias Nyholm
 */
public class Application {
    public static void main(String[] args) {
        int nbThreads = 5;
        if (args.length > 0) {
            nbThreads = Integer.parseInt(args[0]);
        }

        Vector<QueueInterface> queues = getQueues();
        ExecutorInterface client = getExecutor();
        int queueLength = queues.size();

        // Make sure we have at least as many threads as queues.
        if (nbThreads < queueLength) {
            nbThreads = queueLength;
        }

        for (int i = 0; i < nbThreads; i++) {
            new Worker(queues.get(i % queueLength), client).start();
        }
    }

    /**
     *
     * Get the queue names from comma separated string
     *
     * @return Vector<QueueInterface>
     */
    private static Vector<QueueInterface> getQueues() {
        // read queue name from ENV vars
        String queueNamesArg = System.getenv("SF_RABBITMQ_QUEUE_NAMES");
        // direct argument on command line can overwrite
        if (System.getProperty("queueNames") != null) {
            queueNamesArg = System.getProperty("queueNames");
        }
        if (queueNamesArg == null) {
            queueNamesArg = "sf_deferred_events";
        }

        String[] queueNames = queueNamesArg.split(",");

        Vector<QueueInterface> queues = new Vector<QueueInterface>();
        for (String name:queueNames) {
            queues.add(getQueue(name));
        }
        return queues;
    }

    /**
     * Get a queue object from the system properties and env vars
     *
     * Environment can be overwritten with properties, fallback is hard coded as follows
     *
     * property              | env                     | default              |
     * ----------------------|-------------------------|----------------------|
     * messageQueue          | `nil`                   | rabbitmq             |
     * messageQueueHost      | SF_RABBITMQ_HOST        | `localhost`          |
     * messageQueuePort      | SF_RABBITMQ_PORT        | `5672`               |
     * messageQueueUser      | SF_RABBITMQ_USER        | `guest`              |
     * messageQueuePassword  | SF_RABBITMQ_PASSWORD    | `guest`              |
     * messageQueueVhost     | SF_RABBITMQ_VHOST       | `/`                  |
     * queueNames            | SF_RABBITMQ_QUEUE_NAMES | `sf_deferred_events` |
     *
     * @return QueueInterface
     */
    private static QueueInterface getQueue(String queueName) {
        String param = System.getProperty("messageQueue");
        if (param == null) {
            //default
            param = "rabbitmq";
        }

        // read host settings from ENV vars
        String host = System.getenv("SF_RABBITMQ_HOST");
        // direct argument on command line can overwrite
        if (System.getProperty("messageQueueHost") != null) {
            host = System.getProperty("messageQueueHost");
        }
        //default
        if (host == null) {
            host = "localhost";
        }

        // read port settings from ENV vars
        String portString = System.getenv("SF_RABBITMQ_PORT");
        if (System.getProperty("messageQueuePort") != null) {
            portString = System.getProperty("messageQueuePort");
        }
        Integer port;
        try {
            port = Integer.parseInt(portString);
        }
        catch (NumberFormatException e) {
            port = null;
        }

        if (port == null) {
            //default for rabbitmq
            port = 5672;
        }

        // read user settings from ENV vars
        String user = System.getenv("SF_RABBITMQ_USER");
        // direct argument on command line can overwrite
        if (System.getProperty("messageQueueUser") != null) {
            user = System.getProperty("messageQueueUser");
        }
        //default
        if (user == null) {
            user = "guest";
        }

        // read user password settings from ENV vars
        String pass = System.getenv("SF_RABBITMQ_PASSWORD");
        // direct argument on command line can overwrite
        if (System.getProperty("messageQueuePassword") != null) {
            pass = System.getProperty("messageQueuePassword");
        }
        //default
        if (pass == null) {
            pass = "guest";
        }

        // read user vhost settings from ENV vars
        String vhost = System.getenv("SF_RABBITMQ_VHOST");
        // direct argument on command line can overwrite
        if (System.getProperty("messageQueueVhost") != null) {
            vhost = System.getProperty("messageQueueVhost");
        }
        //default
        if (vhost == null) {
            vhost = "/";
        }

        if (param.equalsIgnoreCase("rabbitmq")) {
            if (port == null) {
                //default for rabbitmq
                port = 5672;
            }

            return new RabbitMq(host, port, user, pass, vhost, queueName);
        }

        throw new IllegalArgumentException("Could not find QueueInterface implementation named " + param);
    }

    /**
     * Get a executor object from the system properties
     *
     * @return ExecutorInterface
     */
    private static ExecutorInterface getExecutor() {
        String param = System.getProperty("executor");
        if (param == null) {
            //default
            param = "shell";
        }

        if (param.equalsIgnoreCase("fastcgi")) {
            return new FastCgiExecutor();
        }

        if (param.equalsIgnoreCase("shell")) {
            return new ShellExecutor();
        }

        throw new IllegalArgumentException("Could not find ExecutorInterface implementation named " + param);
    }
}
