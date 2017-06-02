/*
 * This file is part of Guru Cue Search & Recommendation Engine.
 * Copyright (C) 2017 Guru Cue Ltd.
 *
 * Guru Cue Search & Recommendation Engine is free software: you can
 * redistribute it and/or modify it under the terms of the GNU General
 * Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * Guru Cue Search & Recommendation Engine is distributed in the hope
 * that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Guru Cue Search & Recommendation Engine. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package com.gurucue.recommendations;

import com.gurucue.recommendations.data.DataManager;
import com.gurucue.recommendations.data.DataProvider;
import com.gurucue.recommendations.data.postgresql.PostgreSqlDataProvider;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * The driver class for the recommender so that it can be daemonized using the
 * Apache Commons Daemon. It configures, starts, and binds a recommender to a RMI name.
 */
public final class RmiServer implements Daemon {
    private static final Logger log = LogManager.getLogger(RmiServer.class);
    public long recommenderId;
    public Registry registry;
    public BasicRecommenderImpl recommenderImpl;
    public String remoteName;
    private final IntervalRunner rmiCheckRunner = new IntervalRunner("RMI connection checker");

    public static void main(final String[] args) throws Exception {
        final RmiServer instance = new RmiServer();
        instance.init(args);
        instance.start();
    }

    private static void terminate(final String reason, final Exception e, final int exitStatus) {
        try {
            log.error(reason, e);
            Thread.sleep(2000L);
        }
        catch (Throwable ie) {
            ie.printStackTrace();
        }
        throw new IllegalArgumentException(reason, e);
    }

    private void init(final String[] args) {
        if (args.length != 4) {
            terminate("Cannot create a recommender: need 4 arguments: <JDBC string> <username> <password> <AI ID>", null, 1);
        }

        try {
            recommenderId = Long.parseLong(args[3], 10);
        }
        catch (NumberFormatException e) {
            terminate("Invalid AI ID: " + e.toString(), e, 2);
        }

        remoteName = "AI" + recommenderId;

        final DataProvider provider;
        try {
            provider = PostgreSqlDataProvider.create(args[0], args[1], args[2]);
            DataManager.setProvider(provider);
        }
        catch (Exception e) {
            terminate("Failed to connect to the database: " + e.toString(), e, 3);
        }

        log.info("Recommender " + recommenderId + " initialized");
    }

    @Override
    public void init(final DaemonContext daemonContext) throws DaemonInitException, Exception {
        init(daemonContext.getArguments());
    }

    @Override
    public void start() throws Exception {
        try {
            // the server instance can only be bound to the localhost
            registry = LocateRegistry.getRegistry("127.0.0.1");
        }
        catch (Exception e) {
            terminate("Failed to obtain a registry: " + e.toString(), e, 4);
        }

        try {
            recommenderImpl = new BasicRecommenderImpl(recommenderId);
            registry.rebind(remoteName, recommenderImpl);
        }
        catch (Exception e) {
            terminate("Failed to create the recommender: " + e.toString(), e, 10);
        }

        log.info("Recommender " + recommenderId + " successfully started and bound to " + remoteName);

        rmiCheckRunner.start(1000L, new RmiChecker(this));
    }

    @Override
    public void stop() throws Exception {
        log.info("Stopping recommender " + recommenderId + " and unbinding it from " + remoteName);
        rmiCheckRunner.stop();
        registry.unbind(remoteName);
        registry = null;
        recommenderImpl.stop();
        recommenderImpl = null;
        log.info("Recommender " + recommenderId + " stopped");
    }

    @Override
    public void destroy() {
        log.info("Recommender " + recommenderId + " destroyed");
    }

    private static class RmiChecker implements Runnable {
        final RmiServer server;

        RmiChecker(final RmiServer server) {
            this.server = server;
        }

        @Override
        public void run() {
            final String remoteName = server.remoteName;
            final BasicRecommenderImpl recommenderImpl = server.recommenderImpl;
            Registry registry = server.registry;
            if ((registry == null) || (recommenderImpl == null)) return; // the server isn't running

            Remote something = null;
            try {
                something = registry.lookup(remoteName);
            }
            catch (NotBoundException e) {
                log.error("Failed to check my presence on RMI server (not bound): " + e.toString(), e);
            }
            catch (RemoteException e) {
                log.error("Failed to check my presence on RMI server (remote exception): " + e.toString(), e);
            }

            if (something == null) {
                try {
                    // the server instance can only be bound to the localhost
                    registry = LocateRegistry.getRegistry("127.0.0.1");
                    registry.rebind(remoteName, recommenderImpl);
                    server.registry = registry;
                } catch (Throwable e) {
                    log.error("Failed to obtain a RMI registry and rebind the recommender: " + e.toString(), e);
                }
            }
        }
    }
}
