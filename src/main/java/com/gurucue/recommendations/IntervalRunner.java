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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Periodically executes the {@link Runnable} given in {@link #start(long, Runnable)}.
 * It is implemented using a {@link Thread}, thus not suitable for a large number
 * of periodical tasks. Consider using the {@link com.gurucue.recommendations.Timer}
 * instead.
 * TODO: reimplement using Timer, move to the Database library
 */
public final class IntervalRunner implements Runnable {
    private static final Logger log = LogManager.getLogger(IntervalRunner.class);
    private static final Lock threadLock = new ReentrantLock();
    private static final Condition resetInterval = threadLock.newCondition();
    private final String threadName;
    private final String logPrefix;
    private long intervalMillis;
    private Runnable runnable;
    private Thread thread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public IntervalRunner(final String name) {
        this.logPrefix = name;
        this.threadName = "BasicRecommenderServer " + name;
        log.info(logPrefix + ": created");
    }

    public void start(final long intervalMillis, final Runnable runnable) {
        log.info(logPrefix + ": starting...");
        this.intervalMillis = intervalMillis;
        this.runnable = runnable;
        running.set(true);
        final Thread thisThread = thread = new Thread(this, threadName);
        thisThread.setDaemon(true);
        thisThread.start();
    }

    public void stop() {
        log.info(logPrefix + ": stopping...");
        running.set(false);
        final Thread thisThread = thread; // cache it in the local variable, so if anyone changes it, it doesn't mess up our algo
        thread = null;
        runnable = null;
        if (thisThread == null) {
            log.info(logPrefix + ": already stopped");
            return;
        }
        thisThread.interrupt();
        try {
            thisThread.join();
        } catch (InterruptedException e) {
            log.warn(logPrefix + ": interrupted while waiting for the thread to terminate");
        }
        log.info(logPrefix + ": stopped");
    }

    public void setIntervalMillis(final long intervalMillis) throws InterruptedException {
        threadLock.lockInterruptibly();
        try {
            this.intervalMillis = intervalMillis;
            resetInterval.signal();
        }
        finally {
            threadLock.unlock();
        }
    }

    public long getIntervalMillis() throws InterruptedException {
        threadLock.lockInterruptibly();
        try {
            return this.intervalMillis;
        }
        finally {
            threadLock.unlock();
        }
    }

    @Override
    public void run() {
        log.info(logPrefix + ": started");
        try {
            outside:
            while (running.get()) {
                final long destinationTime = System.currentTimeMillis() + intervalMillis;
                long remainingMillis = intervalMillis;
                do {
                    try {
                        threadLock.lockInterruptibly();
                        try {
                            resetInterval.await(remainingMillis, TimeUnit.MILLISECONDS);
                        }
                        finally {
                            threadLock.unlock();
                        }
                    } catch (InterruptedException e) {
                        if (!running.get()) break outside;
                        log.warn(logPrefix + ": interrupted while sleeping: " + e.toString(), e);
                    }
                }
                while ((remainingMillis = destinationTime - System.currentTimeMillis()) > 0);
                try {
                    runnable.run();
                }
                catch (Exception e) {
                    log.error(logPrefix + ": exception while executing the job: " + e.toString(), e);
                }
            }
        }
        finally {
            log.info(logPrefix + ": terminating");
        }
    }
}
