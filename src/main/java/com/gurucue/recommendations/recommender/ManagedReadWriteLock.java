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
package com.gurucue.recommendations.recommender;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Adds a background watcher thread to the {@link ReentrantReadWriteLock} to detect deadlocks or endless loops.
 * Periodically the lock is tried, and if it fails within a predefined interval, then the currently owning
 * thread is interrupted. This should cause a "soft" bail out, if the owning thread is properly implemented,
 * and allow other threads to obtain the lock.
 */
public class ManagedReadWriteLock extends ReentrantReadWriteLock {
    private final static Logger log = LogManager.getLogger(ManagedReadWriteLock.class);
    private final ReentrantReadWriteLockWatcher lockWatcher;

    public ManagedReadWriteLock(final String id) {
        super();
        lockWatcher = new ReentrantReadWriteLockWatcher(id);
    }

    public ManagedReadWriteLock(boolean fair, final String id) {
        super(fair);
        lockWatcher = new ReentrantReadWriteLockWatcher(id);
    }

    public void stop() throws InterruptedException {
        lockWatcher.stop();
    }

    protected class ReentrantReadWriteLockWatcher implements Runnable {
        private final Thread thread;
        private final AtomicBoolean notStopped = new AtomicBoolean(true);
        private final String logPrefix;

        ReentrantReadWriteLockWatcher(final String id) {
            this.logPrefix = "[REC " + id + "] ";
            thread = new Thread(this, "ReentrantReadWriteLockWatcher REC " + id);
            thread.setDaemon(true);
            thread.start();
        }

        @Override
        public void run() {
            final ReadLock readLock = ManagedReadWriteLock.this.readLock();
            while (notStopped.get()) {
                try {
                    // check every 30 seconds
                    Thread.sleep(30000);
                    // first try unfair lock, this is the most efficient
                    if (readLock.tryLock()) {
                        readLock.unlock();
                        continue;
                    }
                    // try to obtain the lock inside 60 seconds
                    if (readLock.tryLock(60, TimeUnit.SECONDS)) {
                        readLock.unlock();
                        continue;
                    }
                    // the lock could not be obtained, interrupt the owning thread and log its stack-trace
                    final Thread thread = ManagedReadWriteLock.this.getOwner();
                    if (null == thread) { // odd: couldn't obtain the lock, but there's nobody owning it
                        log.warn(logPrefix + "Manager thread could not obtain the lock, and there's noone owning it, ignored");
                        continue;
                    }
                    final StackTraceElement[] trace = thread.getStackTrace();
                    final StringBuilder sb = new StringBuilder();
                    sb.append(logPrefix);
                    sb.append("Thread #");
                    sb.append(thread.getId());
                    sb.append(" with name ");
                    sb.append(thread.getName());
                    sb.append(" is most probably hogging the lock (currently there are ");
                    sb.append(ManagedReadWriteLock.this.getQueueLength());
                    sb.append(" threads waiting to obtain the lock), so I'm going to interrupt it, its current stack trace follows:");
                    for (int i = 0; i < trace.length; i++) {
                        sb.append("\n");
                        sb.append(trace[i].toString());
                    }
                    log.error(sb.toString());
                    thread.interrupt();
                }
                catch (InterruptedException e) {
                    log.warn(logPrefix + "Manager thread interrupted: " + e.toString(), e);
                }
                catch (Throwable t) {
                    log.error(logPrefix + "Manager thread caught an exception: " + t.toString(), t);
                }
            }
            log.info(logPrefix + "Manager thread exiting; interrupting any owning thread");
            final Thread thread = ManagedReadWriteLock.this.getOwner();
            if (thread != null) thread.interrupt(); // to guard against any deadlocks while exiting: we're dieing anyway
        }

        void stop() throws InterruptedException {
            if (!notStopped.getAndSet(false)) return;
            thread.interrupt();
            thread.join();
        }
    }
}
