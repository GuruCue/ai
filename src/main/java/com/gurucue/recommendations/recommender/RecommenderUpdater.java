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

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The job runner thread. Executes jobs in sequence for
 * the parent <code>MasterRecommender</code> instance.
 */
class RecommenderUpdater implements Runnable {
    private static final Logger logger = LogManager.getLogger(RecommenderUpdater.class.getName());
    private final MasterRecommender recommender;
    private final String logPrefix;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition recomputeSignal = lock.newCondition();
    private final Condition idleSignal = lock.newCondition();
    private boolean isIdle = false;
    private final AtomicBoolean notStopped = new AtomicBoolean(true);
    private Thread thread = null;
    private final LinkedList<RecommenderUpdaterJob> jobList = new LinkedList<RecommenderUpdaterJob>();

    RecommenderUpdater(final MasterRecommender recommender, final String logPrefix) {
        this.recommender = recommender;
        this.logPrefix = logPrefix;
    }

    /**
     * The main computation thread loop. Never call this directly.
     */
    @Override
    public void run() {
        logger.info(logPrefix + "Updater thread started");
        final Runtime runtime = Runtime.getRuntime();
        while (notStopped.get()) {
            try {
                RecommenderUpdaterJob job; // the job that will get executed
                int jobsRemaining;
                // step 1: wait for a job
                lock.lockInterruptibly();
                try {
                    while ((jobsRemaining = jobList.size()) == 0) { // while there is no job waiting
                        logger.info(logPrefix + "Waiting for a recompute signal");
                        isIdle = true;
                        idleSignal.signalAll(); // signal anyone waiting for our idleness that we're idle
                        recomputeSignal.await(); // and wait for a job
                    }
                    job = jobList.remove();
                    jobsRemaining--;
                }
                finally {
                    lock.unlock();
                }
                // step 2: execute the job
                logger.info(logPrefix + "Starting update " + job.getClass().getSimpleName() + ", number of remaining jobs: " + jobsRemaining);
                long startTime = System.currentTimeMillis();
                try {
                    job.work();
                }
                finally {
                    job.signalFinished(); // any waiting clients must be signaled no matter what the result is
                }
                long endTime = System.currentTimeMillis();
                logger.info(String.format("%sUpdate finished in %d seconds, free: %.3f MB, total: %.3f MB, max: %.3f MB", logPrefix, (endTime - startTime) / 1000, runtime.freeMemory() / 1048576.0, runtime.totalMemory() / 1048576.0, runtime.maxMemory() / 1048576.0));
            }
            catch (InterruptedException e) {
                logger.warn(logPrefix + "Caught an InterruptedException", e);
            }
            catch (Throwable e) {
                logger.error(logPrefix + "Caught an exception: " + e.toString(), e);
            }
        }
        logger.info(logPrefix + "Updater thread stopped");
    }

    /**
     * Schedules an (asynchronous) execution of the provided job, if it's not
     * already in the queue, and returns either the job already queued if it's
     * the same as the given job, or the job given if not found in the queue.
     * The job will be put into the job queue of the job runner thread, and
     * executed within that thread.
     *
     * @param job the job to execute
     * @return the job that will actually get executed
     * @throws InterruptedException if the calling thread gets interrupted while waiting trying to acquire the lock
     */
    public RecommenderUpdaterJob update(final RecommenderUpdaterJob job) throws InterruptedException {
        logger.info(logPrefix + "Scheduling update");
        lock.lockInterruptibly();
        try {
            int existingJobIndex = jobList.indexOf(job);
            if (existingJobIndex >= 0) {
                logger.info(logPrefix + "Update already scheduled");
                return jobList.get(existingJobIndex);
            }
            jobList.add(job);
            job.markUnfinished();
            isIdle = false;
            recomputeSignal.signal();
            return job;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Start the model computation thread.
     */
    public void start() {
        if (null != thread) throw new IllegalStateException("The thread is already running");
        thread = new Thread(this, logPrefix + "MasterRecommender Updater");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Stops the model computation thread if it is running, otherwise
     * does nothing.
     *
     * @throws InterruptedException if the calling thread was interrupted while waiting for the computation thread to finish
     */
    public void stop() throws InterruptedException {
        if (null == thread) return;
        notStopped.set(false);
        thread.interrupt();
        thread.join();
        thread = null;
    }

    /**
     * Waits for the computation thread to become idle. This means that
     * the jobs queue is empty. Handy mainly at startup when we need to
     * wait until the first model is ready.
     *
     * @throws InterruptedException if the calling thread is interrupted while waiting to obtain the lock
     */
    public void waitForIdle() throws InterruptedException {
        logger.info(logPrefix + "Waiting for updater to get idle");
        lock.lockInterruptibly();
        try {
            while (!isIdle) {
                idleSignal.await();
            }
        }
        finally {
            lock.unlock();
        }
    }
}
