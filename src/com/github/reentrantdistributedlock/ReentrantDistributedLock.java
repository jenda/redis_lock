package com.github.reentrantdistributedlock;

import java.time.Clock;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

/**
 * Simple implementation of reentrant distributed lock using Redis.
 * 
 * The implementation implements {@link Lock} so it can be used instead of
 * regular java locks.
 * 
 * The current implementation is guarded by expiring keys so it cannot
 * deadlock forever in case of a disappearing lock holder.
 * 
 * If tje lock is to be held for a longer period of time, it's important to keep
 * refreshing it using {@link #tryLock()} (or some other locking method) so the
 * key doesn't expire.
 * 
 * Note: Unfortunately the condition mechanism is still missing.
 * 
 * @author Jan Spidlen
 */
public class ReentrantDistributedLock implements Lock {

	public static long DEFAULT_LOCK_ACQUIRE_LAMBDA = TimeUnit.MILLISECONDS.toMillis(40);
	public static long DEFAULT_LOCK_EXPIRATION = TimeUnit.MINUTES.toMillis(3);
	public static long MAX_BLOCKING_ACQUIRE_TIMEOUT = TimeUnit.MINUTES.toMillis(30);

	protected final String instanceValue;
	protected final String lockName;
	protected final long lockExpirationMillis;
	protected final long lockAcquireLambdaMillis;
	protected static final String LOCK_PREFIX = "ReentrantDistributedLock/";

	// Redis constant.
	protected static long FAIL = 0;
	// Redis constant.
	protected static long OK = 1;

	protected Jedis jedis;
	private Clock clock;

	/**
	 * Simple public ctor. Mostly you want to use this one.
	 * 
	 * @param lockName
	 *            the lock is identified by its name. Use this as unique
	 *            identifier.
	 * @param jedis
	 *            the jedis client that's used to query redis
	 * @param clock
	 *            the clock used for timeouts and retries
	 */
	public ReentrantDistributedLock(String lockName, Jedis jedis, Clock clock) {
		this(lockName, jedis, clock, DEFAULT_LOCK_EXPIRATION, DEFAULT_LOCK_ACQUIRE_LAMBDA);
	}

	/**
	 * Public ctor with more goodies.
	 * 
	 * @param lockName
	 *            the lock is identified by its name. Use this as unique
	 *            identifier.
	 * @param jedis
	 *            the jedis client that's used to query redis
	 * @param clock
	 *            the clock used for timeouts and retries
	 * @param lockExpirationMillis
	 *            this value autoexpires the lock. This is needed as prevention
	 *            when lock holder dies (as it happens often in the cloud).
	 * @param lockAcquireLambdaMillis
	 *            this value with a poorly chosen name is used as a sleep
	 *            constant between the lock acquisition attempts when two or
	 *            more processes are competing for the resource. Anything in the
	 *            order of millis would do - if your Redis queried too often you
	 *            can increase this value.
	 */
	public ReentrantDistributedLock(String lockName, Jedis jedis, Clock clock, long lockExpirationMillis,
			long lockAcquireLambdaMillis) {
		this.instanceValue = UUID.randomUUID().toString();
		this.lockName = LOCK_PREFIX + lockName;
		this.lockExpirationMillis = lockExpirationMillis;
		this.lockAcquireLambdaMillis = lockAcquireLambdaMillis;
		this.jedis = jedis;
		this.clock = clock;
	}

	/**
	 * Checks whether the lock is currently being held.
	 * 
	 * @param jedis
	 *            the Redis client that is used to check
	 * @param lockName
	 *            the lock
	 * @return {@code true} if lock is held, {@code false} otherwise.
	 */
	public static boolean isLockHeldBySomeone(Jedis jedis, String lockName) {
		return jedis.get(LOCK_PREFIX + lockName) != null;
	}

	@Override
	public void lock() {
		try {
			this.lockInterruptibly();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void lockInterruptibly() throws InterruptedException {
		// MAX_BLOCKING_ACQUIRE_TIMEOUT is here to make sure that we eventually
		// stop trying.
		final long lockAcquireDeadlineInMillis = clock.millis() + MAX_BLOCKING_ACQUIRE_TIMEOUT;
		while (!this.tryLock()) {
			Thread.sleep(this.lockAcquireLambdaMillis);
			if (lockAcquireDeadlineInMillis < clock.millis()) {
				throw new RuntimeException("Lock '" + this.lockName + "' was not acquired within default MAX timeout: "
						+ MAX_BLOCKING_ACQUIRE_TIMEOUT);
			}
		}
	}

	/**
	 * Tries to obtain the lock. Returns immediately {@code true} if lock was
	 * obtained, {@code false} otherwise.
	 */
	@Override
	public boolean tryLock() {
		int counter = 0;
		while (true) {
			if (counter++ > 1000) {
				// Arbitrarily putting in some limit on acquire loop to avoid
				// some unlikely live locks.
				throw new RuntimeException("Something went wrong.");
			}
			jedis.watch(this.lockName);
			String existingLockValue = jedis.get(this.lockName);

			if (existingLockValue != null) {
				if (!this.instanceValue.equals(existingLockValue)) {
					// Someone else has the lock.
					return false;
				} else {
					// This object already owns the lock.
					Transaction transaction = jedis.multi();
					// Set the lock just in case it just expired (we cannot use
					// 'setnx' here as
					// the value is in fact set).
					Response<String> setResponse = transaction.set(this.lockName, this.instanceValue);
					Response<Long> pexpireResponse = transaction.pexpire(this.lockName, this.lockExpirationMillis);
					List<Object> result = transaction.exec();
					// Returned {@code null} means that someone manipulated with
					// the watched value between
					// {@code watch} and {#code exec} calls. Let's try again.
					if (result == null || result.isEmpty()) {
						continue;
					}

					return "OK".equals(setResponse.get()) && pexpireResponse.get() == OK;
				}
			} else {
				// At this point the lock was unlocked. So we try to lock it
				// using {@code setnx}.
				// Transaction is needed because of {@code setnx} does not
				// support expire and we need
				// to set either both or neither.
				Transaction transaction = jedis.multi();
				Response<Long> setnxResponse = transaction.setnx(this.lockName, this.instanceValue);
				Response<Long> pexpireResponse = transaction.pexpire(this.lockName, this.lockExpirationMillis);
				List<Object> result = transaction.exec();
				if (result == null || result.isEmpty()) {
					// Returned {@code null} means that someone manipulated with
					// the watched value between {@code watch} and {#code exec}
					// calls. Let's try again.
					continue;
				}
				// We were able to obtain lock if setnx returned '1'.
				return setnxResponse.get() == OK && pexpireResponse.get() == OK;
			}
		}
	}

	/**
	 * Same as {@link #tryLock()} but keeps trying for the given period.
	 */
	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		long timeoutInMillis = TimeUnit.MILLISECONDS.convert(time, unit);

		long deadlineInMillis = clock.millis() + timeoutInMillis;
		boolean lockAcquired = false;
		while (this.clock.millis() < deadlineInMillis && !(lockAcquired = this.tryLock())) {
			Thread.sleep(this.lockAcquireLambdaMillis);
		}
		return lockAcquired;
	}

	/**
	 * Same as {@link #tryUnlock()} but without return statement.
	 */
	@Override
	public void unlock() {
		tryUnlock();
	}

	/**
	 * Tries to unlock the given lock. If the lock is owned by this lock, it
	 * will be unlocked otherwise nothing happens.
	 * 
	 * @return {@code true} if the lock was owned and unlocked, {@code false} if
	 *         the lock was not owned.
	 */
	public boolean tryUnlock() {
		int counter = 0;
		boolean lockReleased = false;

		while (true) {
			// Arbitrarily putting in some limit on release loop to avoid some
			// unlikely live locks we cannot foresee.
			if (counter++ > 1000) {
				throw new RuntimeException("This should never execute. Cannot release lock: " + this.lockName);
			}
			jedis.watch(this.lockName);
			String lockValue = jedis.get(this.lockName);
			if (this.instanceValue.equals(lockValue)) {
				Transaction transaction = jedis.multi();
				transaction.del(this.lockName);
				List<Object> result = transaction.exec();
				if (result == null || result.isEmpty()) {
					// Returned {@code null} means that someone manipulated with
					// the watched value between {@code watch} and {#code exec}
					// calls. Let's try again.
					continue;
				}
				// The lock was owned and released by this object.
				lockReleased = true;
				break;
			} else {
				// The lock is not owned by this object so we cannot release it.
				lockReleased = false;
				break;
			}
		}
		return lockReleased;
	}

	@Override
	public Condition newCondition() {
		throw new UnsupportedOperationException("TODO: Implement me.");
	}
}
