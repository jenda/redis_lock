package com.github.reentrantdistributedlock.tests;

import static org.junit.Assert.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;

import com.github.reentrantdistributedlock.ReentrantDistributedLock;

import redis.clients.jedis.Jedis;

public class ReentrantDistributedLockTest {

	// sign, there is no good mock; "50onRed/mock-jedis" does not support
	// transactions :-/
	final Jedis jedis = new Jedis("localhost");
	final String TEST_LOCK_NAME = "test";

	@Before
	public void setUp() {
		jedis.flushAll();
	}

	@Test
	public void test_simple_happy_path() {
		ReentrantDistributedLock lock = getLockWithDefaults(TEST_LOCK_NAME);
		assertTrue(lock.tryLock());
		assertTrue(lock.tryUnlock());
	}

	@Test
	public void test_two_locks() {
		ReentrantDistributedLock lock1 = getLockWithDefaults(TEST_LOCK_NAME);
		ReentrantDistributedLock lock2 = getLockWithDefaults(TEST_LOCK_NAME);
		assertTrue("Lock can be obtained.", lock1.tryLock());
		assertFalse("Lock cannot be obtained by anothe object.", lock2.tryLock());
		assertFalse("Lock that is not obtained cannot be unlocked.", lock2.tryUnlock());
		assertTrue("Owned lock can be unlocked.", lock1.tryUnlock());
		assertFalse("Now the lock is now owned anymore.", lock1.tryUnlock());
	}

	@Test
	public void test_two_independent_locks_basic_behavior() {
		String lockName1 = "myLock1";
		String lockName2 = "myLock2";
		ReentrantDistributedLock lock1 = getLockWithDefaults(lockName1);
		ReentrantDistributedLock lock2 = getLockWithDefaults(lockName2);
		assertFalse(isLockHeld(lockName1));
		assertFalse(isLockHeld(lockName2));
		assertTrue(lock1.tryLock());
		assertTrue(lock2.tryLock());
		assertTrue(isLockHeld(lockName1));
		assertTrue(isLockHeld(lockName2));
		assertTrue(lock1.tryUnlock());
		assertTrue(lock2.tryUnlock());
		assertFalse(isLockHeld(lockName1));
		assertFalse(isLockHeld(lockName2));

		lock1.lock();
		lock2.lock();
		assertTrue(isLockHeld(lockName1));
		assertTrue(isLockHeld(lockName2));
	}

	@Test
	public void test_two_independent_locks_are_truly_independent() {
		String lockName1 = "myLock1";
		String lockName2 = "myLock2";
		ReentrantDistributedLock lock1 = getLockWithDefaults(lockName1);
		ReentrantDistributedLock lock2 = getLockWithDefaults(lockName2);

		assertFalse(isLockHeld(lockName1));
		assertFalse(isLockHeld(lockName2));

		// Check that the first lock does not change state from unlocked to
		// locked.
		assertTrue(lock2.tryLock());
		assertFalse(isLockHeld(lockName1));
		assertTrue(lock2.tryUnlock());
		assertFalse(isLockHeld(lockName1));

		// Check that the first lock does not change state from locked to
		// unlocked.
		assertTrue(lock1.tryLock());
		assertTrue(lock2.tryLock());
		assertTrue(isLockHeld(lockName1));
		assertTrue(lock2.tryUnlock());
		assertTrue(isLockHeld(lockName1));

		assertTrue(lock1.tryUnlock());
		assertFalse(isLockHeld(lockName1));
		assertFalse(isLockHeld(lockName1));
	}

	@Test
	public void test_two_dependent_locks() throws InterruptedException {
		ReentrantDistributedLock lock1 = getLockWithDefaults(TEST_LOCK_NAME);
		ReentrantDistributedLock lock2 = getLockWithDefaults(TEST_LOCK_NAME);

		assertFalse(isLockHeld(TEST_LOCK_NAME));
		assertTrue(lock1.tryLock());

		assertFalse(lock2.tryLock());
		assertFalse(lock2.tryLock(1, TimeUnit.MILLISECONDS));

		assertTrue(isLockHeld(TEST_LOCK_NAME));
		assertTrue(lock1.tryUnlock());
		assertFalse(lock2.tryUnlock());
		assertFalse(isLockHeld(TEST_LOCK_NAME));
		assertFalse(lock1.tryUnlock());

		// Acquire after tryUnlock
		assertTrue(lock2.tryLock());
		assertTrue(isLockHeld(TEST_LOCK_NAME));
		assertTrue(lock2.tryUnlock());
		assertFalse(isLockHeld(TEST_LOCK_NAME));
	}

	@Test
	public void test_blocking_acquire_without_timeout() throws InterruptedException {
		// 100 ms is a short lease
		ReentrantDistributedLock lock1 = new ReentrantDistributedLock(TEST_LOCK_NAME, jedis, Clock.systemUTC(), 100,
				ReentrantDistributedLock.DEFAULT_LOCK_ACQUIRE_LAMBDA);
		ReentrantDistributedLock lock2 = new ReentrantDistributedLock(TEST_LOCK_NAME, jedis, Clock.systemUTC(), 100,
				ReentrantDistributedLock.DEFAULT_LOCK_ACQUIRE_LAMBDA);

		assertTrue(lock1.tryLock());
		// Eventually lock1's lease has to expire.
		lock2.lock();
		assertTrue("Now we should have the lock and we just check that we can lock it.", lock2.tryLock());
	}

	@Test
	public void test_dont_unlock_lock_you_dont_own() throws InterruptedException {
		ReentrantDistributedLock lock = new ReentrantDistributedLock(TEST_LOCK_NAME, jedis, Clock.systemUTC(), 50,
				ReentrantDistributedLock.DEFAULT_LOCK_ACQUIRE_LAMBDA);
		assertTrue(lock.tryLock());
		assertTrue(isLockHeld(TEST_LOCK_NAME));
		// Wait for the lease to expire.
		Thread.sleep(100);
		assertFalse("Lock is no longer present.", isLockHeld(TEST_LOCK_NAME));
		assertFalse("And cannot be unlocked.", lock.tryUnlock());
	}

	@Test
	public void test_blocking_acquire_with_timeout() throws InterruptedException {
		// 100 ms is a short lease
		ReentrantDistributedLock lock1 = new ReentrantDistributedLock(TEST_LOCK_NAME, jedis, Clock.systemUTC(), 100,
				ReentrantDistributedLock.DEFAULT_LOCK_ACQUIRE_LAMBDA);
		ReentrantDistributedLock lock2 = new ReentrantDistributedLock(TEST_LOCK_NAME, jedis, Clock.systemUTC(), 100,
				ReentrantDistributedLock.DEFAULT_LOCK_ACQUIRE_LAMBDA);

		assertTrue(lock1.tryLock());
		assertTrue("Eventually lock1's lease has to expire", lock2.tryLock(1000, TimeUnit.MILLISECONDS));
	}

	@Test
	public void test_blocking_acquire_with_timeout_expired() throws InterruptedException {
		// 100 ms is a short lease
		ReentrantDistributedLock lock1 = new ReentrantDistributedLock(TEST_LOCK_NAME, jedis, Clock.systemUTC());
		ReentrantDistributedLock lock2 = new ReentrantDistributedLock(TEST_LOCK_NAME, jedis, Clock.systemUTC(), 100,
				ReentrantDistributedLock.DEFAULT_LOCK_ACQUIRE_LAMBDA);

		assertTrue(lock1.tryLock());
		assertFalse("Lock1's lease won't expire so soon.", lock2.tryLock(100, TimeUnit.MILLISECONDS));
	}

	@Test
	public void test_isLockHeldBySomeone() {
		ReentrantDistributedLock lock = getLockWithDefaults(TEST_LOCK_NAME);
		assertFalse(ReentrantDistributedLock.isLockHeldBySomeone(jedis, TEST_LOCK_NAME));
		assertTrue(lock.tryLock());
		assertTrue(ReentrantDistributedLock.isLockHeldBySomeone(jedis, TEST_LOCK_NAME));
	}

	@Test
	public void test_blocking_call_throws_after_timeout()
			throws InterruptedException, ExecutionException, TimeoutException {
		final ReentrantDistributedLock lock1 = getLockWithDefaults(TEST_LOCK_NAME);
		final FakeClock fakeClock = new FakeClock(Clock.systemUTC().millis());
		final ReentrantDistributedLock lock2 = new ReentrantDistributedLock(TEST_LOCK_NAME, jedis, fakeClock);
		ExecutorService pool = Executors.newFixedThreadPool(5);
		try {
			lock1.lock();
			Future<String> future = pool.submit(new Callable<String>() {

				@Override
				public String call() throws Exception {
					try {
						lock2.lock();
						return "FAIL";
					} catch (Exception expected) {
						return "OK";
					}
				}
			});
			Thread.sleep(100);
			fakeClock.setMillisNow(
					Clock.systemUTC().millis() + ReentrantDistributedLock.MAX_BLOCKING_ACQUIRE_TIMEOUT + 1000);

			String retVal = future.get(1000, TimeUnit.MILLISECONDS);
			assertEquals("OK", retVal);
		} finally {
			pool.shutdown();
		}
	}

	private ReentrantDistributedLock getLockWithDefaults(String lockName) {
		return new ReentrantDistributedLock(lockName, jedis, Clock.systemUTC());
	}

	/**
	 * Helper to determine whether the lock is free or not.
	 */
	private boolean isLockHeld(String lockName) {
		return ReentrantDistributedLock.isLockHeldBySomeone(jedis, lockName);
	}

	static class FakeClock extends Clock {

		public FakeClock() {
			this(0);
		}

		public FakeClock(long millisNow) {
			this.millisNow = millisNow;
		}

		public synchronized void setMillisNow(long millisNow) {
			this.millisNow = millisNow;
		}

		private long millisNow;

		@Override
		public synchronized long millis() {
			return millisNow;
		}

		@Override
		public ZoneId getZone() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Clock withZone(ZoneId zone) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Instant instant() {
			// TODO Auto-generated method stub
			return null;
		}

	}

}
