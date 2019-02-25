#Redis lock 

Simple implementation of reentrant distributed lock using Redis.
The implementation implements {@link Lock} so it can be used instead of
regular java locks.

The current implementation is guarded by expiring keys so it cannot
deadlock forever in case of a disappearing lock holder.
 
If the lock is to be held for a longer period of time, it's important to keep
refreshing it using {@link #tryLock()} (or some other locking method) so the
key doesn't expire.
