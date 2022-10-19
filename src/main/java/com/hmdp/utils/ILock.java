package com.hmdp.utils;

/**
 * <p>
 *     tryLock: get lock and set timeout,
 *     unlock: del lock
 * </p>
 *
 * @author Zihao Shen
 */
public interface ILock {
    boolean tryLock(long timeoutSec);
    void unlock();
}
