package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockName string
	ID       string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockName: lockname, ID: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here

	for {
		value, ver, err := lk.ck.Get(lk.lockName)
		if err == rpc.ErrNoKey {
			// try to put with my key
			putErr := lk.ck.Put(lk.lockName, lk.ID, 0)
			if putErr == rpc.OK {
				return
			} else {
				// someone else got the lock, loop again
				continue
			}
		} else if value == "" {
			err := lk.ck.Put(lk.lockName, lk.ID, ver)
			if err == rpc.OK {
				return
			} else {
				// someone else got the lock, loop again
				continue
			}
		} else if value == lk.ID {
			// I already have the lock, return
			return
		} else {
			// someone else has the lock, loop again
			// if ver > version {
			// 	continue
			// }
			// version = ver
			continue
		}
	}

}

func (lk *Lock) Release() {
	// Your code here
	val, ver, err := lk.ck.Get(lk.lockName)
	if err == rpc.ErrNoKey {
		// lock is already released
		return
	} else if val == "" {
		// lock is already released
		return
	}
	lk.ck.Put(lk.lockName, "", rpc.Tversion(ver))

}
