package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {

    static void helper(LockContext lockContext, LockType lockType, TransactionContext transaction){
        LockType type = lockContext.getExplicitLockType(transaction);
        if (lockContext.parent != null){
            helper(lockContext.parent,lockType,transaction);
        }
        if (type == LockType.NL) {
            lockContext.acquire(transaction, lockType);
        } else if (type==LockType.S && lockType==LockType.IX) {
            lockContext.promote(transaction, LockType.SIX);
        } else if (type==LockType.IS && lockType==LockType.IX) {
            lockContext.promote(transaction, LockType.IX);
        }
    }
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(hw4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        LockType type = lockContext.getEffectiveLockType(transaction);
        if (transaction == null) {
            return;
        }
        if (lockType==LockType.S && type==LockType.NL) {
            helper(lockContext.parent, LockType.IS,transaction);
            lockContext.acquire(transaction, LockType.S);
        }
        if (lockType==LockType.S && type==LockType.IS) {
            lockContext.escalate(transaction);
        }
        if (lockType==LockType.S && type==LockType.IX) {
//            helper(lockContext.parent, LockType.IX,transaction);
            lockContext.promote(transaction, LockType.SIX);
        }

        if (lockType==LockType.X && type==LockType.NL) {
            helper(lockContext.parent, LockType.IX,transaction);
            lockContext.acquire(transaction, LockType.X);
        }
        if (lockType==LockType.X && type==LockType.IX) {
            lockContext.escalate(transaction);
        }
        if (lockType==LockType.X && type==LockType.IS) {
            lockContext.escalate(transaction);
//            helper(lockContext.parent, LockType.IX,transaction);
            lockContext.promote(transaction, LockType.X);
        }
        if (lockType==LockType.X && type == LockType.S) {
            helper(lockContext.parent, LockType.IX,transaction);
            lockContext.promote(transaction, LockType.X);
        }
    }


    // TODO(proj4_part2): add helper methods as you see fit
}

