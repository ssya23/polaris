#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "mem_alloc.h"
#include "manager.h"

enum WdDieSite { WD_DIE_READ_OWNER, WD_DIE_READ_WAITERS, WD_DIE_WRITE_OWNER, WD_DIE_SITE_NUM };
enum WdWaitKind { WD_WAIT_READ, WD_WAIT_WRITE, WD_WAIT_KIND_NUM };
struct alignas(64) WdThreadStats {
  uint64_t die[WD_DIE_SITE_NUM];
  uint64_t cnt[WD_WAIT_KIND_NUM];
  uint64_t total[WD_WAIT_KIND_NUM];
  uint64_t max_total[WD_WAIT_KIND_NUM];
  uint64_t nonhead[WD_WAIT_KIND_NUM];
  uint64_t max_nonhead[WD_WAIT_KIND_NUM];
  uint64_t head[WD_WAIT_KIND_NUM];
  uint64_t max_head[WD_WAIT_KIND_NUM];
  uint64_t head_lost[WD_WAIT_KIND_NUM];
};
static WdThreadStats wd_stats[THREAD_CNT];

static inline void wd_enter_head(LockEntry * en, uint64_t now) {
  if (en->wd_at_head) return;
  en->wd_nonhead_time += now - en->wd_phase_start;
  en->wd_phase_start = now;
  en->wd_at_head = true;
}

static inline void wd_leave_head(LockEntry * en, uint64_t now) {
  if (!en->wd_at_head) return;
  en->wd_head_time += now - en->wd_phase_start;
  en->wd_phase_start = now;
  en->wd_at_head = false;
  ++en->wd_head_lost;
}

void wd_record_wait(uint64_t thd_id, lock_t type, LockEntry * entry) {
  if (!warmup_finish) return;
  WdThreadStats & s = wd_stats[thd_id];
  uint32_t k = (type == LOCK_SH) ? WD_WAIT_READ : WD_WAIT_WRITE;
  uint64_t total = entry->wd_grant_time - entry->wd_wait_start;
  ++s.cnt[k];
  s.total[k] += total;
  if (total > s.max_total[k]) s.max_total[k] = total;
  s.nonhead[k] += entry->wd_nonhead_time;
  if (entry->wd_nonhead_time > s.max_nonhead[k]) s.max_nonhead[k] = entry->wd_nonhead_time;
  s.head[k] += entry->wd_head_time;
  if (entry->wd_head_time > s.max_head[k]) s.max_head[k] = entry->wd_head_time;
  s.head_lost[k] += entry->wd_head_lost;
}

void print_wait_die_stats() {
  static const char * die_names[WD_DIE_SITE_NUM] = {"read_owner", "read_waiters", "write_owner"};
  static const char * wait_names[WD_WAIT_KIND_NUM] = {"read", "write"};
  WdThreadStats t;
  memset(&t, 0, sizeof(t));
  for (uint32_t i = 0; i < g_thread_cnt; i++) {
    const WdThreadStats & s = wd_stats[i];
    for (uint32_t d = 0; d < WD_DIE_SITE_NUM; d++) t.die[d] += s.die[d];
    for (uint32_t k = 0; k < WD_WAIT_KIND_NUM; k++) {
      t.cnt[k] += s.cnt[k];
      t.total[k] += s.total[k];
      if (s.max_total[k] > t.max_total[k]) t.max_total[k] = s.max_total[k];
      t.nonhead[k] += s.nonhead[k];
      if (s.max_nonhead[k] > t.max_nonhead[k]) t.max_nonhead[k] = s.max_nonhead[k];
      t.head[k] += s.head[k];
      if (s.max_head[k] > t.max_head[k]) t.max_head[k] = s.max_head[k];
      t.head_lost[k] += s.head_lost[k];
    }
  }
  printf("Die counts by site:\n");
  for (uint32_t d = 0; d < WD_DIE_SITE_NUM; d++)
    printf("  die_%s:\t%lu\n", die_names[d], t.die[d]);
  printf("Wait counts (time in us):\n");
  for (uint32_t k = 0; k < WD_WAIT_KIND_NUM; k++) {
    const char * n = wait_names[k];
    printf("  wait_%s_count:\t%lu\n", n, t.cnt[k]);
    printf("  wait_%s_total_us:\t%.4f\n", n, t.total[k] / 1000.0);
    printf("  wait_%s_avg_us:\t%.4f\n", n, t.cnt[k] ? t.total[k] / 1000.0 / t.cnt[k] : 0.0);
    printf("  wait_%s_max_us:\t%.4f\n", n, t.max_total[k] / 1000.0);
    printf("  wait_%s_nonhead_total_us:\t%.4f\n", n, t.nonhead[k] / 1000.0);
    printf("  wait_%s_nonhead_max_us:\t%.4f\n", n, t.max_nonhead[k] / 1000.0);
    printf("  wait_%s_head_total_us:\t%.4f\n", n, t.head[k] / 1000.0);
    printf("  wait_%s_head_max_us:\t%.4f\n", n, t.max_head[k] / 1000.0);
    printf("  wait_%s_head_lost_count:\t%lu\n", n, t.head_lost[k]);
  }
}

void Row_lock::init(row_t * row) {
  _row = row;
  owners = NULL;
  waiters_head = NULL;
  waiters_tail = NULL;
  owner_cnt = 0;
  waiter_cnt = 0;

#if LATCH == LH_SPINLOCK
  latch = new pthread_spinlock_t;
	pthread_spin_init(latch, PTHREAD_PROCESS_SHARED);
#elif LATCH == LH_MUTEX
  latch = new pthread_mutex_t;
	pthread_mutex_init(latch, NULL);
#else
  latch = new mcslock();
#endif
  lock_type = LOCK_NONE;
  blatch = false;

}

// taking the latch
void Row_lock::lock(txn_man * txn) {
    if (likely(g_thread_cnt > 1)) {
            if (unlikely(g_central_man))
                glob_manager->lock_row(_row);
            else {
#if LATCH == LH_SPINLOCK
                pthread_spin_lock( latch );
#elif LATCH == LH_MUTEX
                pthread_mutex_lock( latch );
#else
                latch->acquire(txn->mcs_node);
#endif
            }
    }
};

// release the latch
void Row_lock::unlock(txn_man * txn) {
        if (likely(g_thread_cnt > 1)) {
            if (unlikely(g_central_man))
                glob_manager->release_row(_row);
            else {
#if LATCH == LH_SPINLOCK
                pthread_spin_unlock( latch );
#elif LATCH == LH_MUTEX
                pthread_mutex_unlock( latch );
#else
                latch->release(txn->mcs_node);
#endif
            }
        }
};

RC Row_lock::lock_get(lock_t type, txn_man * txn, Access * access) {
  uint64_t *txnids = NULL;
  int txncnt = 0;
  return lock_get(type, txn, txnids, txncnt, access);
}

RC Row_lock::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids,
    int &txncnt, Access * access) {
  assert (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE);
  RC rc;
  int part_id =_row->get_part_id();

  LockEntry * entry = get_entry(access);

#if PF_CS
  uint64_t starttime = get_sys_clock();
#endif
  lock(entry->txn);
  COMPILER_BARRIER
#if PF_CS
  uint64_t endtime = get_sys_clock();
  INC_STATS(txn->get_thd_id(), time_get_latch, endtime - starttime);
  starttime = endtime;
#endif
  assert(owner_cnt <= g_thread_cnt);
  assert(waiter_cnt < g_thread_cnt);
#if DEBUG_ASSERT
  if (owners != NULL)
		assert(lock_type == owners->type); 
	else 
		assert(lock_type == LOCK_NONE);
	LockEntry * en = owners;
	UInt32 cnt = 0;
	while (en) {
		assert(en->txn->get_thd_id() != txn->get_thd_id());
		cnt ++;
		en = en->next;
	}
	assert(cnt == owner_cnt);
	en = waiters_head;
	cnt = 0;
	while (en) {
		cnt ++;
		en = en->next;
	}
	assert(cnt == waiter_cnt);
#endif

  bool conflict = conflict_lock(lock_type, type);
  bool lock_conflict = conflict;
  if (CC_ALG == WAIT_DIE && !conflict) {
    if (waiters_head && txn->get_ts() < waiters_head->txn->get_ts())
      conflict = true;
  }
  // Some txns coming earlier is waiting. Should also wait.
  //if (CC_ALG == DL_DETECT && waiters_head != NULL)
  if (waiters_head != NULL)
    conflict = true;

  if (conflict) {
    // Cannot be added to the owner list.
    if (CC_ALG == NO_WAIT) {
      rc = Abort;
      goto final;
    } else if (CC_ALG == DL_DETECT) {
      //LockEntry * entry = get_entry();
      entry->txn = txn;
      entry->type = type;
      LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
      entry->status = LOCK_WAITER;
      waiter_cnt ++;
      txn->lock_ready = false;
      rc = WAIT;
    } else if (CC_ALG == WAIT_DIE) {
      ///////////////////////////////////////////////////////////
      //  - T is the txn currently running
      //	IF T.ts < ts of all owners
      //		T can wait
      //  ELSE
      //      T should abort
      //////////////////////////////////////////////////////////

      bool canwait = true;
      LockEntry * en = owners;
      while (en != NULL) {
        if (en->txn->get_ts() < txn->get_ts()) {
          canwait = false;
          break;
        }
        en = en->next;
      }
      if (canwait) {
        // insert txn to the right position
        // the waiter list is always in timestamp order
        //LockEntry * entry = get_entry();
        entry->txn = txn;
        entry->type = type;
        uint64_t wd_now = get_sys_clock();
        entry->wd_wait_start = wd_now;
        entry->wd_phase_start = wd_now;
        entry->wd_nonhead_time = 0;
        entry->wd_head_time = 0;
        entry->wd_head_lost = 0;
        entry->wd_at_head = false;
        en = waiters_head;
        while (en != NULL && txn->get_ts() < en->txn->get_ts())
          en = en->next;
        if (en) {
          LIST_INSERT_BEFORE(en, entry);
          if (en == waiters_head)
            waiters_head = entry;
        } else
          LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
        if (waiters_head == entry) {
          if (entry->next)
            wd_leave_head(entry->next, wd_now);
          wd_enter_head(entry, wd_now);
        }
        entry->status = LOCK_WAITER;
        waiter_cnt ++;
        txn->lock_ready = false;
        rc = WAIT;
      }
      else {
        // lock abort is not used for wait_die. since abort itself only
        if (warmup_finish) {
          uint32_t site = (type == LOCK_EX) ? WD_DIE_WRITE_OWNER
                          : (lock_conflict ? WD_DIE_READ_OWNER : WD_DIE_READ_WAITERS);
          ++wd_stats[txn->get_thd_id()].die[site];
        }
        rc = Abort;
        return_entry(entry);
      }
    }
  } else {
    entry->type = type;
    entry->txn = txn;
    txn->lock_ready = true;
    STACK_PUSH(owners, entry);
    entry->status = LOCK_OWNER;
    owner_cnt ++;
    lock_type = type;
    if (CC_ALG == DL_DETECT)
      ASSERT(waiters_head == NULL);
    rc = RCOK;
    //printf("[%p]txn-%lu got %lu\n", entry, txn->get_txn_id(), _row->get_row_id());
  }
  final:

  if (rc == WAIT && CC_ALG == DL_DETECT) {
    // Update the waits-for graph
    ASSERT(waiters_tail->txn == txn);
    txnids = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t) * (owner_cnt + waiter_cnt), part_id);
    txncnt = 0;
    LockEntry * en = waiters_tail->prev;
    while (en != NULL) {
      if (conflict_lock(type, en->type))
        txnids[txncnt++] = en->txn->get_txn_id();
      en = en->prev;
    }
    en = owners;
    if (conflict_lock(type, lock_type))
      while (en != NULL) {
        txnids[txncnt++] = en->txn->get_txn_id();
        en = en->next;
      }
    ASSERT(txncnt > 0);
  }

  COMPILER_BARRIER
  unlock(entry->txn);
#if PF_CS
  INC_STATS(txn->get_thd_id(), time_get_cs, get_sys_clock() - starttime);
#endif

  return rc;
}


RC Row_lock::lock_release(LockEntry * entry) {
#if PF_CS
  uint64_t starttime = get_sys_clock();
#endif
  lock(entry->txn);
  COMPILER_BARRIER
#if PF_CS
  uint64_t endtime = get_sys_clock();
  INC_STATS(entry->txn->get_thd_id(), time_release_latch, endtime - starttime);
  starttime = endtime;
#endif
  LockEntry * en;
  // Try to find the entry in the owners
  if (entry->status == LOCK_OWNER) { // find the entry in the owner list
    en = owners;
    LockEntry * prev = NULL;
    while(en) {
      if (en == entry)
        break;
      prev = en;
      en = en->next;
    }
    // rm from owners
    if (prev)
      prev->next = entry->next;
    else
      owners = entry->next;
    owner_cnt --;
    if (owner_cnt == 0)
      lock_type = LOCK_NONE;
  } else if (entry->status == LOCK_WAITER) {
    // Not in owners list, try waiters list.
    LIST_REMOVE(entry);
    if (entry == waiters_head)
      waiters_head = entry->next;
    if (entry == waiters_tail)
      waiters_tail = entry->prev;
    waiter_cnt --;
  } else {
    assert(false);
  }

  if (owner_cnt == 0)
    ASSERT(lock_type == LOCK_NONE);
#if DEBUG_ASSERT && CC_ALG == WAIT_DIE
  for (en = waiters_head; en != NULL && en->next != NULL; en = en->next)
			assert(en->next->txn->get_ts() < en->txn->get_ts());
#endif
  // If any waiter can join the owners, just do it!
#if CC_ALG == WAIT_DIE
  uint64_t wd_now = get_sys_clock();
#endif
  while (waiters_head && !conflict_lock(lock_type, waiters_head->type)) {
    LIST_GET_HEAD(waiters_head, waiters_tail, en);
#if CC_ALG == WAIT_DIE
    if (en->wd_at_head)
      en->wd_head_time += wd_now - en->wd_phase_start;
    else
      en->wd_nonhead_time += wd_now - en->wd_phase_start;
    en->wd_at_head = false;
    en->wd_grant_time = wd_now;
    COMPILER_BARRIER
#endif
    STACK_PUSH(owners, en);
    en->status = LOCK_OWNER;
    owner_cnt ++;
    waiter_cnt --;
    ASSERT(en->txn->lock_ready == false);
    en->txn->lock_ready = true;
    lock_type = en->type;
    //printf("[%p]txn-%lu got %lu\n", en, en->txn->get_txn_id(), _row->get_row_id());
  }
#if CC_ALG == WAIT_DIE
  if (waiters_head)
    wd_enter_head(waiters_head, wd_now);
#endif
  ASSERT((owners == NULL) == (owner_cnt == 0));
  COMPILER_BARRIER
  unlock(entry->txn);
#if PF_CS
  INC_STATS(entry->txn->get_thd_id(), time_release_cs, get_sys_clock() -
      starttime);
#endif
  return RCOK;
}

bool Row_lock::conflict_lock(lock_t l1, lock_t l2) {
  if (l1 == LOCK_NONE || l2 == LOCK_NONE)
    return false;
  else if (l1 == LOCK_EX || l2 == LOCK_EX)
    return true;
  else
    return false;
}

inline 
LockEntry * Row_lock::get_entry(Access * access) {
  //LockEntry * entry = (LockEntry *) mem_allocator.alloc(sizeof(LockEntry),
  // _row->get_part_id());
  #if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == DL_DETECT
  LockEntry * entry = (LockEntry *) access->lock_entry;
  entry->next = NULL;
  entry->prev = NULL;
  entry->status = LOCK_DROPPED;
  return entry;
  #else 
  return NULL;
  #endif
}

inline 
void Row_lock::return_entry(LockEntry * entry) {
  //mem_allocator.free(entry, sizeof(LockEntry));
  entry->next = NULL;
  entry->prev = NULL;
  entry->type = LOCK_NONE;
  entry->status = LOCK_DROPPED;
}

