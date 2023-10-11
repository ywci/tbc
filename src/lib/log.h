#ifndef _LOG_H
#define _LOG_H

#include <stdio.h>
#include <stdlib.h>
#include <tbc.h>
#include "util.h"

#define DRAW_ENQUEUE   ">> enqueue <<"
#define DRAW_DEQUEUE   ">> dequeue <<"
#define DRAW_DELIVER   "<deliver> ==>"
#define DRAW_SEPARATOR "---------------------------------------------"
#define DRAW_INDICATOR "^------------------------------------------->"

#define PROG_INTV      5000

#define get_func_name() _func_name
#define gen_func_name() \
    char _func_name[1024] = {0}; \
    sprintf(_func_name, "[%s]", __func__) \

#if defined(QUIET) && !defined(DEBUG)
#define log_is_valid() false
#define log_enable() do {} while (0)
#define log_disable() do {} while (0)
#define log_func(...) do {} while (0)
#define log_info(...) do {} while (0)
#define log_debug(...) do {} while (0)
#define log_func_info(...) do {} while (0)
#else
#define log_info(fmt, ...) printf(fmt "\n", ##__VA_ARGS__)
#define log_func_info(fmt, ...)  do { \
    gen_func_name(); \
    printf("%s " fmt "\n", get_func_name(), ##__VA_ARGS__); \
} while (0)

#ifdef DEBUG
#define log_is_valid() true
#define log_enable() do {} while (0)
#define log_disable() do {} while (0)

#define log_func(fmt, ...) do { \
    gen_func_name(); \
    printf("%s " fmt "\n", get_func_name(), ##__VA_ARGS__); \
} while (0)

#define log_debug(fmt, ...) printf(fmt "\n", ##__VA_ARGS__)
#else
#define log_is_valid() !quiet
#define log_enable() (quiet = false)
#define log_disable() (quiet = true)

#define log_func(fmt, ...) do { \
    if (log_is_valid()) { \
        gen_func_name(); \
        printf("%s " fmt "\n", get_func_name(), ##__VA_ARGS__); \
    } \
} while (0)

#define log_debug(fmt, ...) do { \
    if (log_is_valid()) \
        printf(fmt "\n", ##__VA_ARGS__); \
} while (0)
#endif
#endif

#define log_file(fmt, ...) do { \
    FILE *fp = fopen(log_name, "a+"); \
    fprintf(fp, fmt "\n", ##__VA_ARGS__); \
    fclose(fp); \
} while (0)

#define log_file_remove() do { \
    if (access(log_name, F_OK) != -1) \
        remove(log_name); \
} while (0)

#define log_err(fmt, ...) do { \
    printf("Error: " fmt  " (file %s, line %d, function %s\n", ##__VA_ARGS__, __FILE__, __LINE__, __func__); \
    exit(-1); \
} while (0)

#ifdef CRASH_DEBUG
#define crash_debug(fmt, ...) log_debug(">> crash << " fmt, ##__VA_ARGS__)
#define crash_show_array show_array_details
#define crash_show_bitmap show_bitmap_details
#else
#define crash_debug(...) do {} while (0)
#define crash_show_array(...) do {} while (0)
#define crash_show_bitmap(...) do {} while (0)
#endif

#ifdef CRASH_DETAILS
#define crash_details log_func
#define crash_array_details show_array_details
#define crash_bitmap_details show_bitmap_details
#else
#define crash_details(...) do {} while (0)
#define crash_array_details(...) do {} while (0)
#define crash_bitmap_details(...) do {} while (0)
#endif

#define print_timestamp(str, timestamp) printf("%s t=%08lx.%08lx.%lx", str, (unsigned long)(timestamp)->sec, (unsigned long)(timestamp)->usec, (unsigned long)(timestamp)->hid)

#ifdef SHOW_PROGRESS
static inline void show_progress(char *str, int id, seq_t seq, seq_t *progress)
{
    if ((seq != progress[id]) && (seq % PROG_INTV == 0)) {
        progress[id] = seq;
        log_info("%s progress (id=%d, seq=%d)", str, id, seq);
    }
}

static inline void show_pack_progress(char *str, int id, seq_t seq)
{
    static seq_t progress[NODE_MAX] = {0};

    show_progress(str, id, seq, progress);
}

static inline void show_visible_progress(char *str, int id, seq_t seq)
{
    static seq_t progress[NODE_MAX] = {0};

    show_progress(str, id, seq, progress);
}
#else
#define show_pack_progress(...) do {} while (0)
#define show_visible_progress(...) do {} while (0)
#endif

#define show_array_details(str, name, array) do { \
    if (log_is_valid()) { \
        const int bufsz = 1024; \
        const int reserve = 256; \
        if (strlen(str) + reserve < bufsz) { \
            int _n; \
            char buf[bufsz]; \
            char *p = buf; \
            assert(nr_nodes > 1); \
            if (strlen(str) > 0) \
                sprintf(p, "%s %s=|", str, name); \
            else \
                sprintf(p, "%s=|", name); \
            p += strlen(p); \
            for (_n = 0; _n < nr_nodes - 1; _n++) { \
                sprintf(p, "%d|", (array)[_n]); \
                p += strlen(p); \
            } \
            sprintf(p, "%d|", (array)[_n]); \
            log_info("%s", buf); \
        } \
    } \
} while (0)

#define show_array_range(str, name, array, start, len) do { \
    if (log_is_valid()) { \
        const int bufsz = 1024; \
        const int reserve = 256; \
        if (strlen(str) + reserve < bufsz) { \
            int _n; \
            char buf[bufsz]; \
            char *p = buf; \
            assert(nr_nodes > 1); \
            if (strlen(str) > 0) \
                sprintf(p, "%s %s=|", str, name); \
            else \
                sprintf(p, "%s=|", name); \
            p += strlen(p); \
            for (_n = start; _n < start + len - 1; _n++) { \
                sprintf(p, "%d|", array[_n]); \
                p += strlen(p); \
            } \
            sprintf(p, "%d|", array[_n]); \
            log_info("%s", buf); \
        } \
    } \
} while (0)

#define show_bitmap_info(str, name, bitmap) do { \
    const int bufsz = 1024; \
    const int reserve = 256; \
    if (strlen(str) + reserve < bufsz) { \
        int _n; \
        char buf[bufsz]; \
        char *p = buf; \
        sprintf(p, "%s %s=|", str, name); \
        p += strlen(p); \
        for (_n = 0; _n < nr_nodes; _n++) { \
            sprintf(p,"%d|", (bitmap & node_mask[_n]) != 0); \
            p += strlen(p); \
        } \
        log_info("%s", buf); \
    } \
} while (0)

#define show_bitmap_details(str, name, bitmap) do { \
    if (log_is_valid()) \
        show_bitmap_info(str, name, bitmap); \
} while (0)

#define gen_ts_pair(str1, rec1, str2, rec2) \
    char _ts1[128]; \
    char _ts2[128]; \
    if (rec1) { \
        record_t *_prec1 = rec1; \
        timestamp_t *_pts1 = _prec1->timestamp; \
        sprintf(_ts1, "%s=%08lx.%08lx.%lx", str1, (unsigned long)(_pts1)->sec, (unsigned long)(_pts1)->usec, (unsigned long)(_pts1)->hid); \
    } else \
        sprintf(_ts1, "%s=empty", str1); \
    if (rec2) { \
        record_t *_prec2 = rec2; \
        timestamp_t *_pts2 = _prec2->timestamp; \
        sprintf(_ts2, "%s=%08lx.%08lx.%lx", str2, (unsigned long)(_pts2)->sec, (unsigned long)(_pts2)->usec, (unsigned long)(_pts2)->hid); \
    } else \
        sprintf(_ts2, "%s=empty", str2); \

#define ts_fst() _ts1
#define ts_snd() _ts2

#define show_timestamp_info(str, id, timestamp) do { \
    if (id >= 0) { \
        if (strlen(str) > 0) \
            log_info("%s t=%08lx.%08lx.%lx (id=%d)", str, (unsigned long)(timestamp)->sec, (unsigned long)(timestamp)->usec, (unsigned long)(timestamp)->hid, id); \
        else \
            log_info("t=%08lx.%08lx.%lx (id=%d)", (unsigned long)(timestamp)->sec, (unsigned long)(timestamp)->usec, (unsigned long)(timestamp)->hid, id); \
    } else { \
        if (strlen(str) > 0) \
            log_info("%s t=%08lx.%08lx.%lx", str, (unsigned long)(timestamp)->sec, (unsigned long)(timestamp)->usec, (unsigned long)(timestamp)->hid); \
        else \
            log_info("t=%08lx.%08lx.%lx", (unsigned long)(timestamp)->sec, (unsigned long)(timestamp)->usec, (unsigned long)(timestamp)->hid); \
    } \
} while (0)

#define show_timestamp_details(str, id, timestamp) do { \
    if (log_is_valid()) \
        show_timestamp_info(str, id, timestamp); \
} while (0)

#define show_record_details(id, rec) do { \
    if (log_is_valid() && rec) { \
        gen_func_name(); \
        log_func("perceived=%d, ignored=%d", rec->perceived, rec->ignore); \
        show_timestamp_details(DRAW_INDICATOR, id, rec->timestamp); \
    } \
} while (0)

#ifdef SHOW_BATCH
#define show_batch(id, rec) do { \
    if (log_is_valid() && rec) { \
        gen_func_name(); \
        show_bitmap_details(get_func_name(), "recv", rec->receivers); \
        show_array_details(get_func_name(), "visi", rec->visible); \
        show_timestamp_details(DRAW_INDICATOR, id, rec->timestamp); \
    } \
} while (0)
#else
#define show_batch(...) do {} while (0)
#endif

#ifdef SHOW_QUEUE
#define show_queue(id, record, fmt, ...) do { \
    log_func(fmt, ##__VA_ARGS__); \
    show_timestamp(DRAW_INDICATOR, id, record->timestamp); \
} while (0)
#else
#define show_queue(...) do {} while (0)
#endif

#ifdef SHOW_PERCEIVED
#define show_perceived(id, rec) do { \
    if (log_is_valid()) { \
        gen_func_name(); \
        log_func("perceived=%d, ignore=%d", rec->perceived, rec->ignore); \
        show_timestamp_details(DRAW_INDICATOR, id, rec->timestamp); \
    } \
} while (0)
#else
#define show_perceived(...) do {} while (0)
#endif

#ifdef SHOW_DEP
#define show_dep(id, dest, src) do { \
    gen_func_name(); \
    if (log_is_valid()) { \
        seq_t *psrc = src; \
        char name[64] = {0}; \
        if (psrc) { \
            for (int _i = 0; _i < nr_nodes; _i++) { \
                if (_i != id) \
                    sprintf(name, "   src_dep[%d]<", _i); \
                else \
                    sprintf(name, "-> src_dep[%d]<", _i); \
                show_array_details(get_func_name(), name, &psrc[_i * nr_nodes]); \
            } \
            log_info(DRAW_SEPARATOR); \
        } \
        for (int _i = 0; _i < nr_nodes; _i++) { \
            if (_i != id) \
                sprintf(name, "   dst_mtx[%d]<", _i); \
            else \
                sprintf(name, "-> dst_mtx[%d]<", _i); \
            show_array_details(get_func_name(), name, dest[id][_i]); \
        } \
        log_info(DRAW_SEPARATOR); \
    } \
} while (0)
#else
#define show_dep(...) do {} while (0)
#endif

#ifdef SHOW_CLEANER
#define show_cleaner(str, id, rec) do { \
    if (log_is_valid()) { \
        char name[128]; \
        sprintf(name, "%s clean", str); \
        gen_func_name(); \
        show_bitmap_details(get_func_name(), name, rec->clean); \
        show_timestamp_details(DRAW_INDICATOR, id, rec->timestamp); \
    } \
} while (0)
#else
#define show_cleaner(...) do {} while (0)
#endif

#ifdef SHOW_VISIBLE
#define show_visible(id, rec) do { \
    if ((rec->seq[id] > 0) && ((rec->seq[id] % PROG_INTV) == 0)) \
        show_visible_progress(">> visible <<", id, rec->seq[id]); \
    if (log_is_valid()) { \
        log_func(">> visible << (seq=%d, id=%d)", rec->seq[id], id); \
        show_timestamp_details(DRAW_INDICATOR, id, rec->timestamp); \
    } \
} while (0)
#else
#define show_visible(...) do {} while (0)
#endif

#ifdef SHOW_INVISIBLE
#define show_invisible(id, rec, cnt) do { \
    if (log_is_valid()) { \
        log_func(">> invisible << (seq=%d, cnt=%d, id=%d)", rec->seq[id], cnt, id); \
        show_timestamp_details(DRAW_INDICATOR, id, rec->timestamp); \
    } \
} while (0)
#else
#define show_invisible(...) do {} while (0)
#endif

#ifdef SHOW_HEADER
static inline void show_session(int id, int session, int cnt)
{
    static int sessions[NODE_MAX] = {0};

    if (session != sessions[id]) {
        sessions[id] = session;
        log_info(">> (session: %d, count: %d)", session, cnt);
    }
}
#define show_header(id, hdr) show_session(id, (hdr)->session, (hdr)->count)
#else
#define show_header(...) do {} while (0)
#endif

#ifdef SHOW_PACK
#define show_pack(dep_mtx, pack) do { \
    gen_func_name(); \
    for (int _i = 0; _i < nr_nodes; _i++) { \
        if (pack) \
            show_pack_progress(">> pack <<", _i, dep_mtx[node_id][node_id][_i]); \
        else \
            show_pack_progress(get_func_name(), _i, dep_mtx[node_id][node_id][_i]); \
    } \
} while (0)
#else
#define show_pack(...) do {} while (0)
#endif

#ifdef SHOW_PREV
#define show_prev(id, curr, prev, trigger) do { \
    if (log_is_valid()) { \
        gen_ts_pair("curr", curr, "prev", prev); \
        log_func("%s, %s (id=%d)", ts_fst(), ts_snd(), id); \
        if (trigger) { \
            record_t *_trigger = trigger; \
            show_timestamp_details(DRAW_INDICATOR, id, _trigger->timestamp); \
        } \
    } \
} while (0)
#define show_prev_str(id, curr, prev, str) do { \
    if (log_is_valid()) { \
        const char *_pstr = str; \
        gen_ts_pair("curr", curr, "prev", prev); \
        if (_pstr) \
            log_func("%s, %s, %s (id=%d)", _pstr, ts_fst(), ts_snd(), id); \
        else \
            log_func("%s, %s (id=%d)", ts_fst(), ts_snd(), id); \
    } \
} while (0)
#else
#define show_prev(...) do {} while (0)
#endif

#ifdef SHOW_BLOCKER
#define show_blocker(id, blk, rec) do { \
    if (log_is_valid()) { \
        char name[64] = {0}; \
        gen_func_name(); \
        sprintf(name, "%s >> blocker <<", get_func_name()); \
        show_timestamp_details(name, id, blk->timestamp); \
        show_timestamp_details(DRAW_INDICATOR, id, rec->timestamp); \
    } \
} while (0)
#else
#define show_blocker(...) do {} while (0)
#endif

#ifdef SHOW_IGNORE
#define show_ignore(id, rec) do { \
    if (log_is_valid() && is_valid(&rec->checked[id])) { \
        log_enable(); \
        gen_func_name(); \
        show_timestamp_details(get_func_name(), id, rec->timestamp); \
        log_err("%s delivered=%d, perceived=%d", DRAW_INDICATOR, is_delivered(rec), rec->perceived); \
    } \
} while (0)
#else
#define show_ignore(...) do {} while (0)
#endif

#ifdef SHOW_PROVIDERS
#define show_providers(seqlist, suspect, providers) do { \
    if (log_is_valid()) { \
        crash_array_details("collector suspects", "seqlist", seqlist); \
        crash_bitmap_details("collector suspects", "suspect", suspect); \
        for (int _i; _i < nr_nodes; _i++) { \
            if (providers[_i]) { \
                char str[32] = {0}; \
                sprintf(str, "queue_%d providers", _i); \
                crash_bitmap_details("collector suspects", str, providers[_i]); \
            } \
        } \
    } \
} while (0)
#else
#define show_providers(...) do {} while (0)
#endif

#ifdef SHOW_SUSPECT
#define show_suspect(suspect, seq) do { \
    if (log_is_valid()) { \
        gen_func_name(); \
        crash_array_details(get_func_name(), "seqlist", seq); \
        crash_bitmap_details(get_func_name(), "suspect", suspect); \
    } \
} while (0)
#else
#define show_suspect(...) do {} while (0)
#endif

#ifdef SHOW_DELIVER
#define show_deliver(rec, cnt) do { \
    log_func("count=%ld", cnt); \
    show_timestamp_details(DRAW_DELIVER, -1, rec->timestamp); \
} while(0)
#else
#define show_deliver(...) do {} while (0)
#endif

#ifdef SHOW_REPLAYER
#define show_replayer(fmt, ...) do { \
    quiet = false; \
    log_func(fmt, ##__VA_ARGS__); \
} while (0)
#else
#define show_replayer(...) do {} while (0)
#endif

#ifdef SHOW_COLLECTOR
#define show_collector(fmt, ...) do { \
    quiet = false; \
    log_func(fmt, ##__VA_ARGS__); \
} while (0)
#else
#define show_collector(...) do {} while (0)
#endif

#ifdef DEBUG
#define show_array show_array_details
#define show_bitmap show_bitmap_details
#else
#define show_array(...) do {} while (0)
#define show_bitmap(...) do {} while (0)
#endif

#ifdef SHOW_ENQUEUE
#define show_enqueue(id, timestamp) show_timestamp(DRAW_ENQUEUE, id, timestamp)
#define show_dequeue(id, timestamp) show_timestamp(DRAW_DEQUEUE, id, timestamp)
#else
#define show_enqueue(...) do {} while (0)
#define show_dequeue(...) do {} while (0)
#endif

#ifdef SHOW_RESULT
#define show_result printf
#else
#define show_result(...) do {} while (0)
#endif

#ifdef SHOW_TIMESTAMP
#define show_timestamp show_timestamp_details
#else
#define show_timestamp(...) do {} while (0)
#endif

#ifdef SHOW_RECORD
#define show_record show_record_details
#else
#define show_record(...) do {} while (0)
#endif

#ifdef SHOW_STATUS
#define show_status() do { \
    const int cand_max = 2; \
    const int next_max = 16; \
    gen_func_name(); \
    for (int _i = 0; _i < nr_nodes; _i++) { \
        struct list_head *candidates = &tracker_status.candidates[_i]; \
        tracker_lock(_i); \
        tracker_mutex_lock(); \
        if (!list_empty(candidates)) { \
            int cand_cnt = 0; \
            struct list_head *pos; \
            for (pos = candidates->next; pos != candidates; pos = pos->next) { \
                char name[1024]; \
                record_t *curr = list_entry(pos, record_t, cand[_i]); \
                struct list_head *next = &curr->next[_i]; \
                if (is_valid(next)) { \
                    int total = 0; \
                    int next_cnt = 0; \
                    struct list_head *p; \
                    for (p = next->next; p != next; p = p->next) \
                        total++; \
                    log_func_info("********************************************************************************************"); \
                    sprintf(name, "%s * total_next=%d, ignore=%d, perceived=%d", get_func_name(), total, curr->ignore, curr->perceived); \
                    show_timestamp_info(name, _i, curr->timestamp); \
                    show_bitmap_info(get_func_name(), "* receivers", curr->receivers); \
                    log_func_info("********************************************************************************************"); \
                    for (p = next->next; p != next; p = p->next) { \
                        record_t *rec = list_entry(p, record_t, link[_i]); \
                        gen_ts_pair("next", rec, "cand", curr); \
                        log_func_info("<%d> %s (ignore=%d, perceived=%d, checked=%d), %s (id=%d)",  next_cnt + 1, ts_fst(), rec->ignore, rec->perceived, is_valid(&rec->checked[_i]), ts_snd(), _i); \
                        if (++next_cnt == next_max) \
                            break; \
                    } \
                } else { \
                    log_func_info("********************************************************************************************"); \
                    sprintf(name, "%s * total_next=0, ignore=%d, perceived=%d", get_func_name(), curr->ignore, curr->perceived); \
                    show_timestamp_info(name, _i, curr->timestamp); \
                    show_bitmap_info(get_func_name(), "* receivers", curr->receivers); \
                    log_func_info("********************************************************************************************"); \
                } \
                for (int _j = 0; _j < nr_nodes; _j++) { \
                    record_t *prev = curr->prev[_j]; \
                    if (prev) { \
                        gen_ts_pair("blk", prev, "cand", curr); \
                        log_func_info("[x] que%d=>%s (ignore=%d, perceived=%d, checked=%d), %s", _j, ts_fst(), prev->ignore, prev->perceived, is_valid(&prev->checked[_j]), ts_snd()); \
                    } else { \
                        gen_ts_pair( "blk", NULL, "cand", curr); \
                        log_func_info("[v] que%d=>%s (cand_count=%d, cand_checked=%d), %s", _j, ts_fst(), curr->count[_j], is_valid(&curr->checked[_i]), ts_snd()); \
                    } \
                } \
                for (int _j = 0; _j < nr_nodes; _j++) { \
                    struct list_head *prev = curr->cand[_j].prev; \
                    if (prev && (prev != &tracker_status.candidates[_j])) { \
                        record_t *rec = list_entry(prev, record_t, cand[_j]); \
                        gen_ts_pair("prev", rec, "cand", curr); \
                        log_func_info("> que%d=>%s (ignore=%d, perceived=%d, checked=%d), %s", _j, ts_fst(), rec->ignore, rec->perceived, is_valid(&rec->checked[_j]), ts_snd()); \
                    } else { \
                        gen_ts_pair( "prev", NULL, "cand", curr); \
                        log_func_info("> que%d=>%s (cand_input=%d), %s", _j, ts_fst(), is_valid(&curr->input[_j]), ts_snd()); \
                    } \
                } \
                if (++cand_cnt == cand_max) \
                    break; \
            } \
        } \
        tracker_mutex_unlock(); \
        tracker_unlock(_i); \
    } \
} while (0)
#else
#define show_status(...) do {} while (0)
#endif

#endif
