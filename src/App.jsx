import React, { useState, useMemo, useRef, useEffect, useCallback } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import {
  Search, Bell, X, Trophy, Target, Clock, Info,
  Wifi, WifiOff, RefreshCw, ChevronDown, Loader2, Zap
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

function cn(...inputs) { return twMerge(clsx(inputs)); }

// ─── Config ───────────────────────────────────────────────────────────────────
const API_BASE   = 'http://localhost:8000';
const TOP_RANK_N = 3;
const POLL_MS    = 2500;   // progress poll interval (ms)

// ─── Utility: format Unix epoch or duration → HH:MM:SS ───────────────────────
function fmtTime(ts) {
  if (!ts) return '--:--:--';
  if (ts < 86400) {
    const h = String(Math.floor(ts / 3600)).padStart(2, '0');
    const m = String(Math.floor((ts % 3600) / 60)).padStart(2, '0');
    const s = String(ts % 60).padStart(2, '0');
    return `${h}:${m}:${s}`;
  }
  return new Date(ts * 1000).toLocaleTimeString('en-GB', { hour12: false });
}

// ────────────────────────────────────────────────────────────────────────────────
// Hook: useContests — fetch dynamic contest list on mount
// ────────────────────────────────────────────────────────────────────────────────
function useContests() {
  const [contests, setContests] = useState([]);
  const [loading,  setLoading]  = useState(true);

  useEffect(() => {
    fetch(`${API_BASE}/contests/latest`)
      .then(r => r.json())
      .then(data => {
        setContests(Array.isArray(data) ? data : []);
        setLoading(false);
      })
      .catch(() => {
        // Graceful fallback so the UI never fully breaks
        setContests([
          { title: 'Weekly Contest 400',   slug: 'weekly-contest-400'   },
          { title: 'Biweekly Contest 130', slug: 'biweekly-contest-130' },
          { title: 'Weekly Contest 399',   slug: 'weekly-contest-399'   },
        ]);
        setLoading(false);
      });
  }, []);

  return { contests, loading };
}

// ────────────────────────────────────────────────────────────────────────────────
// Hook: usePredictions — fetch + poll progress + handle 202 "scraping" state
// ────────────────────────────────────────────────────────────────────────────────
function usePredictions(contestSlug) {
  const [data,     setData]     = useState([]);
  const [status,   setStatus]   = useState('idle');   // idle | loading | scraping | done | error
  const [error,    setError]    = useState(null);
  const [progress, setProgress] = useState({ pct: 0, pages_done: 0, total_pages: 0 });
  const [meta,     setMeta]     = useState({ total: 0, contest: '' });
  const pollRef = useRef(null);

  const stopPolling = () => {
    if (pollRef.current) { clearInterval(pollRef.current); pollRef.current = null; }
  };

  const pollProgress = useCallback((slug) => {
    stopPolling();
    pollRef.current = setInterval(async () => {
      try {
        const r    = await fetch(`${API_BASE}/predict/${slug}/progress`);
        const prog = await r.json();
        setProgress({ pct: prog.pct, pages_done: prog.pages_done, total_pages: prog.total_pages });

        if (prog.status === 'done') {
          stopPolling();
          // Fetch the finished result
          fetchPredictions(slug, false);
        } else if (prog.status === 'error') {
          stopPolling();
          setStatus('error');
          setError('Scrape failed on the server. Check backend logs.');
        }
      } catch {/* ignore poll errors */}
    }, POLL_MS);
  }, []);

  const fetchPredictions = useCallback(async (slug, triggerRefresh = false) => {
    if (!slug) return;
    setStatus('loading');
    setError(null);
    setData([]);
    setProgress({ pct: 0, pages_done: 0, total_pages: 0 });

    try {
      const url = triggerRefresh
        ? `${API_BASE}/predict/${slug}?refresh=true`
        : `${API_BASE}/predict/${slug}`;

      const res = await fetch(url);

      // 202 = scraping in progress (either just triggered or already running)
      if (res.status === 202) {
        setStatus('scraping');
        pollProgress(slug);
        return;
      }

      if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText}`);

      const json = await res.json();
      setData(json.predictions || []);
      setMeta({ total: json.total_participants, contest: json.contest_slug });
      setStatus('done');

    } catch (err) {
      // If the contest hasn't been scraped yet (net error or 500), auto-trigger a scrape
      if (err.message.includes('202') || err.message.includes('404') || err.message.includes('Failed to fetch')) {
        setStatus('scraping');
        // Trigger scrape by calling the endpoint (server will kick off the pipeline)
        try {
          const r2 = await fetch(`${API_BASE}/predict/${slug}`);
          if (r2.status === 202 || r2.ok) {
            setStatus('scraping');
            pollProgress(slug);
            return;
          }
        } catch { /* fallthrough */ }
      }
      setStatus('error');
      setError(err.message);
    }
  }, [pollProgress]);

  // Refetch when contest changes
  useEffect(() => {
    if (!contestSlug) return;
    stopPolling();
    fetchPredictions(contestSlug, false);
    return () => stopPolling();
  }, [contestSlug]);

  return {
    data, status, error, progress, meta,
    refetch: () => fetchPredictions(contestSlug, true),
  };
}

// ────────────────────────────────────────────────────────────────────────────────
// Root App
// ────────────────────────────────────────────────────────────────────────────────
const App = () => {
  const [search,        setSearch]        = useState('');
  const [selectedUser,  setSelectedUser]  = useState(null);
  const [isSheetOpen,   setIsSheetOpen]   = useState(false);
  const [dropdownOpen,  setDropdownOpen]  = useState(false);
  const [selectedContest, setSelectedContest] = useState(null);

  const { contests, loading: contestsLoading } = useContests();
  const { data, status, error, progress, meta, refetch } = usePredictions(
    selectedContest?.slug
  );

  // Auto-select the first contest once the list loads
  useEffect(() => {
    if (!selectedContest && contests.length > 0) {
      setSelectedContest(contests[0]);
    }
  }, [contests, selectedContest]);

  // Dual-search: username OR rank
  const filtered = useMemo(() => {
    if (!search) return data;
    const q = search.toLowerCase().trim();
    return data.filter(u =>
      u.username.toLowerCase().includes(q) ||
      String(u.global_rank).includes(q)
    );
  }, [data, search]);

  const handleRowClick = (user) => { setSelectedUser(user); setIsSheetOpen(true); };
  const handleContestSelect = (c) => { setSelectedContest(c); setDropdownOpen(false); setSearch(''); };

  const isLive     = status === 'done';
  const isScraping = status === 'scraping' || status === 'fetching_ratings';
  const isLoading  = status === 'loading' || (status === 'idle' && !!selectedContest);

  return (
    <div className="min-h-screen text-slate-100 flex flex-col"
         style={{ background: '#020617', fontFamily: "'Inter', -apple-system, sans-serif" }}>

      {/* ── Header ──────────────────────────────────────────────────────────── */}
      <header style={{ background: 'rgba(2,6,23,0.9)', backdropFilter: 'blur(16px)' }}
              className="sticky top-0 z-40 border-b border-slate-800/70">
        <div className="max-w-screen-2xl mx-auto px-4 sm:px-6 h-16 flex items-center gap-3 justify-between">

          {/* Logo */}
          <div className="flex items-center gap-2.5 shrink-0">
            <div className="w-8 h-8 rounded-lg flex items-center justify-center"
                 style={{ background: 'linear-gradient(135deg,#f97316,#ea580c)', boxShadow: '0 0 16px rgba(249,115,22,0.3)' }}>
              <Trophy className="w-4 h-4 text-white" />
            </div>
            <span className="text-lg font-bold hidden sm:block"
                  style={{ background: 'linear-gradient(90deg,#fff,#94a3b8)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
              Contest Predictor
            </span>
          </div>

          {/* Contest Selector Dropdown */}
          <div className="relative shrink-0" style={{ minWidth: 240 }}>
            <button
              onClick={() => setDropdownOpen(v => !v)}
              className="w-full flex items-center justify-between gap-2 px-4 py-2 rounded-xl text-sm font-semibold border transition-all"
              style={{ background: 'rgba(30,41,59,0.7)', borderColor: dropdownOpen ? 'rgba(249,115,22,0.5)' : 'rgba(51,65,85,0.7)', color: '#e2e8f0' }}>
              <div className="flex items-center gap-2">
                <Zap className="w-3.5 h-3.5 text-orange-400" />
                <span className="truncate max-w-[160px]">
                  {contestsLoading ? 'Loading contests...' : selectedContest?.title ?? 'Select Contest'}
                </span>
              </div>
              <ChevronDown className={cn("w-4 h-4 text-slate-400 transition-transform", dropdownOpen && "rotate-180")} />
            </button>

            <AnimatePresence>
              {dropdownOpen && (
                <motion.div
                  initial={{ opacity: 0, y: -8 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -8 }}
                  transition={{ duration: 0.15 }}
                  className="absolute top-full mt-2 left-0 right-0 z-50 rounded-xl border border-slate-700 overflow-hidden shadow-2xl"
                  style={{ background: 'rgba(15,23,42,0.98)', backdropFilter: 'blur(20px)' }}>
                  {contestsLoading
                    ? <div className="px-4 py-3 text-sm text-slate-500 flex items-center gap-2"><Loader2 className="w-3.5 h-3.5 animate-spin" /> Loading...</div>
                    : contests.map(c => (
                        <button key={c.slug} onClick={() => handleContestSelect(c)}
                                className={cn(
                                  "w-full text-left px-4 py-3 text-sm transition-colors hover:bg-slate-800/60",
                                  selectedContest?.slug === c.slug ? "text-orange-400 bg-orange-500/10" : "text-slate-300"
                                )}>
                          {c.title}
                        </button>
                      ))}
                </motion.div>
              )}
            </AnimatePresence>
          </div>

          {/* Search */}
          <div className="relative flex-1 max-w-lg group hidden md:block">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-500 group-focus-within:text-orange-400 transition-colors" />
            <input
              type="text" placeholder="Search username or rank..."
              value={search} onChange={e => setSearch(e.target.value)}
              className="w-full py-2 pl-10 pr-8 rounded-full text-sm text-slate-200 placeholder-slate-600
                         border border-slate-800 bg-slate-900/50 outline-none transition-all
                         focus:ring-1 focus:ring-orange-500/40 focus:border-orange-500/40" />
            {search && (
              <button onClick={() => setSearch('')} className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-500 hover:text-white">
                <X className="w-3.5 h-3.5" />
              </button>
            )}
          </div>

          {/* Right side */}
          <div className="flex items-center gap-2 shrink-0">
            <button onClick={refetch} disabled={isScraping || isLoading}
                    className="p-2 hover:bg-slate-800 rounded-full transition-colors" title="Refresh">
              <RefreshCw className={cn("w-4 h-4 text-slate-400", (isScraping || isLoading) && "animate-spin")} />
            </button>
            {/* Live/Offline badge */}
            <div className="hidden sm:flex items-center gap-1.5 text-[11px] font-mono px-2.5 py-1 rounded-full border"
                 style={{
                   background:   isLive ? 'rgba(16,185,129,0.1)' : isScraping ? 'rgba(251,191,36,0.1)' : 'rgba(244,63,94,0.1)',
                   borderColor:  isLive ? 'rgba(52,211,153,0.3)'  : isScraping ? 'rgba(251,191,36,0.3)'  : 'rgba(244,63,94,0.3)',
                   color:        isLive ? '#34d399'                : isScraping ? '#fbbf24'                : '#f43f5e',
                 }}>
              {isLive ? <Wifi className="w-3 h-3" /> : isScraping ? <Loader2 className="w-3 h-3 animate-spin" /> : <WifiOff className="w-3 h-3" />}
              <span>{isLive ? 'LIVE' : status === 'fetching_ratings' ? 'FETCHING RATINGS' : isScraping ? 'SCRAPING' : status === 'loading' ? 'LOADING' : 'OFFLINE'}</span>
            </div>
            <div className="w-8 h-8 rounded-full bg-slate-800 border border-slate-700 overflow-hidden cursor-pointer">
              <img src="https://api.dicebear.com/7.x/avataaars/svg?seed=Admin" alt="avatar" className="w-full h-full" />
            </div>
          </div>
        </div>
      </header>

      {/* ── Main ────────────────────────────────────────────────────────────── */}
      <main className="flex-1 max-w-screen-2xl mx-auto w-full px-4 sm:px-6 py-8">

        {/* Sub-header */}
        <div className="flex items-end justify-between mb-6">
          <div>
            <h2 className="text-2xl font-bold tracking-tight">High-Performance Data Table</h2>
            <p className="text-slate-500 text-sm mt-1">
              {isLoading  && 'Connecting to backend...'}
              {status === 'scraping' && <span className="text-amber-400">Turbo scraping in progress — {progress.pct}% ({progress.pages_done}/{progress.total_pages} pages)</span>}
              {status === 'fetching_ratings' && <span className="text-amber-400">JIT GraphQL Fetch in progress — resolving real baseline ratings...</span>}
              {isLive     && `${filtered.length.toLocaleString()} of ${meta.total.toLocaleString()} participants · ${meta.contest}`}
              {status === 'error' && <span className="text-rose-500">Error: {error}</span>}
            </p>
          </div>
          <div className="text-[10px] font-mono text-slate-600 uppercase tracking-widest
                          bg-slate-900/40 px-3 py-1 rounded-full border border-slate-800">
            V-SYNC: ENABLED
          </div>
        </div>

        {/* Progress bar — visible during scraping */}
        <AnimatePresence>
          {isScraping && (
            <motion.div initial={{ opacity: 0, y: -10 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0 }}
                        className="mb-6 rounded-2xl border border-amber-500/20 p-5"
                        style={{ background: 'rgba(251,191,36,0.05)' }}>
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <Loader2 className="w-4 h-4 text-amber-400 animate-spin" />
                  <span className="text-sm font-semibold text-amber-400">
                    {status === 'fetching_ratings' ? 'Resolving Real Ratings...' : 'Turbo Scraping in Progress'}
                  </span>
                </div>
                <span className="text-xs font-mono text-slate-500">
                  {status === 'fetching_ratings' ? (
                    'Dynamic Fetch'
                  ) : (
                    <>{progress.pages_done} / {progress.total_pages || '?'} pages · ETA ~{
                      progress.total_pages
                        ? Math.max(0, Math.round((progress.total_pages - progress.pages_done) * 0.65 / 60))
                        : '?'
                    } min</>
                  )}
                </span>
              </div>
              {/* Progress track */}
              <div className="w-full h-2 rounded-full bg-slate-800 overflow-hidden">
                <motion.div
                  animate={{ width: `${Math.max(2, progress.pct)}%` }}
                  transition={{ duration: 0.5, ease: 'easeOut' }}
                  className="h-full rounded-full"
                  style={{ background: 'linear-gradient(90deg, #f97316, #fbbf24)' }} />
              </div>
              <p className="text-xs text-slate-500 mt-2">
                {status === 'fetching_ratings' 
                  ? 'Packing users into JIT GraphQL batch requests (15 concurrently) to gather baseline ratings.'
                  : 'Using Turbo Stealth profile (12 concurrent · chrome120 impersonation). Results will auto-load when complete.'}
              </p>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Table card */}
        <div className="border border-slate-800 rounded-2xl overflow-hidden shadow-2xl"
             style={{ background: 'rgba(15,23,42,0.4)' }}>
          {(isLoading || status === 'idle') ? <SkeletonTable />
            : isScraping                    ? <ScrapingPlaceholder pct={progress.pct} />
            : status === 'error'            ? <ErrorState message={error} onRetry={refetch} />
            : filtered.length === 0         ? <EmptyState />
            : <LeaderboardTable users={filtered} onRowClick={handleRowClick} />
          }
        </div>
      </main>

      {/* ── Detail Sheet ────────────────────────────────────────────────────── */}
      <AnimatePresence>
        {isSheetOpen && selectedUser && (
          <UserDetailSheet user={selectedUser} totalParticipants={meta.total} onClose={() => setIsSheetOpen(false)} />
        )}
      </AnimatePresence>

      {/* Close dropdown on outside click */}
      {dropdownOpen && <div className="fixed inset-0 z-30" onClick={() => setDropdownOpen(false)} />}
    </div>
  );
};

// ─── Virtualized Table ────────────────────────────────────────────────────────
const COL = 'grid-cols-[70px_1fr_80px_130px_130px_140px_150px]';

const LeaderboardTable = ({ users, onRowClick }) => {
  const parentRef = useRef(null);
  const rowVirtualizer = useVirtualizer({
    count: users.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => 52,
    overscan: 12,
  });

  return (
    <div ref={parentRef} className="overflow-auto" style={{ maxHeight: '72vh' }}>
      <div style={{ minWidth: 800 }}>
        {/* Header */}
        <div className={cn('sticky top-0 z-10 grid gap-4 px-6 py-3 border-b border-slate-800 text-[11px] font-bold uppercase tracking-wider text-slate-500', COL)}
             style={{ background: 'rgba(15,23,42,0.97)', backdropFilter: 'blur(8px)' }}>
          <div>Rank</div><div>Username</div><div>Score</div>
          <div>Finish Time</div><div>Prev Rating</div><div>Delta</div><div>Pred Rating</div>
        </div>

        {/* Rows */}
        <div style={{ height: rowVirtualizer.getTotalSize(), position: 'relative' }}>
          {rowVirtualizer.getVirtualItems().map(vRow => {
            const u     = users[vRow.index];
            const isTop = u.global_rank <= TOP_RANK_N;
            const isPos = u.predicted_delta >= 0;
            return (
              <div key={vRow.key} data-index={vRow.index} ref={rowVirtualizer.measureElement}
                   onClick={() => onRowClick(u)}
                   className={cn('absolute top-0 left-0 w-full grid gap-4 px-6 py-3.5 border-b cursor-pointer transition-all group', COL,
                     isTop ? 'border-amber-500/30 hover:bg-amber-500/8' : 'border-slate-800/40 hover:bg-slate-800/25')}
                   style={{
                     transform: `translateY(${vRow.start}px)`,
                     background: isTop ? 'rgba(245,158,11,0.04)' : undefined,
                     boxShadow:  isTop ? 'inset 3px 0 0 0 rgba(251,191,36,0.65)' : undefined,
                   }}>
                {/* Rank */}
                <div className={cn('font-mono font-semibold text-sm flex items-center gap-1', isTop ? 'text-amber-400' : 'text-slate-500')}>
                  {isTop && <Trophy className="w-3 h-3" />}{u.global_rank}
                </div>
                {/* Username */}
                <div className="font-semibold text-sky-400 text-sm truncate group-hover:text-sky-300 transition-colors">{u.username}</div>
                {/* Score */}
                <div className="text-slate-300 font-mono text-sm">{u.score ?? '—'}</div>
                {/* Finish Time */}
                <div className="text-slate-400 font-mono text-sm">{fmtTime(u.finish_time)}</div>
                {/* Prev Rating */}
                <div className="text-slate-400 font-mono text-sm">{u.previous_rating.toFixed(0)}</div>
                {/* Delta */}
                <div className={cn('font-bold font-mono text-sm', isPos ? 'text-emerald-400' : 'text-rose-500')}
                     style={{ filter: 'drop-shadow(0 0 5px currentColor)', opacity: 0.95 }}>
                  {isPos ? `+${u.predicted_delta.toFixed(1)}` : u.predicted_delta.toFixed(1)}
                </div>
                {/* Predicted Rating */}
                <div className="font-bold font-mono text-sm text-amber-400"
                     style={{ filter: 'drop-shadow(0 0 6px rgba(251,191,36,0.45))' }}>
                  {u.predicted_rating.toFixed(0)}
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
};

// ─── Detail Sheet ─────────────────────────────────────────────────────────────
const UserDetailSheet = ({ user, totalParticipants, onClose }) => {
  const isPos = user.predicted_delta >= 0;
  return (
    <>
      <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
                  onClick={onClose} className="fixed inset-0 z-50"
                  style={{ background: 'rgba(2,6,23,0.65)', backdropFilter: 'blur(4px)' }} />
      <motion.div initial={{ x: '100%' }} animate={{ x: 0 }} exit={{ x: '100%' }}
                  transition={{ type: 'spring', damping: 28, stiffness: 220 }}
                  className="fixed right-0 top-0 h-full w-full max-w-sm z-50 flex flex-col border-l border-slate-800 shadow-2xl"
                  style={{ background: 'rgba(15,23,42,0.97)', backdropFilter: 'blur(20px)' }}>
        {/* Header */}
        <div className="p-5 border-b border-slate-800 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-14 h-14 rounded-xl bg-slate-800 border border-slate-700 p-0.5 overflow-hidden">
              <img src={`https://api.dicebear.com/7.x/avataaars/svg?seed=${user.username}`} alt={user.username} className="w-full h-full rounded-lg object-cover" />
            </div>
            <div>
              <h2 className="text-lg font-bold leading-tight">{user.username}</h2>
              <div className="flex items-center gap-1.5 mt-1 px-2 py-0.5 rounded-full w-fit text-xs font-bold uppercase tracking-wider"
                   style={{ background: 'rgba(251,191,36,0.1)', border: '1px solid rgba(251,191,36,0.2)', color: '#fbbf24' }}>
                <Trophy className="w-3 h-3" /> Global Rank: {user.global_rank}
              </div>
            </div>
          </div>
          <button onClick={onClose} className="p-2 hover:bg-slate-800 rounded-lg transition-colors group">
            <X className="w-5 h-5 text-slate-500 group-hover:text-white" />
          </button>
        </div>

        {/* Scrollable Content */}
        <div className="flex-1 overflow-auto p-5 space-y-6">
          {/* Information */}
          <section>
            <h3 className="text-[11px] font-bold text-slate-500 uppercase tracking-widest mb-3">Information</h3>
            <div className="grid grid-cols-3 gap-2.5">
              <MetricCard label="Previous Rating" value={user.previous_rating.toFixed(0)} />
              <MetricCard label="Predicted Delta"
                value={(isPos ? '+' : '') + user.predicted_delta.toFixed(1)}
                color={isPos ? '#34d399' : '#f43f5e'}
                glow={isPos ? 'rgba(52,211,153,0.25)' : 'rgba(244,63,94,0.25)'}
                bgCol={isPos ? 'rgba(16,185,129,0.08)' : 'rgba(244,63,94,0.08)'}
                border={isPos ? 'rgba(52,211,153,0.25)' : 'rgba(244,63,94,0.25)'} />
              <MetricCard label="New Predicted Rating" value={user.predicted_rating.toFixed(0)}
                color="#fbbf24" glow="rgba(251,191,36,0.3)" />
            </div>
          </section>

          {/* Calculation Context */}
          <section className="rounded-xl p-4 relative overflow-hidden"
                   style={{ background: 'rgba(15,23,42,0.6)', border: '1px solid rgba(51,65,85,0.6)' }}>
            <div className="absolute top-3 right-3 opacity-5 pointer-events-none">
              <Target className="w-16 h-16 text-orange-400" />
            </div>
            <div className="flex items-center gap-2 mb-2">
              <Info className="w-3.5 h-3.5 text-orange-400" />
              <h3 className="text-[11px] font-bold text-white uppercase tracking-wider">Calculation Context</h3>
            </div>
            <p className="text-slate-400 text-xs leading-relaxed">
              <span className="font-semibold text-slate-200">Performance / Seed:</span>{' '}
              Algorithmically calculated expected rank based on{' '}
              <span className="text-orange-400 font-mono">{totalParticipants.toLocaleString()}</span>{' '}
              participants using Histogram Interpolation Elo-MMR.
            </p>
          </section>

          {/* Contest Details */}
          <section className="relative">
            <h3 className="text-[11px] font-bold text-slate-500 uppercase tracking-widest mb-3">Contest Details</h3>
            <div className="rounded-xl overflow-hidden border border-slate-800 divide-y divide-slate-800/50"
                 style={{ background: 'rgba(15,23,42,0.5)' }}>
              <DetailRow label="Final Score:" value={user.score ?? '—'} />
              <DetailRow label="Finish Time:" value={fmtTime(user.finish_time)} />
              <DetailRow label="Penalty details:" value={0} />
              <DetailRow label="Penalty details details:" value={3} />
            </div>
            <div className="absolute -bottom-3 -right-1 opacity-20 pointer-events-none">
              <svg viewBox="0 0 24 24" className="w-10 h-10 fill-slate-500">
                <path d="M12 2L14.5 9.5L22 12L14.5 14.5L12 22L9.5 14.5L2 12L9.5 9.5Z" />
              </svg>
            </div>
          </section>
        </div>

        {/* Footer */}
        <div className="p-5 border-t border-slate-800">
          <button className="w-full py-2.5 rounded-xl font-bold text-sm text-white transition-all active:scale-95"
                  style={{ background: 'linear-gradient(135deg,#f97316,#ea580c)', boxShadow: '0 4px 20px rgba(249,115,22,0.25)' }}>
            Full Contest Analysis
          </button>
        </div>
      </motion.div>
    </>
  );
};

// ─── Helper Components ────────────────────────────────────────────────────────
const MetricCard = ({ label, value, color, glow, bgCol, border }) => (
  <div className="rounded-xl p-3 flex flex-col items-center text-center h-24 justify-between transition-transform hover:scale-105"
       style={{ background: bgCol || 'rgba(30,41,59,0.5)', border: `1px solid ${border || 'rgba(51,65,85,0.5)'}` }}>
    <span className="text-[10px] text-slate-400 font-medium leading-tight">{label}</span>
    <span className="text-2xl font-bold"
          style={{ color: color || '#fff', filter: glow ? `drop-shadow(0 0 8px ${glow})` : undefined }}>
      {value}
    </span>
  </div>
);

const DetailRow = ({ label, value }) => (
  <div className="flex items-center justify-between px-4 py-3 hover:bg-white/5 transition-colors">
    <span className="text-sm font-semibold text-slate-300">{label}</span>
    <span className="text-sm font-bold font-mono text-slate-100">{value}</span>
  </div>
);

const SkeletonTable = () => (
  <div className="p-6 space-y-3 animate-pulse">
    {[...Array(14)].map((_, i) => (
      <div key={i} className="h-10 rounded-xl bg-slate-800/50" style={{ opacity: 1 - i * 0.05 }} />
    ))}
  </div>
);

const ScrapingPlaceholder = ({ pct }) => (
  <div className="flex flex-col items-center justify-center py-24 gap-5">
    <div className="relative w-20 h-20">
      <svg className="w-full h-full -rotate-90" viewBox="0 0 36 36">
        <circle cx="18" cy="18" r="15" fill="none" stroke="rgba(51,65,85,0.5)" strokeWidth="3" />
        <circle cx="18" cy="18" r="15" fill="none" stroke="#f97316" strokeWidth="3"
                strokeDasharray={`${pct * 0.942} 94.2`} strokeLinecap="round" />
      </svg>
      <Loader2 className="absolute inset-0 m-auto w-8 h-8 text-orange-400 animate-spin" />
    </div>
    <div className="text-center">
      <h3 className="text-lg font-bold text-amber-400">{pct.toFixed(0)}% complete</h3>
      <p className="text-slate-500 text-sm mt-1">Turbo scraping leaderboard data from LeetCode...</p>
      <p className="text-slate-600 text-xs mt-1">Results will appear automatically when done</p>
    </div>
  </div>
);

const EmptyState = () => (
  <div className="flex flex-col items-center justify-center py-24 gap-4">
    <div className="w-14 h-14 rounded-full bg-slate-800/50 flex items-center justify-center">
      <Search className="w-6 h-6 text-slate-600" />
    </div>
    <h3 className="text-base font-semibold">No users found</h3>
    <p className="text-slate-500 text-sm">Try a different username or rank number.</p>
  </div>
);

const ErrorState = ({ message, onRetry }) => (
  <div className="flex flex-col items-center justify-center py-24 gap-4">
    <div className="w-14 h-14 rounded-full flex items-center justify-center"
         style={{ background: 'rgba(244,63,94,0.1)', border: '1px solid rgba(244,63,94,0.2)' }}>
      <WifiOff className="w-6 h-6 text-rose-500" />
    </div>
    <div className="text-center">
      <h3 className="text-base font-semibold text-rose-400">Cannot reach API</h3>
      <p className="text-slate-500 text-sm mt-1 font-mono">{message}</p>
    </div>
    <button onClick={onRetry}
            className="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-semibold border border-slate-700 text-slate-300 hover:bg-slate-800 transition-colors">
      <RefreshCw className="w-4 h-4" /> Retry
    </button>
  </div>
);

export default App;
