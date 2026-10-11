## Architecture
- All data access goes through `src/api/endpoints.ts` (HTTP/JSON client) and hooks in `src/api/queries.ts`; components never call fetch directly, so the backend can be swapped without UI changes.
- Data routes set `ssr: false` because live timestamps are time-dependent and would mismatch on hydration.
- UI preferences live in `src/lib/prefs.ts` (useSyncExternalStore + localStorage) instead of zustand to avoid an extra dependency.
- Status colors use `tone-*` utilities and `success/warning/info` tokens in `src/styles.css`, never raw Tailwind palette classes.
