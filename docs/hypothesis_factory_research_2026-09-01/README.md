# Hypothesis factory research (2026-09-01)

Question: should a local LLM continuously generate and validate strategy
hypotheses, and how do we build that factory?

Verdict: yes - and half the factory already exists
(`scripts/strategy_research_loop.py`: LM Studio client, staged fail-closed
funnel). The factory is an upgrade of that loop, not greenfield. The LLM's
role is narrow (schema-constrained mutation/recombination); the moat is the
mechanical evaluator + statistics.

Reports:
1. `01_repo_machinery_inventory.md` - every harness/gate/registry part we
   already have, with exact CLI commands.
2. `02_llm_evolution_landscape.md` - FunSearch/AlphaEvolve/ShinkaEvolve/EoH,
   trading-specific factories, documented pathologies + mitigations.
3. `03_local_llm_and_statistics.md` - what runs on the M5/16GB Mac, honest
   local-vs-API comparison, and the statistical spine (trial ledger,
   Sidak/FDR/e-value gates, planted-oracle test, champion-challenger racing).

Roadmap (phases): 1) statistical spine (trial ledger + planted oracle +
e-value gates) -> 2) generator upgrade (EoH operators, killed-registry as
negative prompt, novelty rejection) -> 3) closed loop on the Mac ->
4) shadow racing for the single live slot. Human sign-off stays on promotion.

User-facing synthesis (Russian): Claude artifact "Фабрика гипотез".
