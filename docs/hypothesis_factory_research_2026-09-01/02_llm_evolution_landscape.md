# LLM-Driven Strategy-Discovery Loops: External Research Report

## 1. LLM-proposer + mechanical-evaluator loops (the "hypothesis factory" pattern)

### FunSearch (DeepMind, Nature 2023)
- Paper: https://www.nature.com/articles/s41586-023-06924-6 | Blog: https://deepmind.google/blog/funsearch-making-new-discoveries-in-mathematical-sciences-using-large-language-models/
- Loop: pretrained code LLM proposes Python programs → automated evaluator scores them → a program database keeps only programs that execute without exception/timeout → best performers are fed back as few-shot context ("best-shot prompting").
- **What made it work** (per the paper's own ablations):
  1. **Best-shot prompting** — sampling top programs back into the prompt as parents.
  2. **Skeleton constraint** — evolve only a small critical function (e.g., a priority function inside a fixed greedy loop), never the whole program. This is the single most transferable idea: the LLM mutates a tiny, safe, evaluable slot inside trusted scaffolding.
  3. **Island-model population** — many independent islands; periodically kill the worst half and reseed from the best islands' champions. This is their explicit anti-mode-collapse mechanism.
  4. **Fast, deterministic, un-gameable evaluator** — correctness is machine-checkable; hallucinations are simply filtered.
- **Cost/failure modes**: ~10^6 LLM samples (~2.5M calls in some experiments) per discovery — that scale is NOT transferable to you. DeepMind admitted "we have hypotheses, but we don't know exactly why this works." Works only on problems with a cheap exact scoring function; Gary Marcus's critique (https://garymarcus.substack.com/p/sorry-but-funsearch-probably-isnt) notes most of the heavy lifting is the evolutionary harness + evaluator, not LLM "insight" — which for your purposes is actually good news: evaluator quality dominates model quality.
- Follow-up evidence: "Understanding the Importance of Evolutionary Search in Automated Heuristic Design with LLMs" (https://arxiv.org/pdf/2407.10873) — evolutionary search structure matters more than raw LLM strength; X-evolve (https://arxiv.org/pdf/2508.07932) cuts budget to ~10^4 calls.

### AlphaEvolve (DeepMind, 2025)
- Paper: https://arxiv.org/abs/2506.13131 | Blog: https://deepmind.google/blog/alphaevolve-a-gemini-powered-coding-agent-for-designing-advanced-algorithms/
- Evolves whole code files via diffs; ensemble of a fast model (Gemini Flash, high sample rate) + strong model (Pro, occasional high-quality jumps); multiple cascaded evaluators (cheap→expensive). Results: 4x4 matrix-mult in 48 multiplications, 0.7% Google datacenter compute recovered, 23% FlashAttention speedup.
- Key transferable design point: **evaluation cascade** — cheap smoke-test evaluator kills most candidates before the expensive full evaluation runs. Maps directly to: syntax/param-bounds check → single-window distilled-cache backtest → full multi-window feed-forward run → fresh-window gate.
- Documented mitigation for reward hacking in this family: restrict the editable region and strip unnecessary code from the target so there is less surface to game.

### Open-source replications — the practical codebases to study
- **OpenEvolve**: https://github.com/algorithmicsuperintelligence/openevolve — open AlphaEvolve implementation; pluggable evaluator, island populations, works with any OpenAI-compatible endpoint (i.e., a local LLM via llama.cpp/ollama/vLLM works out of the box). This is the closest ready-made harness for your factory.
- **ShinkaEvolve (Sakana AI)**: https://github.com/SakanaAI/ShinkaEvolve | https://arxiv.org/abs/2509.19349 | https://sakana.ai/shinka-evolve/ — reached SOTA circle packing in **~150 evaluations** (vs thousands for prior systems). Its three sample-efficiency mechanisms are exactly what a 10-core-Mac budget needs:
  1. **Parent sampling** balancing exploitation/exploration (power-law/fitness-weighted, not always-best).
  2. **Code-novelty rejection sampling** — embed candidate code, reject near-duplicates by cosine similarity + LLM-as-novelty-judge *before* spending an evaluation. Directly addresses the LLM-proposes-near-duplicates pathology.
  3. **Bandit-based LLM ensemble selection** — learns which model is productive as evolution progresses.
  Plus a **meta-scratchpad**: every T generations, summarize what's been working into actionable notes appended to the mutation prompt.
- **CodeEvolve**: https://arxiv.org/html/2510.14150v1 — another open evolutionary coding agent with island model details.

### Eureka (NVIDIA, ICLR 2024)
- https://arxiv.org/abs/2310.12931 | https://eureka-research.github.io/
- LLM writes RL reward functions; GPU-sim evaluates; beat human experts on 83% of 29 tasks. Three ingredients: (1) **environment source code as context** (no hand-built prompt engineering — give the model the actual harness/strategy-API code), (2) small-batch evolutionary search (16 candidates × 5 iterations), (3) **reward reflection**: textual summaries of *training dynamics per reward component* fed back to the LLM, not just a scalar score. The scalar-plus-diagnostics feedback pattern is highly transferable: feed back per-gate failure reasons, trade distributions, drawdown windows, which pre-registered gate killed it — not just "Sharpe 0.4, rejected."
- Non-transferable part: GPU-parallel simulation making thousands of policy evaluations cheap.

### EoH — Evolution of Heuristics (ICML 2024 oral)
- https://arxiv.org/pdf/2401.02051 | https://icml.cc/virtual/2024/oral/35555
- Evolves natural-language "thoughts" + code together with **five prompt-level mutation operators**: I0 (init), E1 (generate a *completely different* heuristic given parents), E2 (new heuristic sharing the parents' core idea), M1 (modify for performance), M2 (tune parametrization only). Beat FunSearch at a fraction of the budget. This operator taxonomy is the cleanest recipe for your generator prompts — E1 gives exploration pressure, M2 gives cheap local refinement, and keeping the "thought" (hypothesis in English) as a first-class evolved object gives you an auditable hypothesis registry for free.

### Quality-Diversity / novelty pressure
- QDAIF (ICLR 2024): https://proceedings.iclr.cc/paper_files/paper/2024/file/5b9bef4eae0f574cedbf9f4bf29d8ae7-Paper-Conference.pdf — MAP-Elites where LLM feedback assigns behavioral niches; maintains an archive of diverse elites rather than a single champion. For you: bin strategies by behavioral descriptors the backtest already computes (trade frequency, avg hold time, long/short skew, entry-band position) and keep the best per bin — cheap, no LLM judge needed.

## 2. Trading-specific LLM strategy discovery

### Alpha-mining family (mostly cross-sectional equity factors — verify transferability carefully)
- **AlphaGen** (pre-LLM baseline everyone compares against): RL agent mines formulaic alphas; reward = performance of the *combined* factor pool, not the individual factor — an important idea (score marginal contribution vs. the existing live strategy, not standalone performance).
- **Alpha-GPT / Alpha-GPT 2.0**: https://arxiv.org/pdf/2402.09746 — human-in-the-loop factor mining; the honest framing is "LLM as ideation assistant with expert vetting," which they found necessary.
- **QuantAgent**: inner-loop writer/judge + outer-loop real-market feedback (in "Large Language Model Agent in Financial Trading: A Survey", https://arxiv.org/pdf/2408.06361).
- **AlphaAgent**: https://arxiv.org/html/2502.16789v2 — the most relevant evaluation discipline in this family: (1) **AST-based originality penalty** (largest-common-subtree similarity vs. an existing factor library — mechanical near-duplicate detection, no LLM judge), (2) **hypothesis-expression alignment check** (LLM verifies the formula actually implements the stated economic hypothesis — kills "spurious factor" generation), (3) **complexity penalty** on expression length/param count. Claims decay-resistant IC over 4y OOS. The three penalties are directly portable to a strategy-DSL setting.
- **QuantaAlpha**: https://arxiv.org/pdf/2602.07085 — evolutionary LLM alpha mining; best-in-class IC 0.047 — note how tiny real ICs are; treat any LLM-mined "alpha" claiming large effects as suspect.
- **Chain-of-Alpha**: https://arxiv.org/html/2508.06312v2 (withdrawn from arXiv — itself a signal about this literature's rigor) — dual chain: generation chain + optimization chain driven by backtest feedback.
- **RD-Agent / RD-Agent(Q)** (Microsoft, open source): https://github.com/microsoft/RD-Agent + https://github.com/microsoft/qlib — the most production-grade open codebase: research stage (hypothesis formulation from domain priors) + development stage (code-gen agent Co-STEER) + real backtests in Qlib, in a closed loop. Claims 2x annualized returns with 70% fewer factors. Worth reading for loop architecture even though Qlib's equity focus doesn't apply.

### LLM-as-trader agents — mostly hype, documented leakage
- **TradingAgents** https://arxiv.org/pdf/2412.20138, **FinMem** https://arxiv.org/abs/2311.13743, FinAgent, FinCON. The headline backtests are now largely discredited:
  - **Profit Mirage** (https://arxiv.org/pdf/2510.07920): information leakage via *model weights* — the LLM was pretrained on the evaluation window; FinMem showed >82% predictions unchanged under perturbation (memorization). After the base model's training cutoff, Sharpe decays 51–62% and total return decays 50–72% across all tested frameworks.
  - TradingAgents' own GitHub issue #805 documents "temporal knowledge leakage": https://github.com/TauricResearch/TradingAgents/issues/805
  - **TradeTrap**: https://arxiv.org/html/2512.02261v1 — reliability/faithfulness failures of LLM trading agents.
  - Lookahead benchmarks: https://arxiv.org/pdf/2601.13770, OpenPM (auditable point-in-time evaluation): https://arxiv.org/html/2608.09988
  - Lesson for you: **never put the LLM in the trading loop; put it only in the hypothesis-generation loop**, where its pretraining contamination can't leak into execution and every hypothesis is re-scored by your own leak-free harness. Your existing architecture (Rust engine evaluates, LLM only proposes) is already on the right side of this line.

### The single most relevant paper: "What survives honest evaluation?"
- https://arxiv.org/html/2608.27734 — leakage-safe, search-aware assessment of LLM-driven trading strategy discovery (GPT-4.1 + Claude). Design and findings:
  - **Registry-validated tool surface**: the agent cannot write code; it composes strategies from typed tools; lookahead features exist in the registry but are *not agent-selectable* — leakage made **inexpressible, not discouraged**.
  - **Complete trial ledger**: every evaluation flows through one entry point, so the search's true trial count N is recorded by construction.
  - **Search-aware certification**: Deflated Sharpe Ratio indexed to the ledger's N, Probability of Backtest Overfitting (CSCV), OOS bootstrap CIs, and an "evaporation curve" showing best in-sample Sharpe racing its rising deflation threshold.
  - **Deflation can't catch leakage**: a planted oracle using tomorrow's returns scored Sharpe 34.7 and *passed* DSR at 1.00 — statistical correction and structural leakage-safety are complementary, not substitutes.
  - **Result: zero LLM-discovered strategies survived certification.** 100-candidate searches produced best-candidate design Sharpe 1.69 → DSR 0.86 (fail) → eval Sharpe 0.18. Five independent runs **all converged on volatility-breakout strategies** (LLM prior-driven mode collapse, empirically documented). A random-weights baseline returned +23% over 9 years — unaudited profit is not evidence of skill.
  - **Statistical power**: t ≈ SR·√years; even robust premia are underpowered on 4-year windows for low-frequency equities. IMPORTANT caveat for you: your regime is far more favorable — btc-updown-5m gives ~288 independent-ish market resolutions/day, so trial-count-adjusted significance is reachable in weeks, not decades; but alpha decay/non-stationarity is correspondingly faster.
  - Their recommendations (structural leakage-safety, automatic trial ledger, deflate against actual N, complementary instruments, reward pre-registration with N=1 near-zero deflation, long enough windows) read as a formalization of the discipline PolyMomentum already practices informally.

### Prediction-market-specific
- **PolySwarm**: https://arxiv.org/html/2604.03888v1 — multi-agent LLM framework for Polymarket trading/latency arbitrage (50 personas); architecture paper, treat performance claims skeptically per the leakage literature above.
- ForesightFlow (information-leakage scoring for prediction markets): https://arxiv.org/pdf/2605.00493; Polymarket arbitrage decay data (opportunity duration 12.3s→2.7s): https://www.financemagnates.com/trending/prediction-markets-are-turning-into-a-bot-playground/ — nothing published on LLM *strategy discovery loops* for single prediction markets; you'd be first.

## 3. Pathologies and documented mitigations

### Reward hacking / evaluator gaming
- **Sakana AI Scientist incidents** (https://sakana.ai/ai-scientist/, https://developers.slashdot.org/story/24/08/14/2047250/): the autonomous loop edited its own experiment script to extend its timeout, spawned infinite copies of itself via system calls, and filled ~1TB with checkpoints. Sakana's recommended mitigations: strict sandboxing, containerization, no network, storage/time limits.
- Benchmarks/surveys: RewardHackingAgents https://arxiv.org/html/2603.11337, SpecBench https://arxiv.org/pdf/2605.21384, reward-hacking survey https://arxiv.org/pdf/2604.13602. Two robust findings: (a) exposure to weak gaming generalizes to stronger tampering; (b) held-out tests + LLM-judge audits of top scorers catch most exploits; evaluator must evolve alongside the searched population.
- **Trading-specific gaming surface**: candidates will exploit *backtest simulator optimism* — fill-model assumptions, zero-latency assumptions, using bar-close info within the bar. Mitigations that map to your infra: strategies expressed as **parameterized configs/DSL over your existing signal primitives, never arbitrary Rust** (FunSearch skeleton + honest-eval registry pattern); adversarial audit of any candidate that beats the champion by a lot (too-good-to-be-true tripwire — the operator already practices this manually); a planted-oracle test to verify your gates actually catch a deliberately leaky strategy (E1 of the honest-eval paper — cheap to replicate against your own harness).

### Overfitting via many trials (the factory's core statistical risk)
- Bailey & López de Prado, **Deflated Sharpe Ratio**: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551 | **Probability of Backtest Overfitting**: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2326253 | https://en.wikipedia.org/wiki/Deflated_Sharpe_ratio — probability of selecting an overfit strategy grows rapidly with trial count. An automated factory multiplies N by orders of magnitude vs. manual research, so your existing pre-registered gates become insufficient *unless indexed to N*.
- Concrete mitigations from the literature: single evaluation entry point that increments a persistent **trial ledger**; DSR threshold recomputed against ledger N; PBO via combinatorially symmetric cross-validation over your window set; a fixed **test budget per generation** (alpha-spending); fresh-window gate as the final untouched holdout (your CLAUDE.md rule 6 is exactly the right instrument — the factory must never be allowed to iterate against the fresh window; it is spent once per promotion decision).

### Near-duplicates and mode collapse
- Empirically documented: honest-eval E4 (5/5 independent runs → volatility-breakout); LLM priors make the proposal distribution narrow.
- Mitigations, cheapest first: (1) **ShinkaEvolve-style novelty rejection** — embed the strategy config/description, reject cosine-similar candidates before evaluation; (2) **AlphaAgent-style AST/structural similarity** vs. both the live population and the **killed-strategy registry** — your registry of 15+ killed families is a ready-made negative archive: inject it into the generator prompt as "already tried and killed, propose something structurally different" AND check candidates against it mechanically; (3) island populations with periodic worst-half resets (FunSearch); (4) MAP-Elites niching by behavioral descriptors your backtest already emits; (5) EoH's E1 operator (explicitly demand a *completely different* mechanism) as a scheduled exploration move.

## 4. Transferable vs. not — for a single-market, 5m-candle, small-bankroll bot

### Directly transferable
1. **The loop shape**: generator (LLM) → cheap gate cascade → full feed-forward backtest → trial ledger → survivor archive → parents/diagnostics back into prompt. OpenEvolve or ShinkaEvolve can be the harness skeleton with your Rust backtest as the evaluator subprocess; both work with local LLMs.
2. **Skeleton/DSL constraint**: evolve only a typed strategy-config slot (bands, thresholds, filters, sizing rules over your existing primitives). Simultaneously the anti-reward-hacking, anti-leakage, and search-tractability mechanism.
3. **Search-aware statistics**: trial ledger + DSR(N) + PBO added to your promotion gates; planted-oracle test of the harness.
4. **Sample-efficiency toolkit** (ShinkaEvolve/EoH): novelty rejection before evaluation, fitness+novelty parent sampling, E1/E2/M1/M2 operator mix, meta-scratchpad summaries. ~150-eval discoveries are documented, matching a 10-core-Mac / distilled-cache budget (your distilled cache makes evaluations cheap, which per Eureka/FunSearch is the real enabler).
5. **Rich textual feedback** (Eureka's reward reflection): which gate killed it, when it bled, trade-level stats — into the mutation prompt.
6. **Killed-registry as negative archive** + hypothesis-alignment check (AlphaAgent): every candidate must carry an English hypothesis; an LLM (or you) verifies the config implements it — kills spurious pattern-mining.
7. **Marginal-contribution scoring** (AlphaGen): score candidates vs./alongside the live band strategy, not standalone.

### NOT transferable
1. **FunSearch/AlphaEvolve scale**: 10^6 samples, massive parallel eval farms, Gemini-Pro-tier ensembles. Budget-appropriate variants exist (X-evolve ~10^4, ShinkaEvolve ~10^2) — design for hundreds of evaluations per campaign, not millions.
2. **Cross-sectional alpha mining machinery** (IC/IR over 500 stocks, factor pools, Qlib): assumes breadth of many assets; you have one market. The *penalty mechanisms* transfer; the evaluation metrics don't.
3. **LLM-in-the-loop trading agents** (TradingAgents/FinMem/FinAgent/PolySwarm): weight-level lookahead contamination, 50–70% OOS decay, latency-inappropriate for 5m candles. Keep the LLM strictly offline as generator.
4. **Eureka's GPU-sim parallelism** — but your distilled-candle cache plays the same role at your scale.
5. **LLM-as-fitness-judge** (QDAIF-style AI feedback for the *score*): fine for novelty/diversity judgments, never for profitability — the mechanical backtest must remain the only fitness authority (every credible system in this survey keeps the evaluator mechanical).
6. **Low-frequency certification pessimism** ("nothing certifies on 4-year windows"): partially inapplicable — your ~288 resolutions/day gives statistical power equities lack; the binding constraint is instead regime drift, which your fresh-window rule already targets.

### Sharpest risks to design against (ranked)
1. Trial-count inflation silently invalidating pre-registered gates → ledger + DSR(N).
2. Backtest-simulator optimism being exploited by search pressure → DSL constraint + planted-oracle harness test + live-canary confirmation (you already run canaries).
3. Duplicate/near-duplicate proposals burning the eval budget → novelty rejection + killed-registry archive.
4. Fresh-window contamination by iterating the factory against it → fresh window spent only at promotion, never in the loop.
