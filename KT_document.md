# Cipher_DSG Knowledge Transfer (KT) Document

This document provides a comprehensive, sequential, and highly detailed Knowledge Transfer for the `cipher_dsg` codebase.

## 1. High-Level Architecture Flow
The `cipher_dsg` application appears to be an agentic or automated pipeline for ingesting, parsing, and managing "Dossiers" (regulatory/product documents) into a structured format (SQL) and a Knowledge Graph (Neo4j), powered by LLMs for intelligence and embeddings for retrieval.

Key flows identified:
- **Configuration & Utilities**: Single source of truth for settings ([config/settings.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/config/settings.py)), logger ([utils/logger.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/utils/logger.py)), and a central registry of expected dossiers ([config/dossier_registry.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/config/dossier_registry.py)).
- **Data Layer (SQL & Pipeline Orchestration)**: Handles interactions with a relational database, change detection, and orchestrates the complex mapping of changed concepts to dossier sections.
- **Graph Layer (Neo4j)**: Stores data in a knowledge graph for semantic traversal.
- **LLM/Embeddings Layer**: Generates vectors and processes content using OpenAI/Azure models.
- **Parsers**: Extracts data from PDFs and segments them into profiles.
- **Agent Workflow & Orchestrators**: Puts the puzzle pieces into loops—a manual build loop and a realtime monitoring CLI agent loop.

---

## 2. Configuration & Utilities (`config/`, `utils/`)

### [config/settings.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/config/settings.py)
- Uses `dotenv` to load [.env](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/.env) from the project root. Exposes Neo4j credentials, Embedding config (local vs openai API), Paths, and Logging config.

### [config/dossier_registry.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/config/dossier_registry.py)
- Contains `DOSSIER_REGISTRY` mapping PDF filenames mapping to ground-truth product metadata.

### [utils/logger.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/utils/logger.py)
- Standard Python logging configured with console and file handlers.

---

## 3. Data Layer & Change Pipeline (`db/`)

The `db/` component is much more than a database client; it is the orchestrator of an intelligent 8-phase pipeline that detects raw SQL changes, interprets them into concepts, maps them to document sections, and determines if new sections are needed or existing ones should be modified.

### [db/sql_client.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/db/sql_client.py) & [db/poller.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/db/poller.py)
- Manages robust, pooled connections to SQL Server. Polls the database for pending changes from `ProductChangeLog` on a continuous loop, parsing results into [ChangeBundle](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/models.py#303-325)s per product.

### [db/change_pipeline.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/db/change_pipeline.py) (The Master Orchestrator)
- Orchestrates Phases 2 through 7. Passes a [ChangeBundle](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/models.py#303-325) to LLM concept extractors, section mappers, situation analyzers, reference finders, and plan builders to output a list of generation-ready updates.

### Intelligence Implementations in DB Layer:
- **[situation_analyzer.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/db/situation_analyzer.py)**: Determines if format holds (`SAME_PATTERN`) or needs structural changes (`NEW_PATTERN`) by pushing context to LLMs.
- **[reference_finder.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/db/reference_finder.py)**: Looks up Top-K candidate reference formats from OTHER dossiers using Graph & embeddings, and uses an LLM to decide on the best one.
- **[plan_builder.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/db/plan_builder.py)**: Combines situation analysis, Neo4j hierarchy context, and cross-dossier templates into structured instructions [SectionUpdatePlan](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/models.py#364-422).
- **[dossier_injector.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/db/dossier_injector.py)**: Automatically versions node updates, manages siblings renumbering maps dynamically, and writes updates.

---

## 4. Knowledge Graph Layer ([graph/](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/agent/workflow.py#122-162))

The Neo4j layer stores Dossiers structurally (Products mapped to Dossiers mapped to Sections). It embeds hierarchical dependencies and vector-based text retrieval without building excessive metadata noise in nodes.

### [graph/neo4j_client.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/graph/neo4j_client.py)
- **Goal**: Thread-safe Neo4j connection pool wrapper.
- **Logic**: Handles lifecycle initialization using settings. Exposes read ([run_query](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/graph/neo4j_client.py#69-78)) and auto-commit writes ([run_auto_commit](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/graph/neo4j_client.py#79-89)), as well as transaction batch executions.

### [graph/neo4j_schema.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/graph/neo4j_schema.py)
- **Goal**: Defines the Cypher queries for constraints, indexes, and merges.
- **Logic**: Schema features product/dossier/section node definitions, parent/child relationships. Crucially, it manages **Semantic Vector Indexes** (`idx_semantic_embedding` and `idx_section_embedding` of dim 1536) for LLM-driven similarities and Fulltext indices. 

### [graph/graph_loader.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/graph/graph_loader.py)
- **Goal**: Pushes newly extracted PDF data (`ParsedDossier` models) into the Neo4j schema.
- **Logic**: Simple and direct. Creates nodes for Product and DossierVersion, then iterates through all parsed sections to embed texts (optional) and populate content graph nodes, completing it with hierarchical edge creations (`HAS_CHILD` etc.).

### [graph/update_storage.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/graph/update_storage.py)
- **Goal**: Store generated updates and maintain strict version control (Phase 7 execution).
- **Logic**: Implements a `SectionVersion` node linked via `HAS_VERSION` to track "who" (agent vs human), "why" (confidence, strategy, comment), and "what" changed in the text, preserving old state as history in the graph.

---

## 5. Embeddings & LLM Service Layer (`embeddings/`, [llm/](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/azure_client.py#284-290))

This layer abstracts text generation and vector mathematics, acting as the brain behind the change pipeline. Note: ALL parsing logic relies heavily on [ask_structured_pydantic](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/azure_client.py#201-278) (OpenAI Structured Outputs functionality) to enforce valid JSON structures and types.

### [embeddings/embedder.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/embeddings/embedder.py)
- **Goal**: Create text embeddings for semantic search capabilities.
- **Logic**: Implements [EmbedderProtocol](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/embeddings/embedder.py#35-41). Exposes `"local"` (determinstic SHA-256 hash pseudo-embeddings, 384 dims, zero-cost, used for fast structure tests) and `"openai"` / `"azure"` (real Azure OpenAI models, 1536 dims). 

### [llm/azure_client.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/azure_client.py)
- **Goal**: GPT-4o client.
- **Logic**: Uses exponential backoff for rate limiting and provides powerful [ask_structured_pydantic](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/azure_client.py#201-278) abstractions to guarantee strict output schemas for reasoning steps.

### [llm/change_interpreter.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/change_interpreter.py)
- **Goal**: Translates SQL technical columns to business concepts (Phase 3).
- **Logic**: Crucially it groups DB operations by time bucket + table, and interprets them as a unit. For instance, creating "Lead" + "Heavy Metal classification" + "Max limit" in three columns becomes ONE concept "heavy metal monitoring" rather than three disparate concepts.

### [llm/section_mapper.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/section_mapper.py)
- **Goal**: 100% LLM-driven section mapping (NO HARCODED RULES) (Phase 4).
- **Logic**: It queries Neo4j for ALL sections for a product (and references from other products) and feeds their FULL TEXT to the LLM alongside the detected concept. The LLM determines the `update_type` (modify, append, replace, remove, create).

### [llm/section_intelligence.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/section_intelligence.py)
- **Goal**: Central brain that discovers graphs and generates hierarchy.
- **Logic**:
    1. **[find_reference_section](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/section_intelligence.py#97-165)**: Performs semantic search for reference formats across products, then has LLM evaluate the top 10 search candidates to pick the best template. Emits [SectionReference](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/section_intelligence.py#48-65).
    2. **[decide_section_placement](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/section_intelligence.py#451-586)**: Takes the target product's hierarchy, the reference section hierarchy, and uses the LLM to decide where the new section should go, outputting a mapping scheme to re-order/renumber `siblings` (`renumber_plan`). Emits [SectionPlacement](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/section_intelligence.py#67-79).

### [llm/content_generator.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/content_generator.py)
- **Goal**: Phase 9 (Post-evaluation Generation). Generate actual Markdown content replacing target data in the reference template.
- **Logic**: Takes [SectionUpdatePlan](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/models.py#364-422) from the orchestrator and uses the LLM to merge Target situation data while strictly matching the Format Style.

---

## 6. Parsing Layer (`parsers/`)

Contains tools for turning PDF dossiers into intelligent structured models.

### [parsers/pdf_extractor.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/pdf_extractor.py)
- **Goal**: Baseline page parsing via `pdfplumber`.
- **Logic**: Converts pages to strings, drops signature pages or pure Table of Contents pages, and runs logical table extraction (turning cells into headers and rows). Extracts flat rows for the text embeddings component.

### [parsers/dossier_parser.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/dossier_parser.py)
- **Goal**: Breaks PDFs sequentially into logical "Sections" based on standard headings.
- **Logic**: Employs Regex heuristics (`2.2.1 ...`) to find content boundaries. Iterates each span, calls up [_llm_detect_table](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/dossier_parser.py#271-309) to see if a table was missed, embeds the section text by calling the [embedder](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/embeddings/embedder.py#175-202), and attempts to [profile](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/section_profiler.py#225-237) it. Emits full `ParsedDossier` and `ParsedSection` models.

### [parsers/section_profiler.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/section_profiler.py)
- **Goal**: Domain-agnostic semantic profiler applied dynamically right after parsing.
- **Logic**: Using the AzureLLMClient, it generates a [SemanticProfile](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/section_profiler.py#26-35) giving generic metrics like "complexity_level", "has_regulatory_references", along with dynamically named "domain_concepts". Reduces hardcoded keyword reliance later down the line.

### [parsers/models.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/models.py)
- **Goal**: Exposes heavily structured models for all phases.
- **Logic**: Defines Pydantic outputs [DBChangeRecord](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/models.py#266-301), [ChangeBundle](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/models.py#303-325), [ImpactedSection](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/models.py#331-362), [SectionUpdatePlan](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/models.py#364-422), etc for strict schema adherence inside the LLM calls and pipeline jumps.

---

## 7. Agent Workflow Layer ([agent/](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/agent/workflow.py#369-377))

### [agent/workflow.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/agent/workflow.py)
- **Goal**: Builds a LangGraph state machine tracking [DossierUpdateState](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/agent/workflow.py#56-89).
- **Logic**: Encapsulates the entire DB/LLM pipeline into a node-based graph. Transitions gracefully between nodes: `poll_changes` -> `interpret_concepts` -> `map_sections` -> `generate_updates` -> [review](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/agent/workflow.py#315-329) -> `store_updates`. Keeps track of errors and warnings robustly so that an agent could pick up and retry if failures occur.

---

## 8. Root Orchestrators (Execution Scripts)

### [main.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/main.py)
- **Goal**: The "Phase 1" Builder. Used to bootstrap the system from scratch.
- **Logic**: 
  1. Initializes the `neo4j_client`.
  2. Ensures the Neo4j schema is in place via [build_schema](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/graph/neo4j_schema.py#251-277).
  3. Loops over every document mapped inside [config/dossier_registry.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/config/dossier_registry.py) -> Runs [parse_dossier](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/parsers/dossier_parser.py#65-128) -> Runs [load_dossier](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/graph/graph_loader.py#37-68) straight into the Graph.
  - Usage parameters include `--clear` (wipes db first) or `--parse-only` (doesn't write out to neo4j).

### [run_agent_realtime.py](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/run_agent_realtime.py)
- **Goal**: The Continuous Monitor loop (The Core Application execution).
- **Logic**: 
  1. Initiates the [ChangeDetectionPipeline](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/db/change_pipeline.py#26-539) and [SectionContentGenerator](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/llm/content_generator.py#46-298).
  2. Loops indefinitely via `time.sleep()`.
  3. Extracts any SQL database changes on the loop ([poll_once()](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/db/poller.py#107-152)).
  4. Runs the entire `pipeline.process_change_bundle` to generate an Update Plan.
  5. Runs the generator to build new AI text from the Plan.
  6. Implements a human-in-the-loop CLI step (`APPROVE REQUIRED (A/R/Q)`) showing the change diff.
  7. Upon approval, invokes the [DossierInjector](file:///c:/Users/SuryaPratapRout/Downloads/Github/cipher_dsg/db/dossier_injector.py#39-357) to execute Neo4j write queries gracefully versioning the sections.

---

## Summary
The system is built sequentially to process unstructured data completely via LLM logic, escaping regex/hardcoded paradigms. Changes originating from SQL are funneled through the LLM DB intelligence layers to modify Graph Nodes seamlessly with built-in versioning and templated references.
