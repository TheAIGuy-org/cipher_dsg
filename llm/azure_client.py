"""
llm/azure_client.py
-------------------
Azure OpenAI GPT-4o client for dossier intelligence.

This is the foundation of the dynamic system - all reasoning happens here.
No regex patterns, no hardcoded fields, just intelligent context processing.

Key Design Principles:
1. Zero-shot capable - works with any dossier format
2. Self-documenting - prompts explain what we're doing
3. Fault-tolerant - handles API failures gracefully
4. Cost-aware - uses structured outputs to minimize tokens
"""
import os
import json
import time
import threading
from typing import Optional, Dict, List, Any
from dataclasses import dataclass

from openai import AzureOpenAI, OpenAIError
from pydantic import BaseModel
from utils.logger import get_logger

log = get_logger("llm.azure_client")


class TokenTracker:
    """
    Thread-safe per-phase LLM token tracker.

    Pipeline usage (sequential phases):
        token_tracker.reset()
        token_tracker.begin_phase("Phase 3: Concept Extraction")
        # ... LLM calls auto-record here ...
        token_tracker.begin_phase("Phase 4: Section Mapping")
        # ...
        token_tracker.print_summary()   # prints table + clears all phases

    Parallel per-section usage (content generation):
        token_tracker.begin_phase(f"Phase 9: {section_number}")
        # ... LLM calls auto-record here ...
        token_tracker.log_current_phase()  # prints + removes only this thread's phase
    """

    def __init__(self):
        self._lock   = threading.Lock()
        self._phases: list[dict] = []
        self._tl     = threading.local()

    def begin_phase(self, name: str):
        """Set the active phase name for the calling thread."""
        self._tl.phase = name

    def record(self, prompt: int, completion: int, cached: int = 0, reasoning: int = 0):
        """Accumulate token counts into the calling thread's active phase."""
        phase = getattr(self._tl, "phase", "UNTRACKED")
        with self._lock:
            for p in self._phases:
                if p["name"] == phase:
                    p["input"]     += prompt
                    p["output"]    += completion
                    p["cached"]    += cached
                    p["reasoning"] += reasoning
                    p["calls"]     += 1
                    return
            self._phases.append({
                "name": phase, "input": prompt, "output": completion,
                "cached": cached, "reasoning": reasoning, "calls": 1,
            })

    def log_current_phase(self):
        """Print and remove only the calling thread's phase — for parallel generators."""
        phase = getattr(self._tl, "phase", None)
        if not phase:
            return
        with self._lock:
            entry = next((p for p in self._phases if p["name"] == phase), None)
            if entry:
                self._phases.remove(entry)
        if entry:
            self._log_phase_line(entry)

    def reset(self):
        """Clear all accumulated data (call at the start of each bundle)."""
        with self._lock:
            self._phases.clear()

    def print_summary(self):
        """Print all accumulated phases as a formatted table, then clear."""
        with self._lock:
            phases = list(self._phases)
            self._phases.clear()
        if not phases:
            return
        self._log_table(phases)

    # ── private helpers ───────────────────────────────────────────────────────

    def _log_phase_line(self, p: dict):
        total  = p["input"] + p["output"]
        hidden = []
        if p["cached"]:    hidden.append(f"cached={p['cached']}")
        if p["reasoning"]: hidden.append(f"reasoning={p['reasoning']}")
        suffix = f"  [{', '.join(hidden)}]" if hidden else ""
        log.info(
            f"TOKENS | {p['name']:<42} | calls={p['calls']}  "
            f"input={p['input']}  output={p['output']}  total={total}{suffix}"
        )

    def _log_table(self, phases: list[dict]):
        W = 40
        log.info("=" * 84)
        log.info("  TOKEN USAGE PER PHASE")
        log.info(f"  {'Phase':<{W}} {'Calls':>5}  {'Input':>8}  {'Output':>8}  {'Cached':>8}  {'Reason':>8}  {'Total':>8}")
        log.info("  " + "-" * 82)
        g_in = g_out = g_cached = g_reason = g_calls = 0
        for p in phases:
            total = p["input"] + p["output"]
            log.info(
                f"  {p['name']:<{W}} {p['calls']:>5}  {p['input']:>8}  {p['output']:>8}  "
                f"{p['cached']:>8}  {p['reasoning']:>8}  {total:>8}"
            )
            g_in     += p["input"]
            g_out    += p["output"]
            g_cached += p["cached"]
            g_reason += p["reasoning"]
            g_calls  += p["calls"]
        log.info("  " + "-" * 82)
        grand = g_in + g_out
        log.info(
            f"  {'TOTAL':<{W}} {g_calls:>5}  {g_in:>8}  {g_out:>8}  "
            f"{g_cached:>8}  {g_reason:>8}  {grand:>8}"
        )
        log.info("=" * 84)


token_tracker = TokenTracker()


@dataclass
class LLMResponse:
    """Structured LLM response with metadata"""
    content: str | dict
    model: str
    tokens_used: int
    latency_ms: float
    success: bool
    error: Optional[str] = None


class AzureLLMClient:
    """
    Production-ready Azure OpenAI client with:
    - Automatic retry logic
    - Token tracking
    - Error handling
    - Response validation
    """
    
    def _record_usage(self, usage) -> None:
        """Extract input/output/hidden token counts from an API usage object and record them."""
        prompt     = getattr(usage, "prompt_tokens",     0) or 0
        completion = getattr(usage, "completion_tokens", 0) or 0
        cached     = getattr(getattr(usage, "prompt_tokens_details",     None), "cached_tokens",    0) or 0
        reasoning  = getattr(getattr(usage, "completion_tokens_details", None), "reasoning_tokens", 0) or 0
        token_tracker.record(prompt, completion, cached, reasoning)

    def __init__(self):
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.model = os.getenv("AZURE_OPENAI_MODEL", "gpt-4o")
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        
        # Validate configuration
        if not all([os.getenv("AZURE_OPENAI_KEY"), os.getenv("AZURE_OPENAI_ENDPOINT")]):
            log.warning("Azure OpenAI not configured - LLM features disabled")
            self.enabled = False
        else:
            self.enabled = True
            log.info(f"Azure OpenAI client initialized: model={self.model}")
    
    def ask(
        self, 
        prompt: str, 
        system_prompt: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: int = 4000,
        response_format: str = "text"
    ) -> LLMResponse:
        """
        Send a prompt to GPT-4o and get response.
        
        Args:
            prompt: The user prompt
            system_prompt: Optional system context
            temperature: 0 = deterministic, 1 = creative
            max_tokens: Maximum response length
            response_format: "text" or "json_object"
        
        Returns:
            LLMResponse with content and metadata
        """
        if not self.enabled:
            return LLMResponse(
                content="",
                model="disabled",
                tokens_used=0,
                latency_ms=0,
                success=False,
                error="Azure OpenAI not configured"
            )
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        start_time = time.time()
        
        for attempt in range(self.max_retries):
            try:
                kwargs = {
                    "model": self.model,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": max_tokens
                }
                
                if response_format == "json_object":
                    kwargs["response_format"] = {"type": "json_object"}
                
                response = self.client.chat.completions.create(**kwargs)

                latency_ms = (time.time() - start_time) * 1000
                content = response.choices[0].message.content
                self._record_usage(response.usage)

                # Parse JSON if requested
                if response_format == "json_object":
                    try:
                        content = json.loads(content)
                    except json.JSONDecodeError as e:
                        log.error(f"Failed to parse JSON response: {e}")
                        return LLMResponse(
                            content="",
                            model=self.model,
                            tokens_used=response.usage.total_tokens,
                            latency_ms=latency_ms,
                            success=False,
                            error=f"JSON parse error: {e}"
                        )
                
                log.debug(f"LLM call successful: {response.usage.total_tokens} tokens, {latency_ms:.0f}ms")
                
                return LLMResponse(
                    content=content,
                    model=self.model,
                    tokens_used=response.usage.total_tokens,
                    latency_ms=latency_ms,
                    success=True
                )
                
            except OpenAIError as e:
                log.warning(f"LLM call failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    return LLMResponse(
                        content="",
                        model=self.model,
                        tokens_used=0,
                        latency_ms=(time.time() - start_time) * 1000,
                        success=False,
                        error=str(e)
                    )
        
        # Should never reach here
        return LLMResponse(
            content="",
            model=self.model,
            tokens_used=0,
            latency_ms=0,
            success=False,
            error="Unknown error"
        )
    
    def ask_structured(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        schema_description: Optional[str] = None
    ) -> LLMResponse:
        """
        Ask LLM for structured JSON response.
        
        This is the primary method for extracting structured data.
        The schema_description helps the LLM understand what format to return.
        
        Args:
            prompt: User prompt
            system_prompt: System context
            schema_description: Description of expected JSON format
        
        Returns:
            LLMResponse with parsed JSON in content field
        """
        full_prompt = prompt
        if schema_description:
            full_prompt += f"\n\nReturn JSON matching this schema:\n{schema_description}"
        
        return self.ask(
            prompt=full_prompt,
            system_prompt=system_prompt,
            temperature=0.0,  # Deterministic for structured extraction
            response_format="json_object"
        )
    
    def ask_structured_pydantic(
        self,
        system_prompt: str,
        user_prompt: str,
        response_model: type[BaseModel],
        temperature: float = 0.0,
        max_tokens: int = 4000
    ) -> BaseModel:
        """
        Ask LLM with Pydantic-validated structured output.
        
        Uses OpenAI's structured outputs API with JSON schema from Pydantic model.
        Guarantees type-safe, validated responses that match the model.
        
        This is the PREFERRED method for Phase 1+ implementation - ensures
        all LLM outputs are validated against Pydantic models.
        
        Args:
            system_prompt: System context/role definition
            user_prompt: User task/question
            response_model: Pydantic model class for response validation
            temperature: Sampling temperature (0 = deterministic)
            max_tokens: Maximum response length
        
        Returns:
            Validated instance of response_model
        
        Raises:
            ValidationError: If LLM response doesn't match schema
            OpenAIError: If API call fails after retries
        """
        if not self.enabled:
            log.error("Azure OpenAI not configured - cannot call ask_structured_pydantic")
            raise RuntimeError("Azure OpenAI not configured")
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        start_time = time.time()
        
        for attempt in range(self.max_retries):
            try:
                # Use OpenAI structured outputs with Pydantic model
                # This automatically converts Pydantic to JSON schema
                response = self.client.beta.chat.completions.parse(
                    model=self.model,
                    messages=messages,
                    response_format=response_model,
                    temperature=temperature,
                    max_tokens=max_tokens
                )
                
                latency_ms = (time.time() - start_time) * 1000

                # Extract parsed model
                parsed_output = response.choices[0].message.parsed
                self._record_usage(response.usage)

                log.debug(
                    f"Structured LLM call successful: {response.usage.total_tokens} tokens, "
                    f"{latency_ms:.0f}ms, model={response_model.__name__}"
                )
                
                return parsed_output
                
            except OpenAIError as e:
                log.warning(
                    f"Structured LLM call failed (attempt {attempt + 1}/{self.max_retries}): {e}"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    log.error(f"All retries exhausted for structured call: {e}")
                    raise
        
        raise RuntimeError("Unexpected: retries exhausted without raising exception")


# Singleton instance
_client_instance: Optional[AzureLLMClient] = None


def get_llm_client() -> AzureLLMClient:
    """Get or create the global LLM client instance"""
    global _client_instance
    if _client_instance is None:
        _client_instance = AzureLLMClient()
    return _client_instance
