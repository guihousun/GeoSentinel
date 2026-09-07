"""Per-run safety budgets shared by the GeoSentinel agent runtime."""

from __future__ import annotations

import os
import threading
from contextvars import ContextVar, Token
from dataclasses import dataclass, field


DEFAULT_MAX_LLM_CALLS_PER_RUN = 50


class ModelCallBudgetExceeded(RuntimeError):
    """Raised before a model request would exceed the current run budget."""


@dataclass
class ModelCallBudget:
    limit: int
    count: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def consume(self) -> int:
        with self._lock:
            if self.count >= self.limit:
                raise ModelCallBudgetExceeded(
                    f"model call limit reached: {self.count}/{self.limit}"
                )
            self.count += 1
            return self.count


_current_budget: ContextVar[ModelCallBudget | None] = ContextVar(
    "geosentinel_model_call_budget", default=None
)


def configured_model_call_limit() -> int:
    raw = str(
        os.getenv("NTL_MAX_LLM_CALLS_PER_RUN", DEFAULT_MAX_LLM_CALLS_PER_RUN) or ""
    ).strip()
    try:
        value = int(raw)
    except ValueError:
        value = DEFAULT_MAX_LLM_CALLS_PER_RUN
    return max(1, min(value, 500))


def begin_model_call_budget(limit: int | None = None) -> tuple[ModelCallBudget, Token]:
    budget = ModelCallBudget(limit=limit or configured_model_call_limit())
    return budget, _current_budget.set(budget)


def end_model_call_budget(token: Token) -> None:
    _current_budget.reset(token)


def consume_model_call() -> int:
    budget = _current_budget.get()
    return budget.consume() if budget is not None else 0
