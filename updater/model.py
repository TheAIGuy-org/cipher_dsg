from typing import Any
 
from pydantic import BaseModel, Field
 
 
class PromptOutput(BaseModel):
    """LLM response: the modified section elements[] only."""
 
    elements: list[dict[str, Any]] = Field(..., min_length=1)