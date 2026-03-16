"""
parsers/section_profiler.py
----------------------------
Semantic profiling for dossier sections.
Generates generic semantic metadata using LLM - no hardcoded domain knowledge.

This module provides the foundation for schema-agnostic document processing.
"""
from typing import List, Dict, Any
from dataclasses import dataclass, field
import json

from parsers.models import (
    SemanticProfileOutput,
    DomainConceptsOutput,
    SemanticCharacteristics
)
from prompts import get_prompt
from llm.azure_client import AzureLLMClient
from embeddings.embedder import EmbedderProtocol
from utils.logger import get_logger

log = get_logger(__name__)


@dataclass
class SemanticProfile:
    """
    Generic semantic profile for a section.
    No hardcoded domain fields - fully adaptable.
    """
    situation_description: str
    situation_embedding: List[float]
    characteristics: Dict[str, Any] = field(default_factory=dict)


class SectionProfiler:
    """
    Generates semantic profiles and domain concepts for document sections.
    
    Fully generic approach:
    - LLM analyzes section content without domain assumptions
    - Extracts characteristics dynamically based on what it observes
    - Works for cosmetics, pharma, food safety, any regulatory domain
    """
    
    def __init__(self, llm: AzureLLMClient, embedder: EmbedderProtocol):
        """
        Initialize profiler with LLM and embedder.
        
        Args:
            llm: Azure LLM client for structured generation
            embedder: Embedding generator for semantic search
        """
        self.llm = llm
        self.embedder = embedder
    
    def generate_semantic_profile(
        self,
        section_title: str,
        section_text: str
    ) -> SemanticProfile:
        """
        Generate semantic profile for a section using LLM.
        
        This is the core method that creates generic, domain-agnostic
        situation descriptions for any type of regulatory document.
        
        Args:
            section_title: Section title/heading
            section_text: Full section content
        
        Returns:
            SemanticProfile with description, embedding, and characteristics
        """
        log.debug(f"Generating semantic profile for section: {section_title}")
        
        try:
            # Get prompts from centralized store (no hardcoding)
            system_prompt = get_prompt('semantic_profiling', 'system')
            user_prompt_template = get_prompt('semantic_profiling', 'user')
            
            # Format user prompt with section content
            user_prompt = user_prompt_template.format(
                title=section_title,
                content=section_text[:1000]  # First 1000 chars for analysis
            )
            
            # Call LLM with structured output (Pydantic model)
            response = self.llm.ask_structured_pydantic(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=SemanticProfileOutput,
                temperature=0.1  # Deterministic for consistency
            )
            
            # Generate embedding for semantic comparison
            embedding = self.embedder.embed(response.description)
            
            # Convert Pydantic model to dict for characteristics
            characteristics = {
                "item_count": response.characteristics.item_count,
                "format_style": response.characteristics.format_style,
                "has_regulatory_references": response.characteristics.has_regulatory_references,
                "complexity_level": response.characteristics.complexity_level,
                "uses_numerical_data": response.characteristics.uses_numerical_data,
                "hierarchical_structure": response.characteristics.hierarchical_structure,
            }
            
            # Remove None values for cleanliness
            characteristics = {k: v for k, v in characteristics.items() if v is not None}
            
            log.debug(f"Generated profile: {response.description[:100]}...")
            
            return SemanticProfile(
                situation_description=response.description,
                situation_embedding=embedding,
                characteristics=characteristics
            )
            
        except Exception as e:
            log.error(f"Failed to generate semantic profile: {e}")
            # Fallback: basic profile without LLM
            return self._create_fallback_profile(section_title, section_text)
    
    def extract_domain_concepts(
        self,
        section_title: str,
        section_text: str
    ) -> List[str]:
        """
        Extract domain concepts that this section addresses.
        
        Uses LLM to discover concepts dynamically - no predefined taxonomy.
        Concepts are short labels like "allergen declaration", "supplier identity".
        
        Args:
            section_title: Section title/heading
            section_text: Section content (first ~800 chars used)
        
        Returns:
            List of 1-3 concept labels
        """
        log.debug(f"Extracting concepts for section: {section_title}")
        
        try:
            # Get prompts from centralized store
            system_prompt = get_prompt('concept_tagging', 'system')
            user_prompt_template = get_prompt('concept_tagging', 'user')
            
            # Format user prompt
            user_prompt = user_prompt_template.format(
                title=section_title,
                content=section_text[:800]  # First 800 chars
            )
            
            # Call LLM with structured output
            response = self.llm.ask_structured_pydantic(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=DomainConceptsOutput,
                temperature=0.1  # Deterministic
            )
            
            log.debug(f"Extracted concepts: {response.concepts}")
            
            return response.concepts
            
        except Exception as e:
            log.error(f"Failed to extract concepts: {e}")
            # Fallback: derive basic concept from title
            return self._create_fallback_concepts(section_title)
    
    def _create_fallback_profile(
        self,
        section_title: str,
        section_text: str
    ) -> SemanticProfile:
        """
        Create basic profile when LLM fails.
        Uses simple heuristics based on observable patterns.
        """
        # Detect format style from content
        has_table = "│" in section_text or "|" in section_text
        has_bullets = any(section_text.startswith(marker) 
                         for marker in ["•", "-", "*", "·"])
        
        if has_table:
            format_style = "table"
        elif has_bullets:
            format_style = "bullet_list"
        else:
            format_style = "narrative"
        
        description = (
            f"Section titled '{section_title}' presents information in "
            f"{format_style} format. "
        )
        
        # Generate embedding from title + first 500 chars
        embedding_text = f"{section_title}. {section_text[:500]}"
        embedding = self.embedder.embed(embedding_text)
        
        return SemanticProfile(
            situation_description=description,
            situation_embedding=embedding,
            characteristics={
                "format_style": format_style,
                "complexity_level": "moderate"
            }
        )
    
    def _create_fallback_concepts(self, section_title: str) -> List[str]:
        """
        Create basic concepts from section title when LLM fails.
        """
        # Simple heuristic: use title as concept (sanitized)
        concept = section_title.lower().strip()
        # Remove section numbers
        concept = " ".join(word for word in concept.split() 
                          if not word[0].isdigit())
        return [concept] if concept else ["general section"]


def create_profiler(llm: AzureLLMClient, embedder: EmbedderProtocol) -> SectionProfiler:
    """
    Factory function to create SectionProfiler instance.
    
    Args:
        llm: Azure LLM client
        embedder: Embedding generator
    
    Returns:
        Configured SectionProfiler instance
    """
    return SectionProfiler(llm=llm, embedder=embedder)
