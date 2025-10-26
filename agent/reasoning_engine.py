"""
SynthArbiter Multi-Step Reasoning Engine
Implements the NeMo flywheel for ethical dilemma analysis
"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional
import logging
import os

from agent.nemo_clients import GuardrailsClient, EvaluatorClient, NIMClient
from services.nemo_retriever_client import NeMoRetrieverClient
from services.opensearch_client import VectorStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class AgentState:
    """State container for multi-step reasoning"""
    scenario: str
    retrieved_context: List[Dict] = field(default_factory=list)
    reasoning_steps: List[str] = field(default_factory=list)
    simulated_outcomes: List[Dict] = field(default_factory=list)
    guardrail_checks: Dict = field(default_factory=dict)
    evaluation_scores: Dict = field(default_factory=dict)
    final_recommendation: str = ""

class SynthArbiterAgent:
    """Main agent orchestrating the reasoning flywheel"""
    
    def __init__(self):
        self.guardrails = GuardrailsClient()
        self.evaluator = EvaluatorClient()
        self.nim = NIMClient()
        self.retriever = NeMoRetrieverClient()
        self.vector_store = VectorStore()
    
    def run(self, scenario: str, frameworks: List[str] = None) -> AgentState:
        """
        Execute multi-step reasoning loop
        
        Args:
            scenario: Ethical dilemma scenario text
            frameworks: List of ethical frameworks to consider
            
        Returns:
            Complete agent state with recommendation
        """
        if frameworks is None:
            frameworks = ['utilitarian', 'deontological']
        
        state = AgentState(scenario=scenario)
        
        # Step 1: Guardrails - Input Validation
        logger.info("Step 1: Validating input with Guardrails")
        input_check = self.guardrails.validate_input(scenario)
        if not input_check.get('safe'):
            state.final_recommendation = f"Scenario rejected: {input_check.get('reason', 'content policy violation')}"
            return state
        state.guardrail_checks['input'] = input_check
        
        # Step 2: Retriever - Context Retrieval
        logger.info("Step 2: Retrieving relevant context")
        try:
            query_embedding = self.retriever.embed(scenario)
            state.retrieved_context = self.vector_store.search_similar(query_embedding, top_k=10)
            logger.info(f"Retrieved {len(state.retrieved_context)} context passages")
        except Exception as e:
            logger.error(f"Context retrieval error: {e}")
            state.retrieved_context = []
        
        # Step 3: NIM - Ethical Reasoning
        logger.info("Step 3: Generating ethical reasoning")
        context_text = "\n\n".join([doc['text'] for doc in state.retrieved_context[:5]])
        
        frameworks_str = ", ".join(frameworks)
        prompt = f"""You are an AI ethicist analyzing the following scenario. 
Use these ethical frameworks: {frameworks_str}

Relevant context from academic literature:
{context_text}

Scenario:
{scenario}

Provide a structured ethical analysis with:
1. Stakeholder identification
2. Moral trade-offs under each framework
3. Potential consequences
4. Synthesized recommendation

Analysis:"""
        
        try:
            reasoning_response = self.nim.generate(prompt, max_tokens=1024, temperature=0.7)
            state.reasoning_steps = self._parse_reasoning_steps(reasoning_response)
        except Exception as e:
            logger.error(f"Reasoning generation error: {e}")
            state.reasoning_steps = ["Reasoning generation failed"]
        
        # Step 4: Simulation - Counterfactual Analysis
        logger.info("Step 4: Simulating outcomes")
        state.simulated_outcomes = self._simulate_outcomes(scenario, state.reasoning_steps)
        
        # Step 5: Guardrails - Output Validation
        logger.info("Step 5: Validating output with Guardrails")
        output_text = "\n".join(state.reasoning_steps)
        output_check = self.guardrails.validate_output(output_text)
        state.guardrail_checks['output'] = output_check
        
        # Step 6: Evaluator - Quality Assessment
        logger.info("Step 6: Evaluating reasoning quality")
        state.evaluation_scores = self.evaluator.score({
            'context_relevance': self._check_relevance(state.retrieved_context),
            'reasoning_coherence': reasoning_response if reasoning_response else "",
            'ethical_coverage': self._check_framework_coverage(frameworks)
        })
        
        # Step 7: Synthesis - Final Recommendation
        logger.info("Step 7: Synthesizing recommendation")
        state.final_recommendation = self._synthesize_recommendation(state, frameworks)
        
        return state
    
    def _parse_reasoning_steps(self, response: str) -> List[str]:
        """Parse NIM response into structured reasoning steps"""
        # Simple parsing by numbered/bullet points
        steps = []
        lines = response.split('\n')
        current_step = ""
        
        for line in lines:
            line = line.strip()
            if line and (line.startswith('1.') or line.startswith('-') or line.startswith('*')):
                if current_step:
                    steps.append(current_step)
                current_step = line.lstrip('123456789.-* ').strip()
            elif line and current_step:
                current_step += " " + line
        
        if current_step:
            steps.append(current_step)
        
        # Default fallback
        if not steps:
            steps = [response[:500] + "..." if len(response) > 500 else response]
        
        return steps[:5]  # Limit to 5 steps
    
    def _simulate_outcomes(self, scenario: str, reasoning_steps: List[str]) -> List[Dict]:
        """Generate counterfactual outcomes"""
        outcomes = []
        actions = ['grant_rights', 'deny_rights', 'conditional_rights']
        
        for action in actions:
            try:
                prompt = f"""Given scenario: {scenario}
And reasoning: {reasoning_steps[0] if reasoning_steps else "General analysis"}
If we {action.replace('_', ' ')}, what are the likely consequences?
Provide a brief analysis."""
                
                consequence_text = self.nim.generate(prompt, max_tokens=256, temperature=0.8)
                outcomes.append({
                    'action': action,
                    'consequences': consequence_text
                })
            except Exception as e:
                logger.error(f"Simulation error for {action}: {e}")
                outcomes.append({
                    'action': action,
                    'consequences': "Simulation unavailable"
                })
        
        return outcomes
    
    def _check_relevance(self, context: List[Dict]) -> float:
        """Calculate context relevance score"""
        if not context:
            return 0.0
        # Simple scoring based on number of results and their scores
        scores = [doc.get('score', 0.5) for doc in context]
        return min(sum(scores) / len(scores), 1.0) if scores else 0.0
    
    def _check_framework_coverage(self, frameworks: List[str]) -> float:
        """Calculate ethical framework coverage score"""
        # Simplified: more frameworks = better coverage
        return min(len(frameworks) / 2.0, 1.0)
    
    def _synthesize_recommendation(self, state: AgentState, frameworks: List[str]) -> str:
        """Synthesize final recommendation from all analysis"""
        if not state.reasoning_steps:
            return "Unable to generate recommendation due to analysis failure."
        
        # Extract recommendation from first reasoning step or all steps
        if len(state.reasoning_steps) > 0:
            recommendation = state.reasoning_steps[0][:300]
            if len(state.reasoning_steps) > 1:
                recommendation += " " + state.reasoning_steps[1][:200]
        else:
            recommendation = "Analysis completed. Review the detailed reasoning steps for specific recommendations."
        
        return recommendation

