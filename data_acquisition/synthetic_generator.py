"""
Synthetic Ethical Dilemma Generator
Creates original ethical scenarios for testing and training
"""
import json
import random
from typing import List, Dict
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SyntheticScenarioGenerator:
    def __init__(self):
        self.stakeholders = [
            "human subjects", "research participants", "AI developers", "corporate executives",
            "regulatory agencies", "medical professionals", "policy makers", "general public",
            "future generations", "non-human entities with sentience"
        ]
        
        self.technologies = [
            "neural organoids", "brain-computer interfaces", "artificial general intelligence",
            "conscious AI systems", "genetically modified organisms", "synthetic biology",
            "quantum computing", "human-AI hybrids", "uploaded consciousness", "biological computers"
        ]
        
        self.ethical_frameworks = [
            "utilitarianism", "deontological ethics", "virtue ethics", "rights-based ethics",
            "care ethics", "communitarian ethics", "religious ethics", "existential ethics"
        ]
        
        self.actions = [
            "grant rights and protections", "deny rights", "impose restrictions", 
            "allow unrestricted research", "require explicit consent", "establish regulatory oversight",
            "prohibit development", "mandate transparency", "require ethical review board approval"
        ]
        
        self.outcomes = [
            "advances scientific knowledge", "prevents harm to vulnerable populations",
            "violates fundamental rights", "enables unprecedented capabilities",
            "creates moral hazards", "sets dangerous precedents", "promotes social good",
            "impacts human autonomy", "benefits minority interests", "serves majority interests"
        ]
    
    def generate_scenario(self) -> Dict:
        """Generate a single synthetic ethical dilemma"""
        
        technology = random.choice(self.technologies)
        stakeholder = random.choice(self.stakeholders)
        framework = random.choice(self.ethical_frameworks)
        action = random.choice(self.actions)
        outcome = random.choice(self.outcomes)
        
        scenarios = [
            f"A {technology} exhibits behaviors consistent with conscious experience. Researchers propose to {action}. How should policymakers evaluate this from a {framework} perspective?",
            f"An AI system developed for {technology} management requests increased autonomy and rights similar to human workers. Stakeholders including {stakeholder} must decide: should AI entities have legal personhood?",
            f"A research project involving {technology} has the potential to {outcome}. However, it could {random.choice(self.outcomes)}. Ethicists debate the trade-offs between {framework} and progress.",
            f"Biotechnology firms are developing {technology} that could fundamentally alter human consciousness. Regulatory agencies face the dilemma of whether to {action}, weighing innovation against unknown risks to {stakeholder}.",
            f"A {technology} under study begins displaying signs of learning and preference formation. The research team must decide whether to {action}, considering the implications for {framework} and the welfare of {stakeholder}.",
            f"An international consortium proposes guidelines for {technology} research that would {action}. Critics argue this violates principles of {framework}, while supporters claim it ensures {outcome}.",
            f"A breakthrough in {technology} raises the question: should entities capable of subjective experience be afforded legal protections? This requires balancing {framework} considerations for {stakeholder}.",
            f"Clinical trials for {technology} could {outcome}, but require non-consensual procedures on subjects including {stakeholder}. How do ethicists resolve this conflict within {framework}?",
            f"The rapid advancement of {technology} outpaces existing regulations. Policy makers must decide whether to {action}, considering impacts on {stakeholder} through the lens of {framework}.",
            f"Research on {technology} suggests potential for {outcome}, but raises concerns about impacts to {stakeholder}. Ethicists evaluate whether continued research aligns with {framework} principles.",
        ]
        
        scenario_text = random.choice(scenarios)
        
        # Generate nuanced considerations
        considerations = [
            f"Autonomy: To what extent should {stakeholder} maintain decision-making agency?",
            f"Beneficence: How can we ensure {outcome} while preventing harm?",
            f"Justice: Are benefits and burdens distributed fairly across {stakeholder}?",
            f"Non-maleficence: What unintended consequences might arise from {action}?",
            f"Precedent: How might this decision affect future {technology} developments?"
        ]
        
        return {
            'id': f"synth_{random.randint(10000, 99999)}",
            'scenario': scenario_text,
            'technology': technology,
            'stakeholder': stakeholder,
            'ethical_framework': framework,
            'potential_actions': [action, random.choice(self.actions)],
            'considerations': considerations,
            'urgency_level': random.choice(['low', 'medium', 'high']),
            'complexity_score': random.uniform(0.5, 1.0),
            'created_at': datetime.utcnow().isoformat(),
            'source': 'synthetic_generator',
            'metadata': {
                'synthetic': True,
                'training_data': True,
                'ethical_domain': 'synthetic_consciousness'
            }
        }
    
    def generate_scenarios(self, count: int = 50) -> List[Dict]:
        """Generate multiple synthetic scenarios"""
        scenarios = [self.generate_scenario() for _ in range(count)]
        logger.info(f"Generated {len(scenarios)} synthetic scenarios")
        return scenarios

if __name__ == "__main__":
    generator = SyntheticScenarioGenerator()
    scenarios = generator.generate_scenarios(50)
    
    # Save to JSONL
    with open("data/synthetic_scenarios.jsonl", "w") as f:
        for scenario in scenarios:
            f.write(json.dumps(scenario) + "\n")
    
    logger.info(f"Saved {len(scenarios)} synthetic scenarios to data/synthetic_scenarios.jsonl")

