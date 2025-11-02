// SynthArbiter Main JavaScript
(function() {
    'use strict';

    const config = window.SYNTHARBITER_CONFIG || {};
    const { Auth } = window.aws_amplify || {};

    const form = document.getElementById('scenario-form');
    const loading = document.getElementById('loading');
    const results = document.getElementById('results');
    let chartInstance = null;
    
    // Form submission handler
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        const scenario = document.getElementById('scenario').value.trim();
        const frameworks = Array.from(document.getElementById('frameworks').selectedOptions)
            .map(opt => opt.value);
        
        if (!scenario) {
            showError('Please enter a scenario to analyze');
            return;
        }
        
        // Show loading, hide results
        loading.classList.remove('hidden');
        results.classList.add('hidden');
        form.querySelector('button[type="submit"]').disabled = true;
        
        try {
            // Get authentication token
            let authHeaders = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            };

            if (Auth) {
                try {
                    const session = await Auth.currentSession();
                    const token = session.getIdToken().getJwtToken();
                    authHeaders['Authorization'] = `Bearer ${token}`;
                } catch (authError) {
                    console.warn('No active session, proceeding without authentication');
                }
            }

            const response = await fetch(`${config.API_BASE_URL}/api/analyze`, {
                method: 'POST',
                headers: authHeaders,
                body: JSON.stringify({ scenario, frameworks })
            });
            
            const data = await response.json();
            
            if (response.ok) {
                displayResults(data);
            } else {
                showError('Error: ' + (data.error || 'Unknown error occurred'));
            }
        } catch (error) {
            console.error('Error:', error);
            showError('Failed to connect to analysis service. Please try again.');
        } finally {
            loading.classList.add('hidden');
            results.classList.remove('hidden');
            form.querySelector('button[type="submit"]').disabled = false;
        }
    });
    
    function displayResults(data) {
        // Add fade-in animation
        results.classList.add('fade-in');
        
        // Display recommendation
        document.getElementById('recommendation').textContent = data.recommendation || 'No recommendation available.';
        
        // Display reasoning steps
        displayReasoningSteps(data.reasoning || []);
        
        // Display outcomes
        displayOutcomes(data.outcomes || []);
        
        // Draw tradeoff chart
        if (data.tradeoffs) {
            drawChart(data.tradeoffs);
        }
        
        // Scroll to results
        results.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
    
    function displayReasoningSteps(steps) {
        const timeline = document.getElementById('reasoning-timeline');
        timeline.innerHTML = '';
        
        if (steps.length === 0) {
            timeline.innerHTML = '<p class="text-gray-400 italic">No reasoning steps available</p>';
            return;
        }
        
        steps.forEach((step, idx) => {
            const stepDiv = document.createElement('div');
            stepDiv.className = 'reasoning-step fade-in';
            stepDiv.style.animationDelay = `${idx * 0.1}s`;
            stepDiv.innerHTML = `
                <span class="step-number">${idx + 1}</span>
                <div class="flex-1">
                    <p class="text-gray-300">${escapeHtml(step)}</p>
                </div>
            `;
            timeline.appendChild(stepDiv);
        });
    }
    
    function displayOutcomes(outcomes) {
        const outcomesDiv = document.getElementById('outcomes');
        outcomesDiv.innerHTML = '';
        
        if (outcomes.length === 0) {
            outcomesDiv.innerHTML = '<p class="text-gray-400 italic">No simulated outcomes available</p>';
            return;
        }
        
        outcomes.forEach((outcome, idx) => {
            const outcomeDiv = document.createElement('div');
            outcomeDiv.className = 'outcome-card fade-in';
            outcomeDiv.style.animationDelay = `${idx * 0.1}s`;
            
            const action = (outcome.action || 'unknown_action')
                .replace(/_/g, ' ')
                .replace(/\b\w/g, l => l.toUpperCase());
            
            const consequences = (outcome.consequences || 'No analysis available')
                .substring(0, 250);
            
            outcomeDiv.innerHTML = `
                <div class="flex items-start space-x-3">
                    <div class="flex-shrink-0">
                        <svg class="w-6 h-6 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"></path>
                        </svg>
                    </div>
                    <div class="flex-1">
                        <h4 class="font-semibold text-blue-300 mb-1">${escapeHtml(action)}</h4>
                        <p class="text-gray-400 text-sm">${escapeHtml(consequences)}${consequences.length > 200 ? '...' : ''}</p>
                    </div>
                </div>
            `;
            outcomesDiv.appendChild(outcomeDiv);
        });
    }
    
    function drawChart(tradeoffs) {
        const canvas = document.getElementById('tradeoff-chart');
        if (!canvas) return;
        
        const ctx = canvas.getContext('2d');
        
        // Destroy previous chart if exists
        if (chartInstance) {
            chartInstance.destroy();
        }
        
        chartInstance = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: ['Utilitarian Harm', 'Deontological Duty', 'Rights Violation', 'Precedent Risk'],
                datasets: [{
                    label: 'Ethical Impact Score',
                    data: [
                        tradeoffs.utilitarian_harm || 0,
                        tradeoffs.deontological_duty || 0,
                        tradeoffs.rights_violation || 0,
                        tradeoffs.precedent_risk || 0
                    ],
                    backgroundColor: [
                        '#ef4444',  // red
                        '#10b981',  // green
                        '#ef4444',  // red
                        '#f59e0b'   // amber
                    ],
                    borderRadius: 4,
                    maxBarThickness: 40
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        min: -10,
                        max: 10,
                        grid: { 
                            color: 'rgba(156, 163, 175, 0.2)',
                            borderColor: 'rgba(156, 163, 175, 0.5)'
                        },
                        ticks: { 
                            color: '#9ca3af',
                            font: { size: 12 }
                        },
                        title: {
                            display: true,
                            text: 'Impact Score (-10 to +10)',
                            color: '#9ca3af'
                        }
                    },
                    y: {
                        grid: { color: 'rgba(156, 163, 175, 0.1)' },
                        ticks: { 
                            color: '#9ca3af',
                            font: { size: 12 }
                        }
                    }
                },
                plugins: {
                    legend: { display: false },
                    tooltip: { 
                        enabled: true,
                        backgroundColor: 'rgba(0, 0, 0, 0.8)',
                        titleColor: '#fff',
                        bodyColor: '#fff',
                        borderColor: 'rgba(118, 185, 0, 0.5)',
                        borderWidth: 1
                    },
                    title: {
                        display: true,
                        text: 'Ethical Framework Analysis',
                        color: '#9ca3af',
                        font: { size: 14 }
                    }
                }
            }
        });
    }
    
    function showError(message) {
        alert(message);
    }
    
    function escapeHtml(text) {
        const map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return text.replace(/[&<>"']/g, m => map[m]);
    }
})();

