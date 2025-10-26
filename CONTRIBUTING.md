# Contributing to SynthArbiter

Thank you for your interest in contributing to SynthArbiter!

## Project Structure

```
SynthArbiter/
├── agent/                    # Reasoning engine and NeMo clients
│   ├── nemo_clients.py      # NeMo microservices client wrappers
│   └── reasoning_engine.py  # Multi-step ethical reasoning agent
│
├── app/                      # Flask web application
│   ├── Dockerfile           # Container image definition
│   ├── requirements.txt     # Python dependencies
│   ├── server.py            # Flask API server
│   └── templates/           # HTML templates
│
├── cloudformation/          # AWS infrastructure templates
│   ├── 00-parameters.yaml  # Parameter management
│   ├── 01-network.yaml     # VPC and networking
│   ├── 02-iam.yaml         # IAM roles and policies
│   ├── 03-storage.yaml     # S3 and OpenSearch
│   ├── 04-eks.yaml         # EKS cluster
│   └── 06-application.yaml # Application monitoring
│
├── config/                   # Configuration files
│   └── parameters.json      # Environment parameters
│
├── data_acquisition/        # Legal data scraping
│   ├── scrapers/            # Web scrapers (SEP, arXiv)
│   ├── sources.yaml        # Data source definitions
│   └── synthetic_generator.py # Scenario generation
│
├── k8s/                     # Kubernetes manifests
│   ├── nemo/               # NeMo microservices
│   └── app/                # Web app deployment
│
├── services/                # Service clients
│   ├── nemo_retriever_client.py # NeMo Retriever API
│   └── opensearch_client.py    # OpenSearch vector store
│
└── scripts/                 # Utility scripts
    └── build_vector_index.py   # Index building
```

## Development Setup

1. **Prerequisites**
   - Python 3.10+
   - AWS CLI configured
   - kubectl installed
   - NVIDIA NGC account

2. **Local Development**
   ```bash
   cd app
   pip install -r requirements.txt
   export NGC_API_KEY="your-key"
   python server.py
   ```

3. **Testing**
   - Run scrapers: `python data_acquisition/scrapers/sep_scraper.py`
   - Generate scenarios: `python data_acquisition/synthetic_generator.py`
   - Build index: `python scripts/build_vector_index.py`

## Deployment

Deployment is automated via GitHub Actions:

1. Push to `main` branch triggers deployment
2. Infrastructure deploys first (CloudFormation)
3. Application deploys after (Kubernetes)

See `.github/workflows/` for deployment pipelines.

## License

Contributions are welcome under Creative Commons 
Attribution-NonCommercial 4.0 International License.

