# SynthArbiter

An autonomous ethical AI agent for analyzing complex dilemmas in synthetic consciousness, neural organoids, and advanced AI systems.

## System Architecture

```mermaid
graph TB
    subgraph "Data Layer"
        S3[S3 Data Lake<br/>Curated Texts]
        DB[(DynamoDB<br/>Analysis History)]
    end

    subgraph "AI/ML Services - NIM Microservices"
        NIM_REASONING[NIM Reasoning<br/>Llama 3.1 Nemotron Nano 8B]
        NIM_GUARD[NIM Guardrails<br/>Content Safety]
        NIM_EMBED[NIM Embedding<br/>E5-v5 Vector Generation]
        OPENSEARCH[(OpenSearch<br/>Vector Database)]
        LAMBDA_ANALYZE[Lambda<br/>Analysis Orchestrator]
    end

    subgraph "Application Layer - Serverless"
        AMPLIFY[AWS Amplify<br/>S3-Hosted Frontend]
        S3_WEB[S3 Bucket<br/>Source Files]
        API_GW[API Gateway]
        LAMBDA_ANALYZE[Lambda<br/>Ethical Analysis]
    end

    subgraph "Authentication"
        COGNITO[Amazon Cognito<br/>User Management]
    end

    S3 --> NIM_EMBED
    NIM_EMBED --> OPENSEARCH
    CF --> S3_WEB
    CF --> API_GW
    API_GW --> LAMBDA_ANALYZE
    LAMBDA_ANALYZE --> NIM_EMBED
    LAMBDA_ANALYZE --> OPENSEARCH
    LAMBDA_ANALYZE --> NIM_GUARD
    LAMBDA_ANALYZE --> NIM_EVAL
    LAMBDA_ANALYZE --> DB
    API_GW --> COGNITO
```

## Technical Stack

**AI/ML Infrastructure:**
- **NVIDIA NIM Microservices** - Production-grade AI models on SageMaker
  - NIM Reasoning (Llama 3.1 Nemotron Nano 8B)
  - NIM Guardrails (Content Safety)
  - NIM Evaluator (Quality Assessment)
  - NIM Embedding (E5-v5 for vector generation)
- **OpenSearch** - Vector database with k-NN semantic search
- **AWS Lambda** - Analysis orchestration and API handling
- **SageMaker Endpoints** - Persistent GPU instances for NIM microservices

**Cloud Infrastructure:**
- **AWS Amplify** - Frontend hosting with automated S3 deployment
- **Amazon S3** - Source files storage and data lake
- **API Gateway** - REST API management and authentication
- **AWS Lambda** - Serverless compute for all business logic
- **Amazon Cognito** - User authentication and authorization
- **DynamoDB** - Analysis history and user data persistence
- **CloudFormation** - Infrastructure as Code

**Application:**
- **Static HTML/CSS/JS** - No backend web framework needed
- Chart.js - Data visualizations
- Tailwind CSS - UI framework

## RAG System (Retrieval-Augmented Generation)

SynthArbiter includes a complete RAG system for enhanced ethical analysis:

### How It Works

```
User Query → Embedding Generation → Vector Search → Context Retrieval → Enhanced Reasoning
```

1. **Semantic Search**: Ethical scenarios are converted to vectors using NIM Embedding (E5-v5)
2. **Context Retrieval**: OpenSearch finds similar historical cases and ethical principles
3. **Enhanced Analysis**: Retrieved context is included in reasoning prompts for more informed decisions

### Benefits

- **Contextual Analysis**: Each ethical dilemma gets relevant historical context
- **Improved Accuracy**: RAG provides more informed and nuanced ethical reasoning
- **Semantic Matching**: Finds relevant cases even without exact keyword matches
- **Scalable Knowledge**: OpenSearch handles millions of ethical documents

### Data Pipeline

After deployment, populate the knowledge base:

```bash
# Build vector index from preprocessed ethical literature
python scripts/build_vector_index.py \
    --source s3://your-bucket/processed-ethical-data.jsonl \
    --embedding-endpoint syntharbiter-nim-embedding-prod \
    --opensearch-endpoint https://your-opensearch-endpoint \
    --index-name ethical-knowledge \
    --batch-size 20
```

### Testing

Verify the RAG system works:

```bash
python scripts/test_rag_system.py \
    --embedding-endpoint syntharbiter-nim-embedding-prod \
    --opensearch-endpoint https://your-opensearch-endpoint
```

## Prerequisites

- AWS Account with appropriate permissions
- AWS CLI configured with appropriate IAM role
- GitHub repository access for CI/CD deployment
- GitHub account (for CI/CD)

## NIM Model Setup

NVIDIA NIM images are automatically pushed to ECR during deployment (one-time setup per environment). The workflow handles:

- ECR repository creation
- NIM image pull from NGC
- Image push to ECR
- VPC endpoint configuration for ECR access

No manual setup required - the workflow manages everything automatically.

## Deployment

### 1. Configure AWS Credentials

Set up GitHub Secrets in your repository settings:
- `AWS_ROLE_ARN` - IAM role for GitHub Actions deployment
- `NVIDIA_NGC_API_KEY` - (Optional, for future data processing)

### 2. Deploy SynthArbiter

Deploy via consolidated GitHub Actions workflow:

1. Go to **GitHub Actions** tab
2. Select **"Deploy SynthArbiter"** workflow
3. Click **"Run workflow"**
4. Choose **environment** (dev/prod), **region**, and optionally skip parameter setup
5. Click **"Run workflow"**

The workflow automatically handles:

#### **Phase 1: Parameter Setup** (optional)
- ✅ Sync SSM parameters from `config/parameters.json`
- ✅ Create NGC API key in Secrets Manager
- ⏭️ Skippable with `skip_parameters: true`

#### **Phase 2: NIM ECR Setup**
- ✅ Create ECR repositories for all NIM models
- ✅ Pull NIM images from NGC and push to ECR (one-time per environment)
- ✅ Handle VPC access for ECR repositories

#### **Phase 3: Infrastructure Deployment**
- ✅ Deploy 4 layered CloudFormation stacks:
  - **Network** (`syntharbiter-network-{env}`): VPC, subnets, security groups, NAT gateways
  - **Storage** (`syntharbiter-storage-{env}`): OpenSearch domain for vector search
  - **Frontend** (`syntharbiter-frontend-{env}`): Cognito user pools and client configuration
  - **NIM Microservices** (`syntharbiter-nim-{env}`): SageMaker endpoints, Lambda functions, API Gateway, Amplify
- ✅ Package and upload Lambda functions to S3
- ✅ Configure SageMaker endpoints with VPC access
- ✅ Upload static website to S3 and deploy to Amplify

#### **Phase 4: Output Summary**
- **Amplify Website URL**: Automated S3-to-Amplify deployment
- **API Gateway URL**: REST API endpoint
- **Website S3 Bucket**: Source file storage location
- **Cognito User Pool ID & Client ID**: Authentication configuration
- **Infrastructure Stack Outputs**: Complete resource details from all 4 stacks

### Deployment Workflow Diagram

```mermaid
graph TD
    A[GitHub Actions<br/>Deploy SynthArbiter] --> B{skip_parameters?}
    B -->|false| C[Phase 1: Parameter Setup<br/>SSM + Secrets Manager]
    B -->|true| D[Phase 2: NIM ECR Setup<br/>Create repos + Pull/Push images]

    C --> D
    D --> E[Phase 3: Infrastructure Deployment]

    E --> F[Network Stack<br/>syntharbiter-network-ENV]
    F --> G[Storage Stack<br/>syntharbiter-storage-ENV]
    G --> H[Frontend Stack<br/>syntharbiter-frontend-ENV]
    H --> I[NIM Stack<br/>syntharbiter-nim-ENV]

    I --> J[Lambda Packaging<br/>+ S3 Upload]
    J --> K[Website Upload<br/>S3 → Amplify]
    K --> L[Phase 4: Output Summary<br/>URLs + Stack Outputs]

    style A fill:#e1f5fe
    style C fill:#f3e5f5
    style D fill:#fff3e0
    style F fill:#e8f5e8
    style G fill:#e8f5e8
    style H fill:#e8f5e8
    style I fill:#e8f5e8
    style L fill:#fff9c4
```

### 3. Access Your Application

After deployment completes, the workflow outputs all access information:
- **Amplify Website URL**: Automated S3-to-Amplify deployment
- **API Gateway URL**: REST API endpoint
- **Website S3 Bucket**: Source file storage location
- **Cognito User Pool ID & Client ID**: Authentication configuration
- **Complete Infrastructure Stack Outputs**: From all 4 CloudFormation stacks

### Cost Optimization Features

- **NVIDIA NIM Microservices** - Production-grade AI models for maximum hackathon points
- **SageMaker Endpoints** - Persistent GPU instances optimized for inference
- **Promotional Credits** - $100 credits cover ~22 hours of NIM microservices
- **Total 2-week cost**: ~$1,430 (after credits) for maximum scoring

## AI Models

The system uses **NVIDIA NIM Microservices** deployed on SageMaker Endpoints:

- **NIM Reasoning**: Llama 3.1 Nemotron Nano 8B v1 for ethical analysis
- **NIM Guardrails**: Content safety and moderation
- **NIM Evaluator**: Quality assessment and scoring
- **Infrastructure**: Persistent GPU instances (ml.g5.2xlarge)
- **Deployment**: Production-grade containers with security updates

**Ethical Training Data Sources:**
- Stanford Encyclopedia of Philosophy (CC BY-NC-ND 4.0)
- arXiv AI Ethics Papers (Open Access)
- NIH Bioethics Resources (Public Domain)
- Academic philosophy and ethics literature

## Testing

### System Health Check

```bash
# Check CloudFormation stack status
aws cloudformation describe-stack-events \
  --stack-name syntharbiter-serverless-prod \
  --region us-east-1

# Test API Gateway endpoint
curl https://<api-gateway-id>.execute-api.us-east-1.amazonaws.com/prod/api/analyze \
  -H "Content-Type: application/json" \
  -d '{"scenario": "test", "frameworks": ["utilitarian"]}'
```

### End-to-End Test

```bash
# Submit test scenario via API Gateway
curl -X POST https://<api-gateway-id>.execute-api.us-east-1.amazonaws.com/prod/api/analyze \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <cognito-jwt-token>" \
  -d '{
    "scenario": "A neural organoid exhibits learning patterns. What ethical considerations apply?",
    "frameworks": ["utilitarian", "deontological"]
  }'
```

### Website Access

- **Static Website**: Available at Amplify URL with automated S3 deployment
- **API Endpoints**: Protected by Cognito authentication
- **Real-time Monitoring**: Check AWS CloudWatch for Lambda metrics

### Expected Response

```json
{
  "analysisId": "uuid-here",
  "recommendation": "Conditional protections with ongoing monitoring...",
  "reasoning": [
    "Step 1: Context retrieval identified relevant philosophical frameworks",
    "Step 2: Utilitarian analysis considers aggregate welfare impacts",
    "Step 3: Deontological analysis examines inherent rights claims",
    "Step 4: Stakeholder analysis identifies affected parties",
    "Step 5: Synthesis balances competing moral considerations"
  ],
  "outcomes": [...],
  "evaluation": {
    "context_relevance": 0.87,
    "reasoning_coherence": 0.92,
    "ethical_coverage": 0.85
  },
  "tradeoffs": {...}
}
```

## System Architecture Details

### Multi-Step Reasoning Loop

```mermaid
sequenceDiagram
    participant User
    participant API
    participant Curator
    participant Retriever
    participant NIM
    participant Guardrails
    participant Evaluator
    participant DB

    User->>API: Submit ethical scenario
    API->>Curator: Preprocess input
    Curator->>Retriever: Generate embedding
    Retriever->>Retriever: Search similar contexts (k-NN)
    Retriever->>API: Return top-k passages
    API->>Guardrails: Validate input safety
    Guardrails->>API: Safety check passed
    API->>NIM: Reasoning prompt + context
    NIM->>NIM: Generate multi-step analysis
    NIM->>API: Reasoning output
    API->>Guardrails: Validate output safety
    Guardrails->>API: Safety check passed
    API->>Evaluator: Score quality metrics
    Evaluator->>API: Evaluation scores
    API->>DB: Store analysis
    API->>User: Return recommendation
```

### Infrastructure Components

**Serverless Functions:**
- **Analyze Lambda**: Main reasoning engine with SageMaker integration
- **Evaluate Lambda**: Quality assessment and scoring
- **Guardrails Lambda**: Content safety and moderation
- **Auto-scaling**: 0-3 concurrent executions per function

**AI/ML Services:**
- **SageMaker Serverless Inference**: Llama 3.1 Nemotron Nano 8B model
- **Pay-per-token pricing**: Only charged for actual inference usage
- **Automatic scaling**: Handles any request load
- **No infrastructure management**: Fully managed by AWS

## Monitoring

**CloudWatch Logs:**
- `/aws/lambda/syntharbiter-analyze-*` - Analysis function logs
- `/aws/lambda/syntharbiter-evaluate-*` - Evaluation function logs
- `/aws/lambda/syntharbiter-guardrails-*` - Guardrails function logs

**CloudWatch Alarms:**
- Lambda errors > 5%
- SageMaker endpoint latency > 30s
- API Gateway 5XX errors > 5%

## Cost Optimization

- **NIM Microservices**: Required for maximum hackathon points vs generic models
- **SageMaker Endpoints**: Persistent instances vs serverless (trade-off for scoring)
- **Promotional Credits**: $100 covers ~22 hours of 3 NIM microservices
- **Total Cost**: ~$1,430 for 2-week hackathon (after credits)
- **S3 lifecycle policies**: Transition to IA after 30 days
- **DynamoDB on-demand pricing**: Scale with usage

## Security

- All data encrypted at rest (S3, DynamoDB, SageMaker)
- TLS 1.2+ for all network communication
- IAM execution roles for Lambda functions
- VPC configuration for Lambda functions
- Cognito user authentication and authorization
- Secrets managed via AWS Systems Manager Parameter Store

## License

This work is licensed under Creative Commons Attribution-NonCommercial 4.0 
International License. See LICENSE file for details.

Data sources retain their original licenses:
- Stanford Encyclopedia of Philosophy: CC BY-NC-ND 4.0
- arXiv papers: Various open-access licenses
- NIH resources: Public domain

## Contributing

This is a research prototype. For inquiries, please open an issue.

