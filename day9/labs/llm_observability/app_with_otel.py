import os
import json
import time
import boto3
import phoenix as px

from openinference.instrumentation.bedrock import BedrockInstrumentor
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# -----------------------------
# PHOENIX SETUP
# -----------------------------
os.environ["PHOENIX_PORT"] = "6006"

print("Launching local Phoenix tracing server...")

session = px.launch_app()

# -----------------------------
# OPEN TELEMETRY SETUP
# -----------------------------
provider = TracerProvider()

provider.add_span_processor(
    SimpleSpanProcessor(
        OTLPSpanExporter(
            endpoint="http://localhost:6006/v1/traces"
        )
    )
)

trace.set_tracer_provider(provider)

# -----------------------------
# BEDROCK INSTRUMENTATION
# -----------------------------
BedrockInstrumentor().instrument()

# -----------------------------
# SUPPORT AGENT
# -----------------------------
def run_support_agent():

    print("\nRunning support agent inquiry...")

    bedrock = boto3.client(
        "bedrock-runtime",
        region_name="us-east-1"
    )

    model_id = "amazon.nova-lite-v1:0"

    body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "text": (
                            "You are a customer support agent. "
                            "Answer the user query clearly.\n\n"
                            "Customer Query: "
                            "'I was charged $50.00 twice "
                            "on my credit card for order #1048. "
                            "I want a refund.'"
                        )
                    }
                ]
            }
        ],
        "inferenceConfig": {
            "maxTokens": 200,
            "temperature": 0.2
        }
    }

    try:

        response = bedrock.invoke_model(
            modelId=model_id,
            body=json.dumps(body)
        )

        response_body = json.loads(
            response["body"].read().decode("utf-8")
        )

        print("\nMODEL RESPONSE:")
        print(json.dumps(response_body, indent=2))

    except Exception as e:
        print("\nBEDROCK ERROR:")
        print(e)


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":

    run_support_agent()

    print("\nPhoenix server is still running...")
    print("Open: http://localhost:6006")
    print("Press CTRL+C to stop.\n")

    while True:
        time.sleep(1)