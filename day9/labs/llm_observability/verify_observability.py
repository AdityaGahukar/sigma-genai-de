import json
import os
import sys
import requests

def main():

    print("Checking LLM Observability Lab completion status...")

    # -------------------------------------------------
    # 1. CHECK SERVER IS REACHABLE
    # -------------------------------------------------

    try:
        response = requests.get("http://localhost:6006")

        if response.status_code != 200:
            raise Exception("Phoenix UI not responding")

    except Exception as e:
        print("❌ Error: Could not connect to Phoenix server.")
        print(e)
        sys.exit(1)

    print("✓ Phoenix Server Connection: SUCCESS")

    # -------------------------------------------------
    # 2. CHECK TRACES ENDPOINT
    # -------------------------------------------------

    try:
        trace_response = requests.get(
            "http://localhost:6006/v1/traces"
        )

        print("✓ Phoenix traces endpoint reachable")

    except Exception as e:
        print("❌ Could not reach traces endpoint")
        print(e)
        sys.exit(1)

    # -------------------------------------------------
    # 3. CREATE SUCCESS FILE
    # -------------------------------------------------

    output_dir = "../output"

    os.makedirs(output_dir, exist_ok=True)

    result = {
        "status": "success",
        "phoenix_active": True,
        "llm_observability_verified": True
    }

    output_file = os.path.join(
        output_dir,
        "llm_observability_success.json"
    )

    with open(output_file, "w") as f:
        json.dump(result, f, indent=2)

    print("\n🎉 Verification SUCCESS!")
    print(f"✓ Created '{output_file}'")


if __name__ == "__main__":
    main()