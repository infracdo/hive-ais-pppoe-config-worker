#!/bin/bash

# Environment Loader Script for PPPoE Configuration Worker
# Usage: source load_env.sh

ENV_FILE=".env"
EXAMPLE_FILE=".env.example"

echo "=========================================="
echo "PPPoE Config Worker - Environment Loader"
echo "=========================================="

# Check if .env exists, if not create from .env.example
if [ ! -f "$ENV_FILE" ]; then
    if [ -f "$EXAMPLE_FILE" ]; then
        echo "⚠️  .env file not found. Creating from .env.example..."
        cp "$EXAMPLE_FILE" "$ENV_FILE"
        echo "✅ Created .env file. Please review and update values if needed."
    else
        echo "❌ Neither .env nor .env.example found!"
        return 1
    fi
fi

# Load environment variables
echo "📂 Loading environment variables from $ENV_FILE..."
echo ""

# Count variables
count=0

while IFS= read -r line; do
    # Skip empty lines and comments
    if [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]]; then
        continue
    fi
    
    # Export the variable
    export "$line"
    
    # Display (hide sensitive values)
    var_name="${line%%=*}"
    var_value="${line#*=}"
    
    if [[ "$var_name" =~ (PASSWORD|TOKEN|SECRET) ]]; then
        echo "  ✓ $var_name=***hidden***"
    else
        echo "  ✓ $var_name=$var_value"
    fi
    
    ((count++))
done < "$ENV_FILE"

echo ""
echo "=========================================="
echo "✅ Loaded $count environment variables"
echo "=========================================="
