#!/bin/bash

# Docker Build and Push Script for PPPoE Configuration Worker
# This script builds the Docker image and pushes it to Docker Hub

set -e  # Exit on any error

# Configuration
IMAGE_NAME="marcandres888/pppoe-config-worker"
TAG="${1:-latest}"  # Default to 'latest' if no tag provided

echo "=================================================="
echo "PPPoE Configuration Worker - Docker Build & Push"
echo "=================================================="
echo "Image: $IMAGE_NAME:$TAG"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if logged in to Docker Hub
if ! docker info 2>&1 | grep -q "Username"; then
    echo "⚠️  Not logged in to Docker Hub"
    read -p "Do you want to login now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker login
    else
        echo "❌ Please login to Docker Hub first: docker login"
        exit 1
    fi
fi

# Build the image
echo "🔨 Building Docker image..."
docker build -t "$IMAGE_NAME:$TAG" .

if [ $? -eq 0 ]; then
    echo "✅ Build successful!"
else
    echo "❌ Build failed!"
    exit 1
fi

# Push to Docker Hub
echo ""
echo "📤 Pushing to Docker Hub..."
docker push "$IMAGE_NAME:$TAG"

if [ $? -eq 0 ]; then
    echo "✅ Push successful!"
    echo ""
    echo "=================================================="
    echo "Image available at: $IMAGE_NAME:$TAG"
    echo "=================================================="
else
    echo "❌ Push failed!"
    exit 1
fi
