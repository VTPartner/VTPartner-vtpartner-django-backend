#!/bin/bash

# Colors for messages
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'  # No color

# GitHub credentials
USERNAME="VTPartner"
PASSWORD="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"

# Repository URL
REPO_URL="https://github.com/VTPartner/VTPartner-vtpartner-django-backend.git"

# Function to handle errors
handle_error() {
    echo -e "${RED}Error: $1${NC}"
    exit 1
}

# Pull new changes
echo -e "${GREEN}Pulling new changes...${NC}"
git pull || handle_error "Failed to pull changes."

# Prompt for commit message
read -p "Enter commit message: " COMMIT_MSG

# Add changes
echo -e "${GREEN}Adding changes...${NC}"
git add . || handle_error "Failed to add changes."

# Commit changes
echo -e "${GREEN}Committing changes...${NC}"
git commit -m "$COMMIT_MSG" || handle_error "Failed to commit changes."

# Push changes
echo -e "${GREEN}Pushing changes...${NC}"
#git push https://$USERNAME:$PASSWORD@$REPO_URL || handle_error "Failed to push changes."
git push -u origin master
echo -e "${GREEN}Operation completed successfully.${NC}"


