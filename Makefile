.DEFAULT_GOAL := help

##@ Development

.PHONY: tidy
tidy: ## Run go mod tidy
	go mod tidy

.PHONY: format
format: ## Format code with gofumpt + goimports
	@which gofumpt > /dev/null || go install mvdan.cc/gofumpt@latest
	@which goimports > /dev/null || go install golang.org/x/tools/cmd/goimports@latest
	gofumpt -w .
	goimports -w .

.PHONY: lint
lint: ## Run golangci-lint
	@which golangci-lint > /dev/null || (echo "install golangci-lint: https://golangci-lint.run/usage/install/" && exit 1)
	golangci-lint run ./...

.PHONY: vet
vet: ## Run go vet
	go vet ./...

##@ Testing

.PHONY: test
test: test-unit ## Run all tests

.PHONY: test-unit
test-unit: ## Run unit tests
	go test -tags unit -count=1 -race -timeout 30s ./...

.PHONY: test-integration
test-integration: ## Run integration tests (requires running Consul)
	go test -tags integration -count=1 -timeout 60s ./...

.PHONY: test-cover
test-cover: ## Run unit tests with coverage report
	go test -tags unit -count=1 -race -timeout 30s -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

##@ Services

.PHONY: up
up: ## Start consul + services via docker-compose
	docker compose up --build

.PHONY: down
down: ## Stop and remove containers
	docker compose down -v

.PHONY: logs
logs: ## Tail service logs
	docker compose logs -f

##@ CI

.PHONY: ci
ci: tidy vet lint test-unit ## Full CI pipeline (no integration tests)

##@ Help

.PHONY: help
help: ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
