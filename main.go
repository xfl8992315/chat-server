package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

const (
	defaultHost      = "0.0.0.0"
	defaultPort      = "8800"
	defaultCountry   = "US"
	upstreamEndpoint = "https://amazon-scraper-api.omkar.cloud/amazon/product-details"
)

var (
	asinPattern = regexp.MustCompile(`(?i)(?:dp|gp/product|product)/([A-Z0-9]{10})|(?:^|/)([A-Z0-9]{10})(?:[/?]|$)`)
	plainASIN   = regexp.MustCompile(`(?i)^[A-Z0-9]{10}$`)
	keyCursor   uint64

	apiKeys []string

	redirectClient = &http.Client{
		Timeout: 15 * time.Second,
	}

	upstreamClient = &http.Client{
		Timeout: 30 * time.Second,
	}
)

type errorResponse struct {
	Error string `json:"error"`
}

func main() {
	var err error
	apiKeys, err = loadAPIKeys()
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/amazon/product-details", withCORS(handleAmazonProductDetails))
	mux.HandleFunc("/healthz", withCORS(handleHealthz))
	mux.HandleFunc("/", withCORS(handleRoot))

	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	host := os.Getenv("HOST")
	if host == "" {
		host = defaultHost
	}

	addr := host + ":" + port
	log.Printf("Server listening on http://%s\n", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func loadAPIKeys() ([]string, error) {
	raw := strings.TrimSpace(os.Getenv("AMAZON_API_KEYS"))
	if raw == "" {
		return nil, errors.New("AMAZON_API_KEYS is not configured")
	}

	parts := strings.Split(raw, ",")
	keys := make([]string, 0, len(parts))
	for _, part := range parts {
		key := strings.TrimSpace(part)
		if key != "" {
			keys = append(keys, key)
		}
	}

	if len(keys) == 0 {
		return nil, errors.New("AMAZON_API_KEYS is empty after parsing")
	}

	return keys, nil
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeJSONError(w, http.StatusNotFound, "not found")
		return
	}

	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"service": "amazon-product-proxy",
		"status":  "ok",
		"routes": map[string]string{
			"product_details": "/api/amazon/product-details?url=<amazon-link>",
			"health":          "/healthz",
		},
	})
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next(w, r)
	}
}

func handleAmazonProductDetails(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	rawURL := strings.TrimSpace(r.URL.Query().Get("url"))
	asin := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("asin")))
	country := strings.TrimSpace(r.URL.Query().Get("country_code"))
	if country == "" {
		country = defaultCountry
	}

	if asin == "" {
		if rawURL == "" {
			writeJSONError(w, http.StatusBadRequest, "missing url or asin")
			return
		}

		resolvedASIN, err := resolveASIN(r.Context(), rawURL)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		asin = resolvedASIN
	}

	payload, statusCode, err := fetchProductDetails(r.Context(), asin, country)
	if err != nil {
		writeJSONError(w, statusCode, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(payload)
}

func resolveASIN(ctx context.Context, input string) (string, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", errors.New("empty product link")
	}

	if plainASIN.MatchString(trimmed) {
		return strings.ToUpper(trimmed), nil
	}

	if asin := extractASIN(trimmed); asin != "" {
		return asin, nil
	}

	finalURL, err := resolveRedirectURL(ctx, trimmed)
	if err != nil {
		return "", err
	}

	if asin := extractASIN(finalURL); asin != "" {
		return asin, nil
	}

	return "", errors.New("unable to resolve asin from the provided link")
}

func extractASIN(value string) string {
	match := asinPattern.FindStringSubmatch(value)
	if len(match) == 0 {
		return ""
	}

	for _, candidate := range match[1:] {
		if candidate != "" {
			return strings.ToUpper(candidate)
		}
	}

	return ""
}

func resolveRedirectURL(ctx context.Context, raw string) (string, error) {
	parsedURL, err := normalizeURL(raw)
	if err != nil {
		return "", errors.New("invalid product link")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsedURL, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

	resp, err := redirectClient.Do(req)
	if err != nil {
		return "", errors.New("unable to resolve short link")
	}
	defer resp.Body.Close()

	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return resp.Request.URL.String(), nil
}

func normalizeURL(raw string) (string, error) {
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		_, err := url.Parse(raw)
		return raw, err
	}

	normalized := "https://" + raw
	_, err := url.Parse(normalized)
	return normalized, err
}

func fetchProductDetails(ctx context.Context, asin, country string) ([]byte, int, error) {
	if len(apiKeys) == 0 {
		return nil, http.StatusInternalServerError, errors.New("no upstream api keys configured")
	}

	start := int(atomic.AddUint64(&keyCursor, 1)-1) % len(apiKeys)
	var lastErr error
	var lastStatus = http.StatusBadGateway

	for attempt := 0; attempt < len(apiKeys); attempt++ {
		key := apiKeys[(start+attempt)%len(apiKeys)]
		body, statusCode, err := fetchWithKey(ctx, key, asin, country)
		if err == nil {
			return body, http.StatusOK, nil
		}

		lastErr = err
		lastStatus = statusCode
	}

	if lastErr == nil {
		lastErr = errors.New("upstream request failed")
	}

	return nil, lastStatus, lastErr
}

func fetchWithKey(ctx context.Context, apiKey, asin, country string) ([]byte, int, error) {
	u, err := url.Parse(upstreamEndpoint)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	query := u.Query()
	query.Set("asin", asin)
	query.Set("country_code", country)
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	req.Header.Set("API-Key", apiKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "amazon-sale-proxy/1.0")

	resp, err := upstreamClient.Do(req)
	if err != nil {
		return nil, http.StatusBadGateway, fmt.Errorf("upstream request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, http.StatusBadGateway, errors.New("failed to read upstream response")
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return body, http.StatusOK, nil
	}

	return nil, mapUpstreamStatus(resp.StatusCode), fmt.Errorf("upstream request failed with status %d: %s", resp.StatusCode, string(body))
}

func mapUpstreamStatus(status int) int {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return http.StatusBadGateway
	case status == http.StatusTooManyRequests:
		return http.StatusTooManyRequests
	case status >= 400 && status < 500:
		return http.StatusBadRequest
	default:
		return http.StatusBadGateway
	}
}

func writeJSONError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: message})
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
