package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	// 服务监听配置。
	defaultHost    = "0.0.0.0"
	defaultPort    = "8800"
	defaultCountry = "US"
	// 商品转换默认配置。
	defaultDiscount        = 30.0
	defaultInventoryPolicy = "deny"
	defaultUpstreamTimeout = 120 * time.Second

	// Shoplus OpenAPI 配置，默认值可被环境变量覆盖。
	defaultShoplusURL    = "https://openapi.shoplus.net"
	defaultShoplusAppKey = "ukku58wPGD9EnsiWcRt39248AkJ5BXoH"

	// Amazon 上游抓取接口。
	upstreamEndpoint = "https://amazon-scraper-api.omkar.cloud/amazon/product-details"

	// 商品转换时的输出限制。
	maxShortDescItems           = 6
	maxProductImages            = 10
	defaultShoplusSubmitVersion = "1.0"
)

var (
	// 本地联调用死变量。
	// 部署时优先读取环境变量；如果不需要本地兜底，清空或删除即可。
	localAmazonAPIKeys = []string{}
	localShoplusSecret = ""

	asinPattern = regexp.MustCompile(`(?i)(?:dp|gp/product|product)/([A-Z0-9]{10})|(?:^|/)([A-Z0-9]{10})(?:[/?]|$)`)
	plainASIN   = regexp.MustCompile(`(?i)^[A-Z0-9]{10}$`)
	numberToken = regexp.MustCompile(`[-+]?\d*\.?\d+`)
	slugChars   = regexp.MustCompile(`[^a-z0-9]+`)
	keyCursor   uint64

	apiKeys    []string
	shoplusCfg = loadShoplusConfig()

	redirectClient = &http.Client{Timeout: 15 * time.Second}
	upstreamClient = &http.Client{Timeout: loadUpstreamTimeout()}
	shoplusClient  = &http.Client{Timeout: 30 * time.Second}
)

type errorResponse struct {
	Error string `json:"error"`
}

type shoplusConfig struct {
	URL             string
	AppKey          string
	Secret          string
	ProductDiscount float64
	InventoryPolicy string
	AutoSubmit      bool
}

type shoplusPushData struct {
	ID        int64  `json:"id"`
	SEOURL    string `json:"seoUrl"`
	ShopID    int64  `json:"shopId"`
	TaxStatus bool   `json:"taxStatus"`
}

type shoplusPushCallback struct {
	Code    string          `json:"code"`
	Msg     string          `json:"msg,omitempty"`
	Message string          `json:"message,omitempty"`
	Data    shoplusPushData `json:"data"`
	RawText string          `json:"rawText,omitempty"`
}

type productPushResponse struct {
	Product         map[string]any       `json:"product"`
	ShoplusPayload  map[string]any       `json:"shoplusPayload"`
	SubmitRequested bool                 `json:"submitRequested"`
	DiscountRate    float64              `json:"discountRate"`
	InventoryPolicy string               `json:"inventoryPolicy"`
	PushSuccess     bool                 `json:"pushSuccess"`
	PushError       string               `json:"pushError,omitempty"`
	PushCallback    *shoplusPushCallback `json:"pushCallback,omitempty"`
}

func main() {
	var err error
	apiKeys, err = loadAPIKeys()
	if err != nil {
		log.Fatal(err)
	}
	if len(apiKeys) == 0 {
		log.Println("AMAZON_API_KEYS/AMAZON_API_KEY 未配置，服务将以本地兼容模式启动；调用 Amazon 解析接口时会返回配置错误")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/amazon/product-details", withCORS(handleAmazonProductDetails))
	mux.HandleFunc("/api/amazon/product-import", withCORS(handleAmazonProductImport))
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
	log.Printf(
		"Server listening on http://%s (discount=%.2f%%, inventoryPolicy=%s, autoSubmit=%t, upstreamTimeout=%s)\n",
		addr,
		shoplusCfg.ProductDiscount,
		shoplusCfg.InventoryPolicy,
		shoplusCfg.AutoSubmit,
		upstreamClient.Timeout,
	)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func loadAPIKeys() ([]string, error) {
	raw := strings.TrimSpace(os.Getenv("AMAZON_API_KEYS"))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("AMAZON_API_KEY"))
	}
	keys := make([]string, 0)

	if raw != "" {
		parts := strings.Split(raw, ",")
		keys = make([]string, 0, len(parts))
		for _, part := range parts {
			key := strings.TrimSpace(part)
			if key != "" {
				keys = append(keys, key)
			}
		}
	} else {
		for _, key := range localAmazonAPIKeys {
			trimmed := strings.TrimSpace(key)
			if trimmed != "" {
				keys = append(keys, trimmed)
			}
		}
	}

	if len(keys) == 0 {
		return []string{}, nil
	}

	return keys, nil
}

func loadShoplusConfig() shoplusConfig {
	discount := defaultDiscount
	if raw := strings.TrimSpace(os.Getenv("SHOPLUS_PRODUCT_DISCOUNT")); raw != "" {
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil {
			discount = parsed
		}
	}

	urlValue := strings.TrimSpace(os.Getenv("SHOPLUS_API_URL"))
	if urlValue == "" {
		urlValue = defaultShoplusURL
	}

	appKey := strings.TrimSpace(os.Getenv("SHOPLUS_APP_KEY"))
	if appKey == "" {
		appKey = defaultShoplusAppKey
	}

	secret := strings.TrimSpace(os.Getenv("SHOPLUS_SECRET"))
	if secret == "" {
		secret = strings.TrimSpace(localShoplusSecret)
	}

	return shoplusConfig{
		URL:             urlValue,
		AppKey:          appKey,
		Secret:          secret,
		ProductDiscount: clamp(discount, 0, 100),
		InventoryPolicy: normalizeInventoryPolicy(os.Getenv("SHOPLUS_INVENTORY_POLICY")),
		AutoSubmit:      parseBoolDefaultTrue(os.Getenv("SHOPLUS_AUTO_SUBMIT")),
	}
}

func loadUpstreamTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv("AMAZON_UPSTREAM_TIMEOUT_SECONDS"))
	if raw == "" {
		return defaultUpstreamTimeout
	}

	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return defaultUpstreamTimeout
	}

	return time.Duration(seconds) * time.Second
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
			"product_import":  "/api/amazon/product-import?url=<amazon-link>",
			"health":          "/healthz",
		},
		"shoplus": map[string]any{
			"discount_rate":    shoplusCfg.ProductDiscount,
			"inventory_policy": shoplusCfg.InventoryPolicy,
			"auto_submit":      shoplusCfg.AutoSubmit,
		},
	})
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok",
		"shoplus": map[string]any{
			"discount_rate":    shoplusCfg.ProductDiscount,
			"inventory_policy": shoplusCfg.InventoryPolicy,
			"auto_submit":      shoplusCfg.AutoSubmit,
		},
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
	handleAmazonProduct(w, r, false)
}

func handleAmazonProductImport(w http.ResponseWriter, r *http.Request) {
	handleAmazonProduct(w, r, true)
}

func handleAmazonProduct(w http.ResponseWriter, r *http.Request, forceSubmit bool) {
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

	var product map[string]any
	if err := json.Unmarshal(payload, &product); err != nil {
		writeJSONError(w, http.StatusBadGateway, "failed to parse upstream product payload")
		return
	}

	shoplusPayload := sanitizeShoplusPayload(buildShoplusProductPayload(product, shoplusCfg))
	product["shoplus_payload"] = shoplusPayload
	product["shoplus_discount_rate"] = shoplusCfg.ProductDiscount
	product["shoplus_inventory_policy"] = shoplusCfg.InventoryPolicy

	shouldSubmit := resolveSubmitRequested(r, forceSubmit, shoplusCfg.AutoSubmit)
	product["shoplus_submit_requested"] = shouldSubmit
	if shouldSubmit {
		response := productPushResponse{
			Product:         product,
			ShoplusPayload:  shoplusPayload,
			SubmitRequested: true,
			DiscountRate:    shoplusCfg.ProductDiscount,
			InventoryPolicy: shoplusCfg.InventoryPolicy,
		}

		submitResult, submitErr := submitShoplusProduct(r.Context(), shoplusPayload, shoplusCfg)
		if submitResult != nil {
			response.PushCallback = submitResult
			response.PushSuccess = strings.TrimSpace(submitResult.Code) == "0" && submitErr == nil
		}
		if submitErr != nil {
			response.PushError = submitErr.Error()
		}

		writeJSON(w, http.StatusOK, response)
		return
	}

	writeJSON(w, http.StatusOK, product)
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
	lastStatus := http.StatusBadGateway

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

func buildShoplusProductPayload(product map[string]any, cfg shoplusConfig) map[string]any {
	productName := firstNonEmpty(getString(product, "product_name"), getString(product, "title"))
	asin := getString(product, "asin")
	parentASIN := firstNonEmpty(getString(product, "parent_asin"), asin)
	currencyCode := firstNonEmpty(getString(product, "currency"), "USD")

	brand := firstNonEmpty(
		getString(product, "brand"),
		getStringMapValue(product, "technical_details", "Brand"),
		getStringMapValue(product, "product_details", "Brand"),
		getString(product, "vendor"),
	)
	if brand == "" {
		brand = guessBrand(productName)
	}

	compareAtPrice := firstPositive(getFloat(product, "original_price"), getFloat(product, "current_price"))
	salePrice := calculateDiscountedPrice(compareAtPrice, cfg.ProductDiscount)
	if salePrice == 0 {
		salePrice = getFloat(product, "current_price")
	}
	if compareAtPrice == 0 {
		compareAtPrice = salePrice
	}

	options := buildProductOptions(product)
	images := buildProductImages(product)
	variants := buildProductVariants(product, salePrice, compareAtPrice, cfg.InventoryPolicy)
	longDesc := buildProductLongDesc(product)
	productType := buildProductType(product)

	seoKeywordRaw := strings.Join(buildSEOKeywords(product), ", ")
	seoURLRaw := buildSEOURL(productName, getString(product, "slug"))

	return map[string]any{
		"seoKeyword":               shorten(seoKeywordRaw, 255),
		"freightTemplateId":        "",
		"productOptions":           options,
		"specialReturnDesc":        "",
		"title":                    shorten(productName, 255),
		"seoTitle":                 shorten(productName, 255),
		"seoDesc":                  buildSEODescription(product, longDesc),
		"tags":                     "amazon-import," + strings.Join(buildTags(product, brand, productType), ","),
		"productShortDesc":         buildProductShortDesc(product),
		"productLongDesc":          longDesc,
		"taxStatus":                "",
		"seoUrl":                   shorten(seoURLRaw, 320),
		"spuRemark":                buildSPURemark(product),
		"productImages":            images,
		"productCustomizedOptions": []any{},
		"productSizeDesc":          buildProductSizeDesc(options),
		"vendor":                   brand,
		"brandId":                  "",
		"spuCode":                  parentASIN,
		"productVariants":          variants,
		"currencyCode":             currencyCode,
		"productType":              productType,
		"publishStatus":            "",
	}
}

func sanitizeShoplusPayload(payload map[string]any) map[string]any {
	return sanitizeAnyValue(payload).(map[string]any)
}

func sanitizeAnyValue(value any) any {
	switch typed := value.(type) {
	case string:
		// Shoplus 服务端会对 data 再做一次 URL decode，原始 % 会触发非法转义异常。
		return strings.ReplaceAll(typed, "%", "％")
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = sanitizeAnyValue(item)
		}
		return result
	case []any:
		result := make([]any, 0, len(typed))
		for _, item := range typed {
			result = append(result, sanitizeAnyValue(item))
		}
		return result
	case []map[string]any:
		result := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			sanitized, _ := sanitizeAnyValue(item).(map[string]any)
			result = append(result, sanitized)
		}
		return result
	case []string:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			result = append(result, strings.ReplaceAll(item, "%", "％"))
		}
		return result
	default:
		return value
	}
}

func submitShoplusProduct(ctx context.Context, payload map[string]any, cfg shoplusConfig) (*shoplusPushCallback, error) {
	if cfg.URL == "" || cfg.AppKey == "" || cfg.Secret == "" {
		return nil, errors.New("shoplus config is incomplete")
	}

	dataBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal shoplus payload failed: %w", err)
	}

	params := map[string]string{
		"app_key": cfg.AppKey,
		"name":    "products.add",
		"version": defaultShoplusSubmitVersion,
		"data":    string(dataBytes),
	}
	params["sign"] = generateSign(params, cfg.Secret)

	form := url.Values{}
	for key, value := range params {
		form.Set(key, value)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create shoplus request failed: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "amazon-sale-proxy/1.0")

	resp, err := shoplusClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("shoplus request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("failed to read shoplus response")
	}

	var result shoplusPushCallback
	if len(body) > 0 && json.Unmarshal(body, &result) == nil {
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if strings.TrimSpace(result.Code) == "0" {
				return &result, nil
			}

			message := firstNonEmpty(result.Msg, result.Message, "shoplus business error")
			return &result, fmt.Errorf("shoplus business error [%s]: %s", strings.TrimSpace(result.Code), message)
		}
		return &result, fmt.Errorf("shoplus request failed with status %d", resp.StatusCode)
	}

	text := strings.TrimSpace(string(body))
	result = shoplusPushCallback{
		RawText: text,
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return &result, nil
	}

	return &result, fmt.Errorf("shoplus request failed with status %d: %s", resp.StatusCode, text)
}

func generateSign(params map[string]string, secret string) string {
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var builder strings.Builder
	for _, key := range keys {
		value := params[key]
		if value == "" {
			continue
		}
		builder.WriteString(key)
		builder.WriteString(value)
	}

	source := secret + builder.String() + secret
	sum := md5.Sum([]byte(source))
	return strings.ToUpper(fmt.Sprintf("%x", sum))
}

func buildProductOptions(product map[string]any) []map[string]any {
	variantGroups := getMap(product, "variants")
	if len(variantGroups) == 0 {
		return []map[string]any{}
	}

	dimensions := getDimensions(product, variantGroups)
	options := make([]map[string]any, 0, len(dimensions))

	for index, dimension := range dimensions {
		items := getSliceFromMap(variantGroups, dimension)
		optionValues := make([]map[string]any, 0, len(items))
		position := 1

		for _, rawItem := range items {
			itemMap, ok := rawItem.(map[string]any)
			if !ok || !getBool(itemMap, "is_available") {
				continue
			}

			value := getString(itemMap, "value")
			if value == "" {
				continue
			}

			optionValues = append(optionValues, map[string]any{
				"imageUrl": getString(itemMap, "photo"),
				"position": strconv.Itoa(position),
				"value":    value,
			})
			position++
		}

		if len(optionValues) == 0 {
			continue
		}

		options = append(options, map[string]any{
			"productOptionValues": optionValues,
			"name":                dimension,
			"position":            strconv.Itoa(index + 1),
		})
	}

	return options
}

func buildProductImages(product map[string]any) []map[string]any {
	imageURLs := getStringSlice(product, "additional_image_urls")
	if len(imageURLs) == 0 {
		if mainImage := getString(product, "main_image_url"); mainImage != "" {
			imageURLs = append(imageURLs, mainImage)
		}
	}
	if len(imageURLs) > maxProductImages {
		imageURLs = imageURLs[:maxProductImages]
	}

	firstVideoURL := ""
	productVideos := getSlice(product, "product_videos")
	if len(productVideos) > 0 {
		if firstVideo, ok := productVideos[0].(map[string]any); ok {
			firstVideoURL = getString(firstVideo, "url")
		}
	}

	productName := firstNonEmpty(getString(product, "product_name"), "Amazon product")
	images := make([]map[string]any, 0, len(imageURLs))
	for index, imageURL := range imageURLs {
		videoURL := ""
		if index == 0 {
			videoURL = firstVideoURL
		}

		images = append(images, map[string]any{
			"imgUrl":     imageURL,
			"fileName":   extractFileName(imageURL),
			"attachment": "",
			"videoUrl":   videoURL,
			"alt":        fmt.Sprintf("%s image %d", productName, index+1),
			"position":   strconv.Itoa(index + 1),
			"title":      productName,
		})
	}

	return images
}

func buildProductVariants(product map[string]any, salePrice, compareAtPrice float64, inventoryPolicy string) []map[string]any {
	allVariants := getMap(product, "all_variants")
	if len(allVariants) == 0 {
		return buildFallbackVariants(product, salePrice, compareAtPrice, inventoryPolicy)
	}

	variantGroups := getMap(product, "variants")
	dimensions := getDimensions(product, variantGroups)
	allowedValues := buildAvailableValueLookup(variantGroups)
	colorImages := buildColorImageLookup(variantGroups)

	technicalDetails := getMap(product, "technical_details")
	gramsValue := extractGrams(getString(technicalDetails, "Item Weight"), getString(technicalDetails, "Item Weight "))
	weightValue := ""
	grams := ""
	weightUnit := ""
	if gramsValue > 0 {
		rounded := int(math.Round(gramsValue))
		grams = strconv.Itoa(rounded)
		weightValue = grams
		weightUnit = "g"
	}

	barcode := firstNonEmpty(getString(technicalDetails, "UPC"), getString(technicalDetails, "EAN"))
	mainImage := getString(product, "main_image_url")
	baseVariant := map[string]any{
		"availableStockQuantity": "",
		"isTrackInventory":       false,
		"salePrice":              formatMoney(salePrice),
		"inventoryPolicy":        normalizeInventoryPolicy(inventoryPolicy),
		"weight":                 weightValue,
		"purchasePrice":          "",
		"customizedOptionNames":  []any{},
		"skuRemark":              "",
		"skuBarcode":             barcode,
		"grams":                  grams,
		"compareAtPrice":         formatMoney(compareAtPrice),
		"weightUnit":             weightUnit,
	}

	asins := make([]string, 0, len(allVariants))
	for asin := range allVariants {
		asins = append(asins, asin)
	}
	sort.Strings(asins)

	variants := make([]map[string]any, 0, len(asins))
	for _, asin := range asins {
		rawVariant, ok := allVariants[asin].(map[string]any)
		if !ok || !isVariantAvailable(rawVariant, dimensions, allowedValues) {
			continue
		}

		optionValueNames := make([]string, 0, len(dimensions))
		for _, dimension := range dimensions {
			if value := getString(rawVariant, dimension); value != "" {
				optionValueNames = append(optionValueNames, value)
			}
		}

		imgURL := mainImage
		if color := getString(rawVariant, "color"); color != "" {
			if colorImage := colorImages[color]; colorImage != "" {
				imgURL = colorImage
			}
		}

		variant := cloneMap(baseVariant)
		variant["optionValueNames"] = optionValueNames
		variant["imgUrl"] = imgURL
		variant["skuCode"] = asin
		variants = append(variants, variant)
	}

	if len(variants) == 0 {
		return buildFallbackVariants(product, salePrice, compareAtPrice, inventoryPolicy)
	}

	return variants
}

func buildFallbackVariants(product map[string]any, salePrice, compareAtPrice float64, inventoryPolicy string) []map[string]any {
	technicalDetails := getMap(product, "technical_details")
	gramsValue := extractGrams(getString(technicalDetails, "Item Weight"))
	weightValue := ""
	grams := ""
	weightUnit := ""
	if gramsValue > 0 {
		rounded := int(math.Round(gramsValue))
		grams = strconv.Itoa(rounded)
		weightValue = grams
		weightUnit = "g"
	}

	return []map[string]any{
		{
			"availableStockQuantity": "",
			"isTrackInventory":       false,
			"salePrice":              formatMoney(salePrice),
			"optionValueNames":       selectedOptionNames(product),
			"inventoryPolicy":        normalizeInventoryPolicy(inventoryPolicy),
			"weight":                 weightValue,
			"purchasePrice":          "",
			"customizedOptionNames":  []any{},
			"imgUrl":                 getString(product, "main_image_url"),
			"skuRemark":              "",
			"skuBarcode":             getString(technicalDetails, "UPC"),
			"grams":                  grams,
			"skuCode":                getString(product, "asin"),
			"compareAtPrice":         formatMoney(compareAtPrice),
			"weightUnit":             weightUnit,
		},
	}
}

func buildProductShortDesc(product map[string]any) string {
	details := getMap(product, "product_details")
	if len(details) == 0 {
		details = getMap(product, "technical_details")
	}
	if len(details) == 0 {
		return "[]"
	}

	preferredKeys := []string{
		"Brand",
		"Model Number",
		"Operating System",
		"Screen Size",
		"Battery Capacity",
		"Connectivity Technology",
		"Special Feature",
		"Memory Storage Capacity",
	}

	items := make([]map[string]string, 0, maxShortDescItems)
	seen := map[string]bool{}

	for _, key := range preferredKeys {
		value := getString(details, key)
		if value == "" || seen[key] {
			continue
		}
		items = append(items, map[string]string{"key": key, "value": value})
		seen[key] = true
		if len(items) >= maxShortDescItems {
			break
		}
	}

	if len(items) < maxShortDescItems {
		keys := make([]string, 0, len(details))
		for key := range details {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		for _, key := range keys {
			value := getString(details, key)
			if value == "" || seen[key] {
				continue
			}
			items = append(items, map[string]string{"key": key, "value": value})
			if len(items) >= maxShortDescItems {
				break
			}
		}
	}

	encoded, err := json.Marshal(items)
	if err != nil {
		return "[]"
	}
	return string(encoded)
}

func buildProductLongDesc(product map[string]any) string {
	if fullDescription := strings.TrimSpace(getString(product, "full_description")); fullDescription != "" {
		return fullDescription
	}

	keyFeatures := getStringSlice(product, "key_features")
	if len(keyFeatures) == 0 {
		return ""
	}
	return strings.Join(keyFeatures, "\n")
}

func buildSEODescription(product map[string]any, longDesc string) string {
	if strings.TrimSpace(longDesc) != "" {
		return shorten(longDesc, 320)
	}
	return shorten(getString(product, "product_name"), 320)
}

func buildSEOKeywords(product map[string]any) []string {
	keywords := []string{
		getString(product, "brand"),
		getString(product, "product_name"),
		getString(product, "slug"),
	}

	options := buildProductOptions(product)
	for _, option := range options {
		if name, ok := option["name"].(string); ok && name != "" {
			keywords = append(keywords, name)
		}
		switch values := option["productOptionValues"].(type) {
		case []map[string]any:
			for _, value := range values {
				keywords = append(keywords, toString(value["value"]))
			}
		case []any:
			for _, raw := range values {
				if valueMap, ok := raw.(map[string]any); ok {
					keywords = append(keywords, toString(valueMap["value"]))
				}
			}
		}
	}

	return uniqueNonEmpty(keywords)
}

func buildTags(product map[string]any, brand, productType string) []string {
	tags := []string{brand, productType}

	for _, raw := range getSlice(product, "category_hierarchy") {
		if item, ok := raw.(map[string]any); ok {
			tags = append(tags, getString(item, "name"))
		}
	}

	if salesVolume := getString(product, "sales_volume"); salesVolume != "" {
		tags = append(tags, salesVolume)
	}

	return uniqueNonEmpty(tags)
}

func buildProductType(product map[string]any) string {
	hierarchy := getSlice(product, "category_hierarchy")
	for idx := len(hierarchy) - 1; idx >= 0; idx-- {
		if item, ok := hierarchy[idx].(map[string]any); ok {
			if name := getString(item, "name"); name != "" {
				return name
			}
		}
	}

	if mainCategory := getMap(product, "main_category"); len(mainCategory) > 0 {
		return getString(mainCategory, "name")
	}

	return ""
}

func buildProductSizeDesc(options []map[string]any) string {
	for _, option := range options {
		name, _ := option["name"].(string)
		if !strings.EqualFold(name, "size") {
			continue
		}

		parts := []string{}
		switch values := option["productOptionValues"].(type) {
		case []map[string]any:
			for _, value := range values {
				parts = append(parts, toString(value["value"]))
			}
		case []any:
			for _, raw := range values {
				if valueMap, ok := raw.(map[string]any); ok {
					parts = append(parts, toString(valueMap["value"]))
				}
			}
		}

		if len(parts) > 0 {
			return "Available sizes: " + strings.Join(uniqueNonEmpty(parts), ", ")
		}
	}

	return ""
}

func buildSPURemark(product map[string]any) string {
	parts := []string{}
	if asin := getString(product, "asin"); asin != "" {
		parts = append(parts, "Amazon ASIN: "+asin)
	}
	if parentASIN := getString(product, "parent_asin"); parentASIN != "" {
		parts = append(parts, "Parent ASIN: "+parentASIN)
	}
	if country := getString(product, "country"); country != "" {
		parts = append(parts, "Country: "+country)
	}
	return strings.Join(parts, "; ")
}

func buildSEOURL(productName, slug string) string {
	if slug = strings.TrimSpace(slug); slug != "" {
		return slug
	}

	normalized := strings.ToLower(strings.TrimSpace(productName))
	normalized = slugChars.ReplaceAllString(normalized, "-")
	normalized = strings.Trim(normalized, "-")
	return normalized
}

func buildColorImageLookup(variantGroups map[string]any) map[string]string {
	lookup := map[string]string{}
	for _, rawItem := range getSliceFromMap(variantGroups, "color") {
		itemMap, ok := rawItem.(map[string]any)
		if !ok {
			continue
		}
		color := getString(itemMap, "value")
		imageURL := getString(itemMap, "photo")
		if color != "" && imageURL != "" {
			lookup[color] = imageURL
		}
	}
	return lookup
}

func buildAvailableValueLookup(variantGroups map[string]any) map[string]map[string]bool {
	lookup := map[string]map[string]bool{}
	for dimension, rawItems := range variantGroups {
		items, ok := rawItems.([]any)
		if !ok {
			continue
		}
		for _, rawItem := range items {
			itemMap, ok := rawItem.(map[string]any)
			if !ok || !getBool(itemMap, "is_available") {
				continue
			}
			value := getString(itemMap, "value")
			if value == "" {
				continue
			}
			if lookup[dimension] == nil {
				lookup[dimension] = map[string]bool{}
			}
			lookup[dimension][value] = true
		}
	}
	return lookup
}

func isVariantAvailable(variant map[string]any, dimensions []string, allowed map[string]map[string]bool) bool {
	for _, dimension := range dimensions {
		value := getString(variant, dimension)
		if value == "" || len(allowed[dimension]) == 0 {
			continue
		}
		if !allowed[dimension][value] {
			return false
		}
	}
	return true
}

func selectedOptionNames(product map[string]any) []string {
	allVariants := getMap(product, "all_variants")
	if len(allVariants) > 0 {
		if currentASIN := getString(product, "asin"); currentASIN != "" {
			if currentVariant, ok := allVariants[currentASIN].(map[string]any); ok {
				dimensions := getDimensions(product, getMap(product, "variants"))
				result := make([]string, 0, len(dimensions))
				for _, dimension := range dimensions {
					if value := getString(currentVariant, dimension); value != "" {
						result = append(result, value)
					}
				}
				return result
			}
		}
	}

	details := getMap(product, "technical_details")
	return uniqueNonEmpty([]string{
		getString(details, "Color"),
		getString(details, "Style Name"),
	})
}

func getDimensions(product map[string]any, variantGroups map[string]any) []string {
	dimensions := getStringSlice(product, "variation_dimensions")
	if len(dimensions) > 0 {
		return dimensions
	}

	keys := make([]string, 0, len(variantGroups))
	for key := range variantGroups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func getMap(value map[string]any, key string) map[string]any {
	raw, ok := value[key]
	if !ok || raw == nil {
		return map[string]any{}
	}
	if parsed, ok := raw.(map[string]any); ok {
		return parsed
	}
	return map[string]any{}
}

func getStringMapValue(root map[string]any, mapKey, itemKey string) string {
	return getString(getMap(root, mapKey), itemKey)
}

func getSlice(value map[string]any, key string) []any {
	raw, ok := value[key]
	if !ok || raw == nil {
		return []any{}
	}
	if parsed, ok := raw.([]any); ok {
		return parsed
	}
	return []any{}
}

func getSliceFromMap(value map[string]any, key string) []any {
	raw, ok := value[key]
	if !ok || raw == nil {
		return []any{}
	}
	if parsed, ok := raw.([]any); ok {
		return parsed
	}
	return []any{}
}

func getStringSlice(value map[string]any, key string) []string {
	items := getSlice(value, key)
	result := make([]string, 0, len(items))
	for _, item := range items {
		if str := strings.TrimSpace(toString(item)); str != "" {
			result = append(result, str)
		}
	}
	return result
}

func getString(value map[string]any, key string) string {
	return strings.TrimSpace(toString(value[key]))
}

func getFloat(value map[string]any, key string) float64 {
	return toFloat(value[key])
}

func getBool(value map[string]any, key string) bool {
	raw, exists := value[key]
	if !exists {
		return true
	}
	switch typed := raw.(type) {
	case bool:
		return typed
	case string:
		trimmed := strings.TrimSpace(strings.ToLower(typed))
		if trimmed == "" {
			return true
		}
		return trimmed == "true" || trimmed == "1" || trimmed == "yes" || trimmed == "y"
	case float64:
		return typed != 0
	default:
		return true
	}
}

func toString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 64)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case uint32:
		return strconv.FormatUint(uint64(typed), 10)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

func toFloat(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case json.Number:
		if v, err := typed.Float64(); err == nil {
			return v
		}
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0
		}
		if v, err := strconv.ParseFloat(trimmed, 64); err == nil {
			return v
		}
	}
	return 0
}

func extractGrams(values ...string) float64 {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}

		number := firstNumber(value)
		if number == 0 {
			continue
		}

		lower := strings.ToLower(value)
		switch {
		case strings.Contains(lower, "kg"), strings.Contains(lower, "kilogram"):
			return number * 1000
		case strings.Contains(lower, "ounce"), strings.Contains(lower, "oz"):
			return number * 28.3495
		case strings.Contains(lower, "pound"), strings.Contains(lower, "lb"):
			return number * 453.592
		default:
			return number
		}
	}
	return 0
}

func firstNumber(value string) float64 {
	match := numberToken.FindString(value)
	if match == "" {
		return 0
	}
	parsed, err := strconv.ParseFloat(match, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func calculateDiscountedPrice(price, discount float64) float64 {
	if price <= 0 {
		return 0
	}
	return roundTo(price*(1-clamp(discount, 0, 100)/100), 2)
}

func roundTo(value float64, precision int) float64 {
	pow := math.Pow(10, float64(precision))
	return math.Round(value*pow) / pow
}

func clamp(value, minValue, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

func firstPositive(values ...float64) float64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func uniqueNonEmpty(values []string) []string {
	result := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		result = append(result, trimmed)
	}
	return result
}

func cloneMap(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func normalizeInventoryPolicy(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "continue":
		return "continue"
	default:
		return defaultInventoryPolicy
	}
}

func parseBool(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func parseBoolDefaultTrue(value string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	if trimmed == "" {
		return true
	}
	switch trimmed {
	case "0", "false", "no", "n", "off":
		return false
	default:
		return true
	}
}

func resolveSubmitRequested(r *http.Request, forceSubmit bool, defaultValue bool) bool {
	if forceSubmit {
		return true
	}

	raw := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("submit")))
	if raw == "" {
		return defaultValue
	}

	switch raw {
	case "0", "false", "no", "n", "off":
		return false
	default:
		return true
	}
}

func formatMoney(value float64) string {
	if value <= 0 {
		return ""
	}
	return fmt.Sprintf("%.2f", roundTo(value, 2))
}

func shorten(value string, limit int) string {
	trimmed := strings.TrimSpace(value)
	runes := []rune(trimmed)
	if len(runes) <= limit {
		return trimmed
	}
	suffix := "..."
	if limit <= len(suffix) {
		return string(runes[:limit])
	}
	return strings.TrimSpace(string(runes[:limit-len(suffix)])) + suffix
}

func extractFileName(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}

	parts := strings.Split(strings.TrimSpace(parsed.Path), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

func guessBrand(productName string) string {
	parts := strings.Fields(strings.TrimSpace(productName))
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

func writeJSONError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: message})
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(payload)
}
