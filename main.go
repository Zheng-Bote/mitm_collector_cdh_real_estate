/**
 * SPDX-FileComment: CDH Real Estate Collector
 * SPDX-FileType: SOURCE
 * SPDX-FileContributor: ZHENG Robert
 * SPDX-FileCopyrightText: 2026 ZHENG Robert
 * SPDX-License-Identifier: Apache-2.0
 *
 * @file main.go
 * @brief Collects the CDH real-estate hierarchy (locations > buildings > levels > rooms) per country,
 *        flattens it into one row per entity, encrypts every row and stores it in the RAW table.
 * @version 0.1.0
 * @date 2026-11-10
 *
 * @author ZHENG Robert (robert@hase-zheng.net)
 * @copyright Copyright (c) 2026 ZHENG Robert
 * @license Apache-2.0
 */

package main

import (
	"bufio"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	identifierRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	countryRegex    = regexp.MustCompile(`^[A-Z]{2}$`)
	appName         = "CDH Real Estate Collector"
	version         = "0.1.0"

	// retryBackoff is the wait before the second attempt; it doubles per attempt (5/10/20/40 s).
	retryBackoff = 5 * time.Second
)

const (
	componentName   = "mitm_collector_cdh_real_estate"
	defaultTopic    = "cdh.real_estate.data"
	defaultPageSize = 1000 // API maximum
	defaultTimeout  = 60 * time.Second
	maxAttempts     = 4
	maxBatchSize    = 1000
	maxResponseSize = 100 << 20
)

// metaColumns are kept only on location level; they are identical for every level of one row.
var metaColumns = map[string]bool{"data_source": true, "ingest_date": true}

// TableNames are the CDH table names of the four hierarchy levels.
type TableNames struct {
	Locations string `json:"locations"`
	Buildings string `json:"buildings"`
	Levels    string `json:"levels"`
	Rooms     string `json:"rooms"`
}

// CollectorArgs are the job arguments passed by the scheduler as JSON in os.Args[1].
type CollectorArgs struct {
	SourceName         string     `json:"source_name"`
	Topic              string     `json:"topic"`
	DatasetID          string     `json:"dataset_id"`
	Tables             TableNames `json:"tables"`
	Countries          []string   `json:"countries"`
	BulkChildren       bool       `json:"bulk_children"`
	MaxPages           int        `json:"max_pages"`
	InsecureSkipVerify bool       `json:"insecure_skip_verify"`
}

// SourceConfig is the decrypted content of source_credentials.config_payload.
type SourceConfig struct {
	ClientID       string `json:"client_id"`
	ClientSecret   string `json:"client_secret"`
	AuthURL        string `json:"auth_url"`
	EndpointURL    string `json:"endpoint_url"`
	Hub            string `json:"hub"`
	Region         string `json:"region"`
	Stage          string `json:"stage"`
	PageSize       int    `json:"page_size"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

// TargetDBConfig defines the MitM target database connection.
type TargetDBConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

// StatusEvent is sent to the scheduler Unix socket.
type StatusEvent struct {
	RunID     int    `json:"run_id"`
	Type      string `json:"type"` // "status" or "audit"
	Component string `json:"component"`
	Status    string `json:"status"`
	Message   string `json:"message"`
	Progress  int    `json:"progress"`
}

// IPCClient sends events to the scheduler; a nil client is a no-op (standalone execution).
type IPCClient struct {
	SocketPath string
	RunID      int
	Component  string
	Topic      string
	SourceName string
}

// Row is one record of a CDH table or one flat row.
type Row map[string]any

// level describes one level of the real-estate hierarchy.
type level struct {
	table  string
	pk     string
	fk     string // join key to the parent level; empty for locations
	prefix string
}

func hierarchy(tables TableNames) []level {
	return []level{
		{tables.Locations, "location_code", "", "loc_"},
		{tables.Buildings, "building_code", "location_code", "bld_"},
		{tables.Levels, "level_code", "building_code", "lvl_"},
		{tables.Rooms, "room_code", "level_code", "room_"},
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.LUTC)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)
	cancel()
	if err != nil {
		log.Printf("%s failed: %v", appName, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	if dbCfg, masterKey, err := fetchCredentialsFromScheduler(); err == nil {
		if dbCfg != "" {
			os.Setenv("MITM_DB_CONFIG_JSON", dbCfg)
		}
		if masterKey != "" {
			os.Setenv("MASTER_KEY", masterKey)
		}
	} else if os.Getenv("RUN_ID") != "" && os.Getenv("SCHEDULER_SOCKET_PATH") != "" {
		log.Printf("[IPC Warning] Failed to get credentials from scheduler: %v", err)
	}

	version = strings.Split(version, "-")[0]
	ipc := newIPCClient()
	ipc.SendEvent("started", fmt.Sprintf("%s (%s) started", appName, version), 0)
	ipc.SendAudit(fmt.Sprintf("%s (%s) started", appName, version))

	rawArgs := ""
	if len(os.Args) >= 2 {
		rawArgs = os.Args[1]
	}
	args, err := parseArgs(rawArgs)
	if err != nil {
		return fail(ipc, err)
	}
	if ipc != nil {
		ipc.Topic = args.Topic
		ipc.SourceName = args.SourceName
	}

	targetCfg, configSource, err := loadTargetDBConfig()
	if err != nil {
		return fail(ipc, err)
	}
	ipc.SendAudit("Loaded database configuration from " + configSource)

	kek, err := validateKEK(os.Getenv("MASTER_KEY"))
	if err != nil {
		return fail(ipc, err)
	}

	pool, err := connectTargetDB(ctx, targetCfg)
	if err != nil {
		return fail(ipc, fmt.Errorf("Failed to connect to MitM database: %w", err))
	}
	defer pool.Close()
	ipc.SendEvent("processing", "Connected to MitM database", 20)

	configPayload, credentialsNonce, dekID, wrappedDEK, err := loadSourceSecrets(ctx, pool, args.SourceName)
	if err != nil {
		return fail(ipc, err)
	}
	dek, err := unwrapDEK(kek, wrappedDEK)
	if err != nil {
		return fail(ipc, errors.New("Failed to decrypt wrapped DEK (KEK mismatch or corrupted key data)"))
	}
	dekGCM, err := newGCM(dek)
	if err != nil {
		return fail(ipc, fmt.Errorf("Failed to initialize GCM with DEK: %w", err))
	}
	configBytes, err := decrypt(dekGCM, credentialsNonce, configPayload)
	if err != nil {
		return fail(ipc, errors.New("Failed to decrypt source config payload using DEK"))
	}
	ipc.SendAudit("Decrypted CDH connection credentials payload successfully")

	sourceCfg, err := parseSourceConfig(configBytes)
	if err != nil {
		return fail(ipc, err)
	}

	if args.InsecureSkipVerify {
		ipc.SendAudit("WARNING: TLS certificate verification is disabled (insecure_skip_verify)")
	}
	client := newCDHClient(sourceCfg, args, ipc)
	if err := client.refreshToken(ctx); err != nil {
		return fail(ipc, err)
	}
	ipc.SendEvent("processing", "Authenticated at CDH API", 50)

	var rows []Row
	for _, country := range args.Countries {
		countryRows, err := client.collectCountry(ctx, country)
		if err != nil {
			return fail(ipc, err)
		}
		rows = append(rows, countryRows...)
	}
	if len(client.skipped) > 0 {
		ipc.SendAudit(fmt.Sprintf("WARNING: %d requests were skipped, data is incomplete", len(client.skipped)))
	}

	ipc.SendEvent("processing", "Encrypting and ingesting flat rows", 70)
	cursor, err := fingerprint(rows)
	if err != nil {
		return fail(ipc, err)
	}
	lastCursor, err := loadCursor(ctx, pool, args.SourceName)
	if err != nil {
		return fail(ipc, err)
	}
	if cursor == lastCursor {
		ipc.SendAudit(fmt.Sprintf("Snapshot is unchanged (%d flat rows); no record ingested", len(rows)))
	} else {
		if err := ingestRows(ctx, pool, args, rows, dekGCM, dekID, cursor); err != nil {
			return fail(ipc, err)
		}
		ipc.SendAudit(fmt.Sprintf("Ingested %d flat rows. Cursor updated to %s.", len(rows), cursor))
	}

	ipc.SendAudit(fmt.Sprintf("%s (%s) finished", appName, version))
	log.Printf("%s finished. Flat rows: %d, skipped requests: %d.", appName, len(rows), len(client.skipped))
	return nil
}

// fail reports a fatal error as status event and job audit entry; the caller exits with code 1.
func fail(ipc *IPCClient, err error) error {
	ipc.SendEvent("failed", err.Error(), 0)
	ipc.SendAudit("ERROR: " + err.Error())
	return err
}

func parseArgs(raw string) (CollectorArgs, error) {
	var args CollectorArgs
	if strings.TrimSpace(raw) == "" {
		return args, errors.New("collector arguments are required (JSON in the first argument)")
	}
	if err := json.Unmarshal([]byte(raw), &args); err != nil {
		return args, fmt.Errorf("Failed to parse collector arguments: %w", err)
	}
	if args.Topic == "" {
		args.Topic = defaultTopic
	}
	switch {
	case strings.TrimSpace(args.SourceName) == "":
		return args, errors.New("source_name is required in the collector arguments")
	case strings.TrimSpace(args.DatasetID) == "":
		return args, errors.New("dataset_id is required in the collector arguments")
	case len(args.Countries) == 0:
		return args, errors.New("countries is required in the collector arguments")
	case args.MaxPages < 0:
		return args, errors.New("max_pages must not be negative")
	}
	for name, table := range map[string]string{
		"locations": args.Tables.Locations, "buildings": args.Tables.Buildings,
		"levels": args.Tables.Levels, "rooms": args.Tables.Rooms,
	} {
		if !identifierRegex.MatchString(table) {
			return args, fmt.Errorf("tables.%s is missing or has an invalid format", name)
		}
	}
	for i, country := range args.Countries {
		country = strings.ToUpper(strings.TrimSpace(country))
		if !countryRegex.MatchString(country) {
			return args, fmt.Errorf("invalid country code %q (ISO-3166 alpha-2 expected)", args.Countries[i])
		}
		args.Countries[i] = country
	}
	return args, nil
}

func parseSourceConfig(raw []byte) (SourceConfig, error) {
	var cfg SourceConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("Failed to parse decrypted CDH configuration: %w", err)
	}
	for name, value := range map[string]string{
		"client_id": cfg.ClientID, "client_secret": cfg.ClientSecret, "auth_url": cfg.AuthURL,
		"endpoint_url": cfg.EndpointURL, "hub": cfg.Hub, "region": cfg.Region, "stage": cfg.Stage,
	} {
		if strings.TrimSpace(value) == "" {
			return cfg, fmt.Errorf("source configuration is missing %s", name)
		}
	}
	if cfg.PageSize == 0 {
		cfg.PageSize = defaultPageSize
	}
	if cfg.PageSize < 1 || cfg.PageSize > defaultPageSize {
		return cfg, fmt.Errorf("source configuration page_size must be between 1 and %d", defaultPageSize)
	}
	if cfg.TimeoutSeconds < 0 {
		return cfg, errors.New("source configuration timeout_seconds must not be negative")
	}
	return cfg, nil
}

// --- CDH API ---------------------------------------------------------------

// httpStatusError is returned for a non-2xx API response.
type httpStatusError struct {
	table  string
	status int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("CDH request for table %s returned HTTP %d", e.table, e.status)
}

// transientError marks timeouts and connection errors that are retried.
type transientError struct{ err error }

func (e *transientError) Error() string { return e.err.Error() }
func (e *transientError) Unwrap() error { return e.err }

type cdhClient struct {
	http    *http.Client
	cfg     SourceConfig
	args    CollectorArgs
	ipc     *IPCClient
	token   string
	skipped []string
}

func newCDHClient(cfg SourceConfig, args CollectorArgs, ipc *IPCClient) *cdhClient {
	timeout := defaultTimeout
	if cfg.TimeoutSeconds > 0 {
		timeout = time.Duration(cfg.TimeoutSeconds) * time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if args.InsecureSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &cdhClient{
		http: &http.Client{Timeout: timeout, Transport: transport},
		cfg:  cfg, args: args, ipc: ipc,
	}
}

// refreshToken requests an access token with the OAuth2 client-credentials grant.
func (c *cdhClient) refreshToken(ctx context.Context) error {
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {c.cfg.ClientID},
		"client_secret": {c.cfg.ClientSecret},
		"scope":         {"machine2machine client_" + c.cfg.ClientID},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.AuthURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("create CDH access-token request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("request CDH access token: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return fmt.Errorf("CDH access-token request returned HTTP %d", response.StatusCode)
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&token); err != nil {
		return fmt.Errorf("decode CDH access-token response: %w", err)
	}
	if token.AccessToken == "" {
		return errors.New("CDH access-token response does not contain access_token")
	}
	c.token = token.AccessToken
	return nil
}

// fetchTable reads all pages of a table. No column list is sent, so every column is returned.
func (c *cdhClient) fetchTable(ctx context.Context, table string, filter map[string]any) ([]Row, error) {
	filterJSON, err := json.Marshal(filter)
	if err != nil {
		return nil, fmt.Errorf("encode filter for table %s: %w", table, err)
	}
	var rows []Row
	nextToken := ""
	for page := 1; ; page++ {
		params := url.Values{
			"table":      {table},
			"hub":        {c.cfg.Hub},
			"region":     {c.cfg.Region},
			"stage":      {c.cfg.Stage},
			"dataset_id": {c.args.DatasetID},
			"filter":     {string(filterJSON)},
			"limit":      {strconv.Itoa(c.cfg.PageSize)},
		}
		if nextToken != "" {
			params.Set("next_token", nextToken)
		}
		endpoint, err := url.Parse(c.cfg.EndpointURL)
		if err != nil {
			return nil, fmt.Errorf("parse endpoint_url: %w", err)
		}
		endpoint.RawQuery = params.Encode()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("create CDH request for table %s: %w", table, err)
		}
		request.Header.Set("Accept", "application/json")
		request.Header.Set("Authorization", "Bearer "+c.token)

		response, err := c.http.Do(request)
		if err != nil {
			return nil, &transientError{fmt.Errorf("request CDH table %s: %w", table, err)}
		}
		body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseSize))
		response.Body.Close()
		if err != nil {
			return nil, &transientError{fmt.Errorf("read CDH response for table %s: %w", table, err)}
		}
		if response.StatusCode < 200 || response.StatusCode > 299 {
			return nil, &httpStatusError{table: table, status: response.StatusCode}
		}

		var payload struct {
			Data      []Row  `json:"data"`
			NextToken string `json:"next_token"`
		}
		decoder := json.NewDecoder(strings.NewReader(string(body)))
		decoder.UseNumber()
		if err := decoder.Decode(&payload); err != nil {
			return nil, fmt.Errorf("decode CDH response for table %s: %w", table, err)
		}
		rows = append(rows, payload.Data...)
		nextToken = payload.NextToken
		if nextToken == "" || (c.args.MaxPages > 0 && page >= c.args.MaxPages) {
			return rows, nil
		}
	}
}

// safeFetch retries transient errors, refreshes the token once on HTTP 401/403 and skips a
// request that still fails. Any other HTTP 4xx and non-transient errors are returned.
func (c *cdhClient) safeFetch(ctx context.Context, table string, filter map[string]any) ([]Row, error) {
	refreshed := false
	reason := ""
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		rows, err := c.fetchTable(ctx, table, filter)
		if err == nil {
			return rows, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var statusErr *httpStatusError
		var transient *transientError
		switch {
		case errors.As(err, &statusErr):
			status := statusErr.status
			if (status == http.StatusUnauthorized || status == http.StatusForbidden) && !refreshed {
				log.Printf("%s %s: HTTP %d, refreshing token", table, filterText(filter), status)
				if err := c.refreshToken(ctx); err != nil {
					return nil, err
				}
				refreshed = true
				continue
			}
			if status < 500 && status != http.StatusTooManyRequests {
				if status == http.StatusUnauthorized || status == http.StatusForbidden {
					c.skip(table, filter, fmt.Sprintf("still HTTP %d after token refresh", status))
					return nil, nil
				}
				return nil, err
			}
			reason = fmt.Sprintf("HTTP %d", status)
		case errors.As(err, &transient):
			reason = transient.Error()
		default:
			return nil, err
		}
		if attempt == maxAttempts {
			break
		}
		wait := retryBackoff << (attempt - 1)
		log.Printf("%s %s: %s (attempt %d/%d), retrying in %s", table, filterText(filter), reason, attempt, maxAttempts, wait)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	c.skip(table, filter, fmt.Sprintf("giving up after %d attempts: %s", maxAttempts, reason))
	return nil, nil
}

func (c *cdhClient) skip(table string, filter map[string]any, reason string) {
	entry := fmt.Sprintf("%s %s: %s", table, filterText(filter), reason)
	c.skipped = append(c.skipped, entry)
	log.Printf("WARNING: skipped request %s", entry)
	c.ipc.SendAudit("WARNING: skipped request " + entry)
}

func filterText(filter map[string]any) string {
	data, _ := json.Marshal(filter)
	return string(data)
}

// --- Hierarchy and flat rows -------------------------------------------------

// collectCountry reads the hierarchy of one country and returns its flat rows.
func (c *cdhClient) collectCountry(ctx context.Context, country string) ([]Row, error) {
	levels := hierarchy(c.args.Tables)
	frames := make([][]Row, len(levels))

	locations, err := c.safeFetch(ctx, levels[0].table, map[string]any{"country_iso2": country})
	if err != nil {
		return nil, err
	}
	frames[0] = c.dedupe(levels[0], locations)
	for i := 1; i < len(levels); i++ {
		frames[i], err = c.fetchChildren(ctx, levels[i], keyValues(frames[i-1], levels[i].fk))
		if err != nil {
			return nil, err
		}
	}
	c.ipc.SendAudit(fmt.Sprintf("%s: %d locations, %d buildings, %d levels, %d rooms",
		country, len(frames[0]), len(frames[1]), len(frames[2]), len(frames[3])))

	for i := 1; i < len(levels); i++ {
		if orphans := countOrphans(frames[i], frames[i-1], levels[i].fk); orphans > 0 {
			c.ipc.SendAudit(fmt.Sprintf("WARNING: %s: %d rows without parent in %s", levels[i].table, orphans, levels[i-1].table))
		}
	}
	return buildFlat(levels, frames), nil
}

// fetchChildren reads the rows of a child level for the given parent key values.
func (c *cdhClient) fetchChildren(ctx context.Context, lvl level, parentKeys []any) ([]Row, error) {
	var rows []Row
	if c.args.BulkChildren {
		all, err := c.safeFetch(ctx, lvl.table, map[string]any{})
		if err != nil {
			return nil, err
		}
		wanted := make(map[string]bool, len(parentKeys))
		for _, key := range parentKeys {
			wanted[keyString(key)] = true
		}
		for _, row := range all {
			if wanted[keyString(row[lvl.fk])] {
				rows = append(rows, row)
			}
		}
	} else {
		for _, key := range parentKeys {
			children, err := c.safeFetch(ctx, lvl.table, map[string]any{lvl.fk: key})
			if err != nil {
				return nil, err
			}
			rows = append(rows, children...)
		}
	}
	return c.dedupe(lvl, rows), nil
}

// dedupe drops rows without primary key and duplicate primary keys (first one wins).
func (c *cdhClient) dedupe(lvl level, rows []Row) []Row {
	seen := make(map[string]bool, len(rows))
	result := make([]Row, 0, len(rows))
	missing, duplicates := 0, 0
	for _, row := range rows {
		key := keyString(row[lvl.pk])
		switch {
		case key == "":
			missing++
		case seen[key]:
			duplicates++
		default:
			seen[key] = true
			result = append(result, row)
		}
	}
	if missing > 0 {
		c.ipc.SendAudit(fmt.Sprintf("WARNING: %s: dropped %d rows without %s", lvl.table, missing, lvl.pk))
	}
	if duplicates > 0 {
		c.ipc.SendAudit(fmt.Sprintf("WARNING: %s: dropped %d duplicate %s values", lvl.table, duplicates, lvl.pk))
	}
	return result
}

// keyValues returns the distinct, non-empty values of a column in row order.
func keyValues(rows []Row, column string) []any {
	seen := make(map[string]bool, len(rows))
	var values []any
	for _, row := range rows {
		key := keyString(row[column])
		if key != "" && !seen[key] {
			seen[key] = true
			values = append(values, row[column])
		}
	}
	return values
}

func countOrphans(children, parents []Row, fk string) int {
	if len(children) == 0 || len(parents) == 0 {
		return 0
	}
	parentKeys := make(map[string]bool, len(parents))
	for _, row := range parents {
		parentKeys[keyString(row[fk])] = true
	}
	orphans := 0
	for _, row := range children {
		if !parentKeys[keyString(row[fk])] {
			orphans++
		}
	}
	return orphans
}

func keyString(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case json.Number:
		return v.String()
	default:
		return fmt.Sprint(v)
	}
}

// flatColumn maps a column of a level to its flat name; "" drops the column.
// The level's own primary and join keys stay unprefixed; meta columns are only kept for locations.
func flatColumn(lvl level, column string) string {
	if column == lvl.pk || (lvl.fk != "" && column == lvl.fk) {
		return column
	}
	if lvl.fk != "" && metaColumns[column] {
		return ""
	}
	return lvl.prefix + column
}

// buildFlat left-joins locations > buildings > levels > rooms into one row per entity.
func buildFlat(levels []level, frames [][]Row) []Row {
	var columns []string
	for i, lvl := range levels {
		seen := map[string]bool{}
		for _, row := range frames[i] {
			for column := range row {
				if name := flatColumn(lvl, column); name != "" && !seen[name] {
					seen[name] = true
					columns = append(columns, name)
				}
			}
		}
	}

	children := make([]map[string][]Row, len(levels))
	for i := 1; i < len(levels); i++ {
		children[i] = map[string][]Row{}
		for _, row := range frames[i] {
			key := keyString(row[levels[i].fk])
			children[i][key] = append(children[i][key], row)
		}
	}

	var flat []Row
	var walk func(depth int, path []Row)
	walk = func(depth int, path []Row) {
		if depth < len(levels) {
			parentKey := keyString(path[depth-1][levels[depth-1].pk])
			if rows := children[depth][parentKey]; len(rows) > 0 {
				for _, row := range rows {
					walk(depth+1, append(path, row))
				}
				return
			}
		}
		flat = append(flat, flatRow(levels, columns, path))
	}
	for _, location := range frames[0] {
		walk(1, []Row{location})
	}
	return flat
}

func flatRow(levels []level, columns []string, path []Row) Row {
	row := make(Row, len(columns)+1)
	for _, column := range columns {
		row[column] = nil
	}
	for i, source := range path {
		for column, value := range source {
			if name := flatColumn(levels[i], column); name != "" {
				row[name] = value
			}
		}
	}
	row["row_level"] = [...]string{"location", "building", "level", "room"}[len(path)-1]
	return row
}

// businessKey is the code of the deepest level of a flat row.
func businessKey(row Row) string {
	for _, column := range []string{"room_code", "level_code", "building_code", "location_code"} {
		if key := keyString(row[column]); key != "" {
			return key
		}
	}
	return ""
}

// fingerprint is the SHA-256 over all flat rows, independent of their order.
func fingerprint(rows []Row) (string, error) {
	lines := make([]string, len(rows))
	for i, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			return "", fmt.Errorf("serialize flat row: %w", err)
		}
		lines[i] = string(data)
	}
	sort.Strings(lines)
	hash := sha256.New()
	for _, line := range lines {
		hash.Write([]byte(line))
		hash.Write([]byte{'\n'})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// --- MitM database -------------------------------------------------------------

// ingestRows writes every flat row and the cursor in one transaction.
func ingestRows(ctx context.Context, pool *pgxpool.Pool, args CollectorArgs, rows []Row,
	dekGCM cipher.AEAD, dekID uuid.UUID, cursor string) error {
	namespace := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin ingestion transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for start := 0; start < len(rows); start += maxBatchSize {
		end := min(start+maxBatchSize, len(rows))
		batch := &pgx.Batch{}
		for _, row := range rows[start:end] {
			payload, err := json.Marshal(row)
			if err != nil {
				return fmt.Errorf("serialize flat row: %w", err)
			}
			nonce := make([]byte, dekGCM.NonceSize())
			if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
				return fmt.Errorf("generate nonce: %w", err)
			}
			correlationID := uuid.NewSHA1(namespace, []byte(businessKey(row)))
			batch.Queue(`
				INSERT INTO raw_ingestion (topic, source_system, correlation_id, payload, nonce, dek_id, status)
				VALUES ($1, $2, $3, $4, $5, $6, 'pending')
			`, args.Topic, args.SourceName, correlationID, dekGCM.Seal(nil, nonce, payload, nil), nonce, dekID)
		}
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return fmt.Errorf("insert raw ingestion records: %w", err)
		}
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO ingestion_cursors (source_name, last_cursor, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (source_name)
		DO UPDATE SET last_cursor = EXCLUDED.last_cursor, updated_at = NOW()
	`, args.SourceName, cursor); err != nil {
		return fmt.Errorf("update ingestion cursor: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit ingestion transaction: %w", err)
	}
	return nil
}

func loadTargetDBConfig() (TargetDBConfig, string, error) {
	var cfg TargetDBConfig
	source := "Environment Variables"
	sslMode := os.Getenv("MITM_DB_SSLMODE")
	if raw := os.Getenv("MITM_DB_CONFIG_JSON"); raw != "" {
		var full struct {
			DB struct {
				Host     string `json:"host"`
				Port     int    `json:"port"`
				User     string `json:"user"`
				Password string `json:"password"`
				Database string `json:"database"`
				SSLMode  bool   `json:"sslmode"`
			} `json:"db"`
		}
		if err := json.Unmarshal([]byte(raw), &full); err != nil {
			return cfg, source, fmt.Errorf("Failed to parse MitM database JSON config: %w", err)
		}
		cfg = TargetDBConfig{full.DB.Host, full.DB.Port, full.DB.User, full.DB.Password, full.DB.Database, ""}
		sslMode = strconv.FormatBool(full.DB.SSLMode)
		source = "JSON Config (MITM_DB_CONFIG_JSON)"
	} else {
		cfg.Host = os.Getenv("MITM_DB_HOST")
		cfg.Port, _ = strconv.Atoi(os.Getenv("MITM_DB_PORT"))
		cfg.User = os.Getenv("MITM_DB_USER")
		cfg.Password = os.Getenv("MITM_DB_PASSWORD")
		cfg.Database = os.Getenv("MITM_DB_NAME")
	}
	switch sslMode {
	case "", "false":
		cfg.SSLMode = "disable"
	case "true":
		cfg.SSLMode = "require"
	default:
		cfg.SSLMode = sslMode
	}
	if cfg.Host == "" {
		return cfg, source, errors.New("MitM database credentials not found in environment (MITM_DB_HOST or MITM_DB_CONFIG_JSON)")
	}
	return cfg, source, nil
}

func connectTargetDB(ctx context.Context, cfg TargetDBConfig) (*pgxpool.Pool, error) {
	dsn := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(cfg.User, cfg.Password),
		Host:     net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Path:     "/" + cfg.Database,
		RawQuery: "sslmode=" + url.QueryEscape(cfg.SSLMode),
	}
	poolCfg, err := pgxpool.ParseConfig(dsn.String())
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = 20
	poolCfg.MaxConnIdleTime = 5 * time.Minute
	poolCfg.MaxConnLifetime = time.Hour
	return pgxpool.NewWithConfig(ctx, poolCfg)
}

func validateKEK(masterKey string) ([]byte, error) {
	if masterKey == "" {
		return nil, errors.New("Missing MASTER_KEY environment variable")
	}
	kek := []byte(masterKey)
	if decoded, err := base64.StdEncoding.DecodeString(masterKey); err == nil && len(decoded) == 32 {
		kek = decoded
	}
	if len(kek) != 32 {
		return nil, fmt.Errorf("MASTER_KEY must be exactly 32 bytes, got %d", len(kek))
	}
	return kek, nil
}

func loadSourceSecrets(ctx context.Context, pool *pgxpool.Pool, sourceName string) ([]byte, []byte, uuid.UUID, []byte, error) {
	var configPayload, nonce, wrappedDEK []byte
	var dekID uuid.UUID
	err := pool.QueryRow(ctx, `
		SELECT config_payload, nonce, dek_id
		FROM source_credentials
		WHERE source_name = $1 AND is_active = true
		LIMIT 1
	`, sourceName).Scan(&configPayload, &nonce, &dekID)
	if err != nil {
		return nil, nil, uuid.Nil, nil, fmt.Errorf("Failed to load source credentials for '%s': %w", sourceName, err)
	}
	err = pool.QueryRow(ctx, `
		SELECT wrapped_key
		FROM storage_keys
		WHERE id = $1 AND is_active = true
		LIMIT 1
	`, dekID).Scan(&wrappedDEK)
	if err != nil {
		return nil, nil, uuid.Nil, nil, fmt.Errorf("Failed to load wrapped DEK (ID: %s): %w", dekID, err)
	}
	return configPayload, nonce, dekID, wrappedDEK, nil
}

func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// unwrapDEK decrypts a wrapped DEK stored as nonce (12 bytes) || AES-GCM ciphertext.
func unwrapDEK(kek, wrappedDEK []byte) ([]byte, error) {
	gcm, err := newGCM(kek)
	if err != nil {
		return nil, err
	}
	if len(wrappedDEK) < gcm.NonceSize() {
		return nil, errors.New("wrapped DEK is too short")
	}
	return gcm.Open(nil, wrappedDEK[:gcm.NonceSize()], wrappedDEK[gcm.NonceSize():], nil)
}

func decrypt(gcm cipher.AEAD, nonce, encrypted []byte) ([]byte, error) {
	if len(nonce) != gcm.NonceSize() {
		return nil, fmt.Errorf("invalid nonce length %d", len(nonce))
	}
	return gcm.Open(nil, nonce, encrypted, nil)
}

func loadCursor(ctx context.Context, pool *pgxpool.Pool, sourceName string) (string, error) {
	var cursor string
	err := pool.QueryRow(ctx, "SELECT last_cursor FROM ingestion_cursors WHERE source_name = $1", sourceName).Scan(&cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("Failed to load ingestion cursor: %w", err)
	}
	return cursor, nil
}

// --- Scheduler IPC -------------------------------------------------------------

func newIPCClient() *IPCClient {
	socketPath := os.Getenv("SCHEDULER_SOCKET_PATH")
	runID, err := strconv.Atoi(os.Getenv("RUN_ID"))
	if err != nil || socketPath == "" {
		return nil
	}
	return &IPCClient{SocketPath: socketPath, RunID: runID, Component: componentName}
}

func (c *IPCClient) SendEvent(status, message string, progress int) {
	if c == nil || c.SocketPath == "" {
		return
	}
	c.send(StatusEvent{RunID: c.RunID, Type: "status", Status: status, Message: c.prefix(message), Progress: progress})
}

func (c *IPCClient) SendAudit(message string) {
	if c == nil || c.SocketPath == "" {
		return
	}
	c.send(StatusEvent{RunID: c.RunID, Type: "audit", Component: c.Component, Message: c.prefix(message)})
}

func (c *IPCClient) prefix(message string) string {
	if c.Topic != "" && c.SourceName != "" {
		return fmt.Sprintf("%s: %s: %s", c.Topic, c.SourceName, message)
	}
	return message
}

func (c *IPCClient) send(event StatusEvent) {
	conn, err := net.Dial("unix", c.SocketPath)
	if err != nil {
		log.Printf("[IPC ERROR] Failed to connect to scheduler socket: %v", err)
		return
	}
	defer conn.Close()
	data, _ := json.Marshal(event)
	_, _ = conn.Write(append(data, '\n'))
}

func fetchCredentialsFromScheduler() (string, string, error) {
	runIDStr := os.Getenv("RUN_ID")
	socketPath := os.Getenv("SCHEDULER_SOCKET_PATH")
	if runIDStr == "" || socketPath == "" {
		return "", "", errors.New("not running under scheduler")
	}
	runID, err := strconv.Atoi(runIDStr)
	if err != nil {
		return "", "", err
	}
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return "", "", err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	data, _ := json.Marshal(map[string]any{"type": "get_credentials", "run_id": runID})
	if _, err := conn.Write(append(data, '\n')); err != nil {
		return "", "", err
	}
	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		var resp struct {
			MasterKey    string `json:"master_key"`
			DBConfigJSON string `json:"db_config_json"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &resp); err == nil {
			return resp.DBConfigJSON, resp.MasterKey, nil
		}
	}
	return "", "", errors.New("no response or invalid JSON from scheduler")
}
