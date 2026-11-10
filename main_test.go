// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

const validArgs = `{"source_name":"CDH_RE","dataset_id":"ds","countries":["at"],
	"tables":{"locations":"v2_locations","buildings":"v2_buildings","levels":"v2_levels","rooms":"v2_rooms"}}`

func TestValidateKEK(t *testing.T) {
	key := bytes.Repeat([]byte{1}, 32)
	got, err := validateKEK(base64.StdEncoding.EncodeToString(key))
	if err != nil || !bytes.Equal(got, key) {
		t.Fatalf("validateKEK(base64) = %v, %v", got, err)
	}
	if _, err := validateKEK("too short"); err == nil {
		t.Fatal("validateKEK accepted an invalid key")
	}
}

func TestUnwrapDEK(t *testing.T) {
	kek := bytes.Repeat([]byte{1}, 32)
	dek := bytes.Repeat([]byte{2}, 32)
	gcm, _ := newGCM(kek)
	nonce := make([]byte, gcm.NonceSize())
	rand.Read(nonce)
	got, err := unwrapDEK(kek, append(nonce, gcm.Seal(nil, nonce, dek, nil)...))
	if err != nil || !bytes.Equal(got, dek) {
		t.Fatalf("unwrapDEK = %v, %v", got, err)
	}
	if _, err := unwrapDEK(kek, []byte{1, 2}); err == nil {
		t.Fatal("unwrapDEK accepted a too short key")
	}
}

func TestParseArgs(t *testing.T) {
	args, err := parseArgs(validArgs)
	if err != nil {
		t.Fatal(err)
	}
	if args.Topic != defaultTopic || args.Countries[0] != "AT" {
		t.Fatalf("unexpected defaults: %+v", args)
	}
	for name, raw := range map[string]string{
		"empty":           "",
		"not json":        "not-json",
		"no dataset":      `{"source_name":"x","countries":["AT"],"tables":{"locations":"a","buildings":"b","levels":"c","rooms":"d"}}`,
		"missing table":   `{"source_name":"x","dataset_id":"d","countries":["AT"],"tables":{"locations":"a"}}`,
		"invalid table":   `{"source_name":"x","dataset_id":"d","countries":["AT"],"tables":{"locations":"a;b","buildings":"b","levels":"c","rooms":"d"}}`,
		"invalid country": `{"source_name":"x","dataset_id":"d","countries":["AUT"],"tables":{"locations":"a","buildings":"b","levels":"c","rooms":"d"}}`,
	} {
		if _, err := parseArgs(raw); err == nil {
			t.Errorf("%s: parseArgs accepted invalid arguments", name)
		}
	}
}

func TestParseSourceConfig(t *testing.T) {
	cfg, err := parseSourceConfig([]byte(`{"client_id":"id","client_secret":"s","auth_url":"a","endpoint_url":"e","hub":"h","region":"r","stage":"p"}`))
	if err != nil || cfg.PageSize != defaultPageSize {
		t.Fatalf("parseSourceConfig = %+v, %v", cfg, err)
	}
	if _, err := parseSourceConfig([]byte(`{"client_id":"id"}`)); err == nil {
		t.Fatal("parseSourceConfig accepted an incomplete configuration")
	}
	if _, err := parseSourceConfig([]byte(`{"client_id":"id","client_secret":"s","auth_url":"a","endpoint_url":"e","hub":"h","region":"r","stage":"p","page_size":1001}`)); err == nil {
		t.Fatal("parseSourceConfig accepted page_size > 1000")
	}
}

// fakeCDH serves the token endpoint and a data endpoint driven by the handler.
func fakeCDH(t *testing.T, data http.HandlerFunc) (*cdhClient, *atomic.Int32) {
	t.Helper()
	retryBackoff = 0
	tokens := &atomic.Int32{}
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if r.FormValue("grant_type") != "client_credentials" || r.FormValue("scope") != "machine2machine client_id" {
			t.Errorf("unexpected token request: %v", r.Form)
		}
		tokens.Add(1)
		json.NewEncoder(w).Encode(map[string]string{"access_token": "tok"})
	})
	mux.HandleFunc("/data", data)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	args, _ := parseArgs(validArgs)
	client := newCDHClient(SourceConfig{
		ClientID: "id", ClientSecret: "secret", AuthURL: server.URL + "/token", EndpointURL: server.URL + "/data",
		Hub: "global", Region: "eu-west-1", Stage: "prod", PageSize: 2,
	}, args, nil)
	if err := client.refreshToken(context.Background()); err != nil {
		t.Fatal(err)
	}
	return client, tokens
}

func TestFetchTablePaginates(t *testing.T) {
	client, _ := fakeCDH(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("table") != "v2_locations" || q.Get("dataset_id") != "ds" || q.Get("limit") != "2" ||
			q.Get("filter") != `{"country_iso2":"AT"}` || q.Has("columns") || r.Header.Get("Authorization") != "Bearer tok" {
			t.Errorf("unexpected request: %s", r.URL.RawQuery)
		}
		if q.Get("next_token") == "" {
			w.Write([]byte(`{"data":[{"location_code":"A"},{"location_code":"B"}],"next_token":"p2"}`))
		} else {
			w.Write([]byte(`{"data":[{"location_code":"C","latitude":48.1}]}`))
		}
	})
	rows, err := client.fetchTable(context.Background(), "v2_locations", map[string]any{"country_iso2": "AT"})
	if err != nil || len(rows) != 3 {
		t.Fatalf("fetchTable = %d rows, %v", len(rows), err)
	}
	if rows[2]["latitude"] != json.Number("48.1") {
		t.Fatalf("numbers must be kept exactly, got %#v", rows[2]["latitude"])
	}
}

func TestSafeFetchRefreshesTokenOnce(t *testing.T) {
	calls := 0
	client, tokens := fakeCDH(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Write([]byte(`{"data":[{"location_code":"A"}]}`))
	})
	rows, err := client.safeFetch(context.Background(), "v2_locations", map[string]any{})
	if err != nil || len(rows) != 1 || tokens.Load() != 2 || len(client.skipped) != 0 {
		t.Fatalf("rows=%d err=%v tokens=%d skipped=%v", len(rows), err, tokens.Load(), client.skipped)
	}
}

func TestSafeFetchSkipsAfterRetries(t *testing.T) {
	calls := 0
	client, _ := fakeCDH(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	rows, err := client.safeFetch(context.Background(), "v2_rooms", map[string]any{"level_code": "L1"})
	if err != nil || rows != nil || calls != maxAttempts || len(client.skipped) != 1 {
		t.Fatalf("rows=%v err=%v calls=%d skipped=%v", rows, err, calls, client.skipped)
	}
}

func TestSafeFetchFailsOnClientError(t *testing.T) {
	client, _ := fakeCDH(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	})
	if _, err := client.safeFetch(context.Background(), "v2_rooms", map[string]any{}); err == nil {
		t.Fatal("safeFetch must fail on HTTP 400")
	}
}

func TestCollectCountryBulkChildren(t *testing.T) {
	client, _ := fakeCDH(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("table") {
		case "v2_locations":
			w.Write([]byte(`{"data":[{"location_code":"L1"}]}`))
		case "v2_buildings":
			w.Write([]byte(`{"data":[{"building_code":"B1","location_code":"L1"},{"building_code":"BX","location_code":"OTHER"}]}`))
		default:
			w.Write([]byte(`{"data":[]}`))
		}
	})
	client.args.BulkChildren = true
	rows, err := client.collectCountry(context.Background(), "AT")
	if err != nil || len(rows) != 1 || rows[0]["building_code"] != "B1" || rows[0]["row_level"] != "building" {
		t.Fatalf("collectCountry = %v, %v", rows, err)
	}
}

func TestDedupe(t *testing.T) {
	client := &cdhClient{}
	lvl := hierarchy(TableNames{"l", "b", "v", "r"})[0]
	rows := client.dedupe(lvl, []Row{
		{"location_code": "A", "name": "first"}, {"location_code": "A", "name": "dup"},
		{"name": "no key"}, {"location_code": nil}, {"location_code": "B"},
	})
	if len(rows) != 2 || rows[0]["name"] != "first" {
		t.Fatalf("dedupe = %v", rows)
	}
}

func TestBuildFlat(t *testing.T) {
	levels := hierarchy(TableNames{"l", "b", "v", "r"})
	frames := [][]Row{
		{{"location_code": "L1", "name": "Plant", "data_source": "SAP", "ingest_date": "2026-11-10"}, {"location_code": "L2", "name": "Empty"}},
		{{"building_code": "B1", "location_code": "L1", "name_en": "Hall", "data_source": "SAP"}},
		{{"level_code": "V1", "building_code": "B1", "level_short_code": "01"}, {"level_code": "V2", "building_code": "B1"}},
		{{"room_code": "R1", "level_code": "V1", "level_short_code": "01"}, {"room_code": "R2", "level_code": "V1"}},
	}
	flat := buildFlat(levels, frames)
	if len(flat) != 4 {
		t.Fatalf("expected 4 flat rows (2 rooms, 1 level, 1 location), got %d: %v", len(flat), flat)
	}
	room := flat[0]
	if room["row_level"] != "room" || businessKey(room) != "R1" || room["location_code"] != "L1" ||
		room["building_code"] != "B1" || room["loc_name"] != "Plant" || room["bld_name_en"] != "Hall" ||
		room["lvl_level_short_code"] != "01" || room["room_level_short_code"] != "01" || room["loc_data_source"] != "SAP" {
		t.Fatalf("unexpected room row: %v", room)
	}
	if _, ok := room["bld_data_source"]; ok {
		t.Fatal("meta columns must only be kept for locations")
	}
	if flat[2]["row_level"] != "level" || businessKey(flat[2]) != "V2" || flat[2]["room_code"] != nil {
		t.Fatalf("unexpected level row: %v", flat[2])
	}
	location := flat[3]
	if location["row_level"] != "location" || businessKey(location) != "L2" {
		t.Fatalf("unexpected location row: %v", location)
	}
	if _, ok := location["room_room_code"]; ok {
		t.Fatal("join keys must not be prefixed")
	}
	if _, ok := location["bld_name_en"]; !ok {
		t.Fatal("every flat row must contain every column")
	}
}

func TestFingerprintIsOrderIndependent(t *testing.T) {
	a, _ := fingerprint([]Row{{"location_code": "A"}, {"location_code": "B"}})
	b, _ := fingerprint([]Row{{"location_code": "B"}, {"location_code": "A"}})
	c, _ := fingerprint([]Row{{"location_code": "A"}})
	if a != b || a == c {
		t.Fatalf("fingerprint: a=%s b=%s c=%s", a, b, c)
	}
}
