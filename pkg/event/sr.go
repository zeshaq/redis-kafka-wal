package event

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// SRClient is a minimal Confluent Schema Registry HTTP client. It does not
// support auth, soft deletes, or compatibility checks; it covers only the
// flows the lab uses: fetch by id, register a subject.
type SRClient struct {
	BaseURL string
	HTTP    *http.Client
}

func NewSRClient(baseURL string) *SRClient {
	return &SRClient{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 10 * time.Second},
	}
}

type registeredSchema struct {
	ID     uint32 `json:"id"`
	Schema string `json:"schema"`
}

// SchemaByID fetches a previously-registered schema. Used at startup so the
// codec knows which id to embed in produced messages.
func (s *SRClient) SchemaByID(id uint32) (string, error) {
	url := fmt.Sprintf("%s/schemas/ids/%d", s.BaseURL, id)
	resp, err := s.HTTP.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("SR get id=%d: %s: %s", id, resp.Status, body)
	}
	var rs registeredSchema
	if err := json.NewDecoder(resp.Body).Decode(&rs); err != nil {
		return "", err
	}
	return rs.Schema, nil
}

// LatestForSubject returns (id, schemaJSON) for the latest version of a
// subject. The lab only ever registers one schema per subject so this is
// the canonical lookup the producer/consumer use at boot.
func (s *SRClient) LatestForSubject(subject string) (uint32, string, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions/latest", s.BaseURL, subject)
	resp, err := s.HTTP.Get(url)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return 0, "", fmt.Errorf("SR latest %s: %s: %s", subject, resp.Status, body)
	}
	var v struct {
		ID     uint32 `json:"id"`
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return 0, "", err
	}
	return v.ID, v.Schema, nil
}

// Register POSTs a schema to the subject and returns its assigned id.
func (s *SRClient) Register(subject, schemaJSON string) (uint32, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", s.BaseURL, subject)
	body, _ := json.Marshal(map[string]string{
		"schemaType": "AVRO",
		"schema":     schemaJSON,
	})
	req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := s.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("SR register %s: %s: %s", subject, resp.Status, b)
	}
	var v struct {
		ID uint32 `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return 0, err
	}
	return v.ID, nil
}

// WaitReady blocks until the SR is reachable, with bounded backoff.
func (s *SRClient) WaitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		resp, err := s.HTTP.Get(s.BaseURL + "/subjects")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
			last = fmt.Errorf("status %s", resp.Status)
		} else {
			last = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("schema registry not ready: %w", last)
}
