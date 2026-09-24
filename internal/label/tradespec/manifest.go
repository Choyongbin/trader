package tradespeclabel

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type ReproductionStatus string

const (
	ReproductionNotChecked ReproductionStatus = "NOT_CHECKED"
	ReproductionPassed     ReproductionStatus = "CHECKED_PASSED"
	ReproductionFailed     ReproductionStatus = "CHECKED_FAILED"
)

type PartitionStats struct {
	RawDecisions int64      `json:"raw_decisions"`
	Purged       int64      `json:"purged"`
	Included     int64      `json:"included"`
	Valid        int64      `json:"valid"`
	Invalid      int64      `json:"invalid"`
	Positive     int64      `json:"positive"`
	Negative     int64      `json:"negative"`
	MeanNet      float64    `json:"mean_net_return_ex_funding"`
	MedianNet    float64    `json:"median_net_return_ex_funding"`
	Statistics   Statistics `json:"statistics"`
}
type Manifest struct {
	CandidateLabelVersion int                       `json:"candidate_label_version"`
	CandidateID           string                    `json:"candidate_id"`
	Side                  string                    `json:"side"`
	TPBps                 int                       `json:"tp_bps"`
	SLBps                 int                       `json:"sl_bps"`
	HorizonSeconds        int                       `json:"horizon_seconds"`
	SourceFeatureVersion  int                       `json:"source_feature_version"`
	SourceBarrierVersion  int                       `json:"source_barrier_version"`
	CostProfile           string                    `json:"cost_profile"`
	FundingIncluded       bool                      `json:"funding_included"`
	TriggerPriceSource    string                    `json:"trigger_price_source"`
	MaxLabelDependencyMs  int64                     `json:"max_label_dependency_ms"`
	Phase9AReportPath     string                    `json:"source_phase9a_report_path"`
	Phase9AReportSHA256   string                    `json:"source_phase9a_report_sha256"`
	FeatureRegistryHash   string                    `json:"feature_registry_hash"`
	ReproductionStatus    ReproductionStatus        `json:"reproduction_status"`
	Partitions            map[string]PartitionStats `json:"partitions"`
	Complete              bool                      `json:"complete"`
}

func ReadManifest(p string) (Manifest, error) {
	var m Manifest
	b, e := os.ReadFile(p)
	if e == nil {
		e = json.Unmarshal(b, &m)
	}
	if e == nil && m.ReproductionStatus == "" {
		m.ReproductionStatus = ReproductionNotChecked
	}
	return m, e
}
func WriteManifest(p string, m Manifest) error {
	if m.ReproductionStatus == "" {
		m.ReproductionStatus = ReproductionNotChecked
	}
	if m.Complete && m.ReproductionStatus != ReproductionPassed {
		return errors.New("complete candidate label manifest requires CHECKED_PASSED reproduction")
	}
	b, e := json.MarshalIndent(m, "", "  ")
	if e != nil {
		return e
	}
	b = append(b, '\n')
	if e = os.MkdirAll(filepath.Dir(p), 0755); e != nil {
		return e
	}
	t := p + ".tmp"
	if e = os.WriteFile(t, b, 0644); e != nil {
		return e
	}
	if e = os.Remove(p); e != nil && !errors.Is(e, os.ErrNotExist) {
		return e
	}
	return os.Rename(t, p)
}
