package uiapi

import (
	"context"
	"net/http"
)

type SpotAssetBalance struct {
	Asset     string   `json:"asset"`
	Free      float64  `json:"free"`
	Locked    float64  `json:"locked"`
	Total     float64  `json:"total"`
	USDTValue *float64 `json:"usdt_value"`
}

type SpotAssetProvider interface {
	Assets(context.Context, TradingEnvironment) ([]SpotAssetBalance, error)
}

type FuturesWalletAsset struct {
	Asset   string  `json:"asset"`
	Balance Balance `json:"balance"`
}

type Portfolio struct {
	Environment      TradingEnvironment `json:"environment"`
	Status           string             `json:"status"`
	PortfolioSummary struct {
		Status    string   `json:"status"`
		TotalUSDT *float64 `json:"total_usdt"`
	} `json:"portfolio_summary"`
	Spot struct {
		Status string             `json:"status"`
		Assets []SpotAssetBalance `json:"assets"`
	} `json:"spot"`
	Futures struct {
		Status       string               `json:"status"`
		WalletAssets []FuturesWalletAsset `json:"wallet_assets"`
		Positions    []Position           `json:"positions"`
	} `json:"futures"`
}

func (s *Server) getPortfolio(w http.ResponseWriter, r *http.Request) {
	environment, ok := environmentFromRequest(w, r)
	if !ok {
		return
	}
	if environment == TradingEnvironmentTestnet && s.testnet != nil {
		s.refreshTestnet(r.Context())
	}
	s.mu.RLock()
	state := s.states[environment]
	balance := state.Balance
	positions := append([]Position(nil), state.Positions...)
	s.mu.RUnlock()
	result := Portfolio{Environment: environment, Status: "PARTIAL"}
	result.PortfolioSummary.Status = "TOTAL_VALUE_UNAVAILABLE"
	result.Spot.Status = "UNAVAILABLE"
	result.Futures.Status = "NOT_CONNECTED"
	result.Spot.Assets = []SpotAssetBalance{}
	result.Futures.WalletAssets = []FuturesWalletAsset{}
	result.Futures.Positions = positions
	if environment == TradingEnvironmentTestnet && balance.Connected {
		result.Futures.Status = "READY"
		result.Futures.WalletAssets = append(result.Futures.WalletAssets, FuturesWalletAsset{Asset: "USDT", Balance: balance})
	}
	if environment == TradingEnvironmentMainnet {
		result.Status = "NOT_CONNECTED"
		result.Spot.Status = "NOT_CONNECTED"
	} else if s.spotAssets != nil {
		assets, err := s.spotAssets.Assets(r.Context(), environment)
		if err == nil {
			result.Spot.Status = "READY"
			result.Spot.Assets = assets
		}
	}
	if result.Spot.Status == "READY" && result.Futures.Status == "READY" && balance.MarginUSDT != nil {
		allValued := true
		total := *balance.MarginUSDT
		for _, asset := range result.Spot.Assets {
			if asset.USDTValue == nil {
				allValued = false
				break
			}
			total += *asset.USDTValue
		}
		if allValued {
			result.PortfolioSummary.Status = "READY"
			result.PortfolioSummary.TotalUSDT = &total
			result.Status = "READY"
		}
	}
	writeJSON(w, http.StatusOK, result)
}
