package main

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"binance_trader/internal/credentials"
	"binance_trader/internal/live/autopipeline"
	"binance_trader/internal/uiapi"
)

//go:embed web/*
var webFiles embed.FS

func main() {
	listen := flag.String("listen", "127.0.0.1:8080", "localhost listen address")
	audit := flag.Bool("audit", false, "run bounded UI checkpoint audit without serving")
	auditFinalize := flag.Bool("audit-finalize", false, "mark build verification complete after commands pass")
	opsReport := flag.Bool("ops-report", false, "write Phase UI-2/OPS-1 checkpoint report")
	opsRuntimeSmoke := flag.Bool("ops-runtime-smoke", false, "verify bounded read-only live runtime")
	opsSmoke := flag.Bool("ops-smoke-pass", false, "bounded HTTP/WebSocket smoke passed")
	opsBuild := flag.Bool("ops-build-pass", false, "full Go test, vet, and build passed")
	flag.Parse()
	if !strings.HasPrefix(*listen, "127.0.0.1:") && !strings.HasPrefix(*listen, "localhost:") {
		fmt.Fprintln(os.Stderr, "traderui only permits a localhost bind")
		os.Exit(1)
	}
	static, err := fs.Sub(webFiles, "web")
	if err != nil {
		fatal(err)
	}
	provider := credentials.NewFileProvider()
	if *auditFinalize {
		if err = finalizeAuditBuild(); err != nil {
			fatal(err)
		}
		return
	}
	if *opsReport {
		if err = runOpsReport(static, *opsSmoke, *opsBuild); err != nil {
			fatal(err)
		}
		return
	}
	if *opsRuntimeSmoke {
		if err = runRuntimeSmoke(); err != nil {
			fatal(err)
		}
		return
	}
	if *audit {
		if err = runAudit(provider, static); err != nil {
			fatal(err)
		}
		return
	}
	var testnet uiapi.TestnetBackend
	frozenPipeline, err := autopipeline.LoadFrozen(".")
	if err != nil {
		fatal(fmt.Errorf("frozen auto pipeline unavailable: %w", err))
	}
	if store, loadErr := provider.Load(); loadErr == nil && store.Testnet.Available() {
		if backend, backendErr := uiapi.NewBinanceTestnetBackend(store.Testnet.APIKey, store.Testnet.APISecret); backendErr == nil {
			syncCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = backend.SyncTime(syncCtx)
			cancel()
			testnet = backend
		}
	}
	app, err := uiapi.NewServer(provider, static, uiapi.Options{TestnetBackend: testnet, AutoPipeline: frozenPipeline, LiveFeatureRuntime: true, LiveBootstrap: true, BootstrapStabilization: 5 * time.Minute, PaperOrders: map[uiapi.TradingEnvironment]bool{
		uiapi.TradingEnvironmentTestnet: false,
		uiapi.TradingEnvironmentMainnet: false,
	}})
	if err != nil {
		fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	app.RunPublicMarketFeed(ctx)
	go app.RunAutoReconciliation(ctx)
	server := &http.Server{Addr: *listen, Handler: app.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	fmt.Printf("BTCUSDT TRADING CONSOLE V1 http://%s\n", *listen)
	fmt.Println("EXECUTION=TESTNET MAINNET_ORDERS=DISABLED")
	if err = server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fatal(err)
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
