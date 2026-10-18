package cmd

import (
	"context"
	"encoding/hex"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/brocaar/chirpstack-simulator/internal/as"
	"github.com/brocaar/chirpstack-simulator/internal/config"
	loris_payload "github.com/brocaar/chirpstack-simulator/internal/loris-payload"
	"github.com/brocaar/chirpstack-simulator/internal/ns"
	"github.com/brocaar/chirpstack-simulator/internal/simulator"
)

var runCmd = &cobra.Command{
	Use:   "run [file.json]",
	Short: "Run simulator",
	Long: `Run simulator optionally providing paylaod to parse from json file

File payload is being parsed according to the **loris** protocol.
If no file is provided, config payload will be used and no further protocol validation will be performed`,
	Args: cobra.MaximumNArgs(1),
	RunE: run,
}

func run(cnd *cobra.Command, args []string) error {

	tasks := []func(context.Context, *sync.WaitGroup) error{
		setLogLevel,
		printStartMessage,
		parseFilePayload(args),
		setupASAPIClient,
		setupASIntegration,
		setupNSIntegration,
		setupPrometheus,
		startSimulator,
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	for _, t := range tasks {
		if err := t(ctx, &wg); err != nil {
			log.Fatal(err)
		}
	}

	exitChan := make(chan struct{})
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	go func() {
		cancel()
		wg.Wait()
		exitChan <- struct{}{}
	}()
	cancel()
	select {
	case <-exitChan:
	case s := <-sigChan:
		log.WithField("signal", s).Info("signal received, terminating")
	}

	return nil
}

func parseFilePayload(args []string) func(context.Context, *sync.WaitGroup) error {
	return func(ctx context.Context, wg *sync.WaitGroup) error {

		if len(args) == 0 {
			return nil
		}

		file := args[0]
		in, err := os.ReadFile(file)
		if err != nil {
			return errors.Wrap(err, "unable to read file "+file)
		}

		var message loris_payload.Message
		if err := protojson.Unmarshal(in, &message); err != nil {
			return errors.Wrap(err, "payload file is not using loris protoco")
		}

		out, err := proto.Marshal(&message)
		if err != nil {
			panic(err)
		}

		log.WithFields(
			log.Fields{"File": file, "hex": hex.EncodeToString(out)},
		).Info("successfuly encoded")

		for _, c := range config.C.Simulator {
			c.Device.Payload = out
		}

		return nil
	}
}

func setLogLevel(ctx context.Context, wg *sync.WaitGroup) error {
	log.SetLevel(log.Level(uint8(config.C.General.LogLevel)))
	return nil
}

func printStartMessage(ctx context.Context, wg *sync.WaitGroup) error {
	log.WithFields(log.Fields{
		"version": version,
		"docs":    "https://www.chirpstack.io/",
	}).Info("starting ChirpStack Simulator")
	return nil
}

func setupASAPIClient(ctx context.Context, wg *sync.WaitGroup) error {
	return as.Setup(config.C)
}

func setupASIntegration(ctx context.Context, wg *sync.WaitGroup) error {
	return nil
}

func setupNSIntegration(ctx context.Context, wg *sync.WaitGroup) error {
	return ns.Setup(config.C)
}

func setupPrometheus(ctx context.Context, wg *sync.WaitGroup) error {
	log.WithFields(log.Fields{
		"bind": config.C.Prometheus.Bind,
	}).Info("starting Prometheus endpoint server")

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := http.Server{
		Handler: mux,
		Addr:    config.C.Prometheus.Bind,
	}

	go func() {
		err := server.ListenAndServe()
		log.WithError(err).Error("prometheus endpoint server error")
	}()

	return nil
}

func startSimulator(ctx context.Context, wg *sync.WaitGroup) error {
	if err := simulator.Start(ctx, wg, config.C); err != nil {
		return errors.Wrap(err, "start simulator error")
	}
	return nil
}
