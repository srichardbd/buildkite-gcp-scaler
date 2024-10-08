package scaler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"buildkite-gcp-scaler/pkg/bk"
	"buildkite-gcp-scaler/pkg/buildkite"
	"buildkite-gcp-scaler/pkg/gce"

	hclog "github.com/hashicorp/go-hclog"
)

type Config struct {
	GCPProject            string
	GCPZone               string
	InstanceGroupName     string
	InstanceGroupTemplate string
	BuildkiteQueue        string
	BuildkiteToken        string
	BuildkiteApiToken     string
	BuildkiteOrg          string

	IdleTimeout  *time.Duration
	PollInterval *time.Duration
}

type Scaler interface {
	Run(context.Context) error
}

func NewAutoscaler(cfg *Config, logger hclog.Logger) Scaler {
	client, err := gce.NewClient(logger)
	if err != nil {
		// TODO return erros rather than panicing
		panic(err)
	}

	return &scaler{
		cfg:       cfg,
		logger:    logger.Named("scaler").With("queue", cfg.BuildkiteQueue),
		buildkite: buildkite.NewClient(cfg.BuildkiteToken, logger),
		bk:        bk.NewClient(cfg.BuildkiteApiToken, cfg.BuildkiteOrg, cfg.IdleTimeout, cfg.BuildkiteQueue, logger),
		gce:       client,
	}
}

type scaler struct {
	cfg *Config

	gce interface {
		LiveInstanceCount(ctx context.Context, projectID, zone, instanceGroupName string) (int64, error)
		LaunchInstanceForGroup(ctx context.Context, projectID, zone, groupName, templateName string) error
		DestroyInstance(ctx context.Context, projectID, zone, instanceName string) error
	}

	buildkite interface {
		GetAgentMetrics(context.Context, string) (*buildkite.AgentMetrics, error)
	}

	bk interface {
		GetIdleAgents(context.Context) ([]string, error)
		StopAgents(context.Context, []string) error
	}

	logger hclog.Logger
}

func (s *scaler) Run(ctx context.Context) error {
	ticker := time.NewTimer(0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:

			if err := s.run(ctx); err != nil {
				s.logger.Error("Autoscaling failed", "error", err)
			}

			if s.cfg.PollInterval != nil {
				ticker.Reset(*s.cfg.PollInterval)
			} else {
				return nil
			}
		}
	}
}

func (s *scaler) run(ctx context.Context) error {
	metrics, err := s.buildkite.GetAgentMetrics(ctx, s.cfg.BuildkiteQueue)
	if err != nil {
		return err
	}
	totalInstanceRequirement := metrics.ScheduledJobs + metrics.RunningJobs

	liveInstanceCount, err := s.gce.LiveInstanceCount(ctx, s.cfg.GCPProject, s.cfg.GCPZone, s.cfg.InstanceGroupName)
	if err != nil {
		return err
	}
	s.logger.Debug(fmt.Sprintf("liveInstanceCount: %d", liveInstanceCount))

	// If the number of live instances meets or exceeds the requirement
	if liveInstanceCount >= totalInstanceRequirement {
		// get any idleAgents
		idleAgents, err := s.bk.GetIdleAgents(ctx)
		if err != nil {
			return err
		}
		// stop the idleAgents
		err = s.bk.StopAgents(ctx, idleAgents)
		if err != nil {
			return err
		}
		// strip off the last hyphen in buildkite agents
		instanceNames := stripAllAfterLastHyphen(idleAgents)
		// delete all the now stopped idleInstances
		return s.deleteInstances(ctx, instanceNames)
	}

	required := totalInstanceRequirement - liveInstanceCount
	s.logger.Debug(fmt.Sprintf("total required instances %d", required))

	for i := int64(0); i < required; i++ {
		err := s.gce.LaunchInstanceForGroup(ctx, s.cfg.GCPProject, s.cfg.GCPZone, s.cfg.InstanceGroupName, s.cfg.InstanceGroupTemplate)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *scaler) deleteInstances(ctx context.Context, instances []string) error {
	var wg sync.WaitGroup
	var mu sync.Mutex
	errors := []error{}

	// Iterate over the instances
	for _, instance := range instances {
		s.logger.Debug(fmt.Sprintf("Preparing to destroy instance %s", instance))
		wg.Add(1)

		// Launch a goroutine to delete the instance
		go func(name string) {
			defer wg.Done() // Signal completion when the goroutine finishes

			err := s.gce.DestroyInstance(ctx, s.cfg.GCPProject, s.cfg.GCPZone, name)
			if err != nil {
				mu.Lock() // Lock to avoid race condition when appending to errors
				errors = append(errors, fmt.Errorf("Error destroying instance %s: %v", name, err))
				mu.Unlock()
			} else {
				s.logger.Info(fmt.Sprintf("Successfully destroyed instance %s", name))
			}
		}(instance) // Pass instanceName to the goroutine
	}

	wg.Wait() // Wait for all goroutines to finish

	// Check if there were any errors
	if len(errors) > 0 {
		return fmt.Errorf("errors occurred while deleting instances: %v", errors)
	}

	return nil
}

// stripAfterLastHyphen removes everything after the last hyphen in the input string.
func stripAfterLastHyphen(s string) string {
	lastIndex := strings.LastIndex(s, "-")
	if lastIndex == -1 {
		return s // No hyphen found, return the original string
	}
	return s[:lastIndex]
}

// stripAllAfterLastHyphen iterates over a list of strings and strips after the last hyphen from each.
func stripAllAfterLastHyphen(list []string) []string {
	strippedList := make([]string, len(list))
	for i, item := range list {
		strippedList[i] = stripAfterLastHyphen(item)
	}
	return strippedList
}
